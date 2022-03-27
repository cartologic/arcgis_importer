# -*- coding: utf-8 -*-
import datetime

import requests

from .import_status import ImportStatus

try:
    import ogr
    import osr
except ImportError:
    from osgeo import ogr, osr
import json
import os
from contextlib import contextmanager

from ags2sld.handlers import Layer as AgsLayer
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.urls import reverse_lazy
from esridump.dumper import EsriDumper
from esridump.errors import EsriDownloadError
from requests.exceptions import ConnectionError

from geonode.geoserver.helpers import gs_catalog, get_store

from osgeo_manager.base_manager import OSGEOManager, get_connection
from osgeo_manager.config import LayerConfig
from osgeo_manager.constants import ICON_REL_PATH, SLUGIFIER
from osgeo_manager.decorators import validate_config
from osgeo_manager.exceptions import EsriFeatureLayerException
from osgeo_manager.layers import OSGEOLayer
from osgeo_manager.os_utils import get_new_dir
from osgeo_manager.publishers import GeonodePublisher, GeoserverPublisher
from osgeo_manager.utils import get_store_schema, urljoin

from .models import ArcGISLayerImport
from .serializers import EsriSerializer

try:
    from celery.utils.log import get_task_logger as get_logger
except ImportError:
    from cartoview.log_handler import get_logger

logger = get_logger(__name__)


class EsriManager(EsriDumper):
    def __init__(self, *args, **kwargs):
        self.task_id = kwargs.pop('task_id', None)
        self._conf = None
        self._task = None
        super(EsriManager, self).__init__(*args, **kwargs)
        self.esri_serializer = EsriSerializer(self._layer_url)
        self.update_task("Getting Layer Info", ImportStatus.IN_PROGRESS)
        self.esri_serializer.get_data()
        if not self.config_obj.name:
            self.config_obj.name = self.esri_serializer.get_name()
        self.config_obj.get_new_name()
        self.update_task("Configuration Parsed")

    @property
    def task(self):
        if self.task_id and not self._task:
            try:
                self._task = ArcGISLayerImport.objects.get(id=self.task_id)
            except ObjectDoesNotExist as e:
                logger.warn(e)
        return self._task

    def update_task(self, message, status=None):
        logger.info(message)
        self.task.task_result = message
        if status:
            self.task.status = status
        self.task.save()

    @property
    def config_obj(self):
        if self.task and not self._conf:
            self._conf = LayerConfig(config=self.task.config_dict)
        if not self._conf:
            self._conf = LayerConfig()
        return self._conf

    @config_obj.setter
    def config_obj(self, value):
        self._conf = value

    @classmethod
    def create_task(cls, url, config=LayerConfig()):
        config_obj = validate_config(config_obj=config)
        esri_serializer = EsriSerializer(url)
        esri_serializer.get_data()
        if not config_obj.name:
            config_obj.name = esri_serializer.get_name()
        config_obj.get_new_name()
        import_obj, _created = ArcGISLayerImport.objects.get_or_create(url=url, config=json.dumps(config_obj.as_dict()),
                                                                       status=ImportStatus.PENDING, user=config_obj.get_user())
        return import_obj.id

    # set _outSR to fetch the data with a projection
    # NOTE: this function MUST be called before iterating features to take effect.
    def set_out_sr(self, wkid):
        self._outSR = wkid

    def get_geom_coords(self, geom_dict):
        if "rings" in geom_dict:
            return geom_dict["rings"]
        elif "paths" in geom_dict:
            return geom_dict["paths"] if len(
                geom_dict["paths"]) > 1 else geom_dict["paths"][0]
        else:
            return geom_dict["coordinates"]

    def create_feature(self, layer, featureDict, expected_type, srs=None):
        created = False
        try:
            feature = ogr.Feature(layer.GetLayerDefn())

            if self.esri_serializer.is_feature_layer:
                geom = self.create_geometry(expected_type, featureDict, srs)
                if geom and expected_type == geom.GetGeometryType() and geom.IsValid():
                    feature.SetGeometry(geom)

            for prop, value in featureDict["properties"].items():
                name = str(SLUGIFIER(prop))
                field_index = layer.GetLayerDefn().GetFieldIndex(name)
                # property should have value and the field is created and mapped in the destination layer to be handled
                if value and field_index != -1:
                    if layer.schema[field_index].type == ogr.OFTDateTime:
                        # convert from milliseconds to seconds(value/1000) and get datetime object
                        datetime_value = datetime.datetime.fromtimestamp(value/1000)
                        feature.SetField(name, datetime_value.year, datetime_value.month, datetime_value.day,
                                         datetime_value.hour, datetime_value.minute, datetime_value.second,
                                         0  # 0 for unknown timezone , TODO: check if it handled properly
                                         )
                        continue
                    # replace id/code with mapped valued for subtypes
                    elif prop in self.esri_serializer.subtypes_fields:
                        type_field_value = featureDict["properties"][self.esri_serializer.subtype_field_name]
                        # It is supposed to find the value, but check in case the data is not correct
                        if value in self.esri_serializer.subtypes[type_field_value][prop]:
                            value = self.esri_serializer.subtypes[type_field_value][prop][value]
                    # It is supposed to find the value, but check in case the data is not correct
                    elif prop in self.esri_serializer.fields_domains \
                            and value in self.esri_serializer.fields_domains[prop]:
                        # replace id/code with mapped value for domain coded values
                        value = self.esri_serializer.fields_domains[prop][value]
                    feature.SetField(name, value)
            created = layer.CreateFeature(feature) == ogr.OGRERR_NONE
        except Exception as e:
            logger.error('Failed to create feature', e)
        return created

    def create_geometry(self, expected_type, featureDict, srs):
        geom_dict = featureDict["geometry"]
        if not geom_dict:
            raise EsriFeatureLayerException("No Geometry Information")
        geom_type = geom_dict["type"]
        coords = self.get_geom_coords(geom_dict)
        f_json = json.dumps({"type": geom_type, "coordinates": coords})
        geom = ogr.CreateGeometryFromJson(f_json)
        if geom and srs:
            geom.Transform(srs)
        if geom and expected_type != geom.GetGeometryType():
            geom = ogr.ForceTo(geom, expected_type)
        return geom

    @contextmanager
    def create_source_layer(self, source, name, projection, gtype, options):
        layer = source.CreateLayer(str(name), srs=projection, geom_type=gtype, options=options)
        if not layer:
            raise EsriFeatureLayerException("Failed to Create Layer Table")
        yield layer
        layer = None

    def esri_to_postgis(self, geom_name='geom'):
        gpkg_layer = None
        try:
            # self.esri_serializer.get_data()
            if not self.config_obj.name:
                self.config_obj.name = self.esri_serializer.get_name()
            self.config_obj.get_new_name()
            feature_iter = iter(self)
            if self.task:
                self.task.status = ImportStatus.IN_PROGRESS
                self.task.save()
            with OSGEOManager.open_source(get_connection()) as source:
                options = [
                    'OVERWRITE={}'.format(
                        "YES" if self.config_obj.overwrite else 'NO'),
                    'TEMPORARY={}'.format(
                        "OFF" if not self.config_obj.temporary else "ON"),
                    'LAUNDER={}'.format(
                        "YES" if self.config_obj.launder else "NO"),
                    'GEOMETRY_NAME={}'.format(
                        geom_name if geom_name else 'geom'),
                    'SCHEMA={}'.format(get_store_schema())
                ]
                gtype = self.esri_serializer.get_geometry_type()
                # get source layer projection
                projection = self.esri_serializer.get_projection()
                # set outSR with original wkid , so no need to transform the geometry after fetching
                self.set_out_sr(int(projection.GetAuthorityCode(None)))
                try:
                    with self.create_source_layer(source, str(self.config_obj.name), projection, gtype, options) as layer:
                        self.update_task("DB table created")
                        for field in self.esri_serializer.build_fields():
                            layer.CreateField(field)
                        layer.StartTransaction()
                        self.update_task("Starting loading data into db table")
                        created_count = 0
                        failed_count = 0
                        feature_count = self.get_feature_count()
                        static_msg = "Features: Processed {processed} of {total}, Created {created}, Failed {failed}"
                        for next_feature in feature_iter:
                            if self.create_feature(layer, next_feature, gtype):
                                created_count += 1
                            else:
                                failed_count += 1
                            current_state = static_msg.format(processed=created_count+failed_count,
                                                              total=feature_count,
                                                              created=created_count, failed=failed_count)
                            self.update_task(current_state)
                        layer.CommitTransaction()
                        self.update_task("Data imported into DB table")
                        gpkg_layer = OSGEOLayer(layer, source)
                # TODO: check all possible exceptions and handle it properly
                except EsriDownloadError as e:
                    # delete the layer as not all features imported successfully
                    source.DeleteLayer(self.config_obj.name)
                    logger.error(e)
        except (StopIteration, EsriFeatureLayerException, ConnectionError) as e:
            logger.debug(e)
        # except BaseException as e:
        #     logger.error(e)
        # finally:
        #     return gpkg_layer
        return gpkg_layer

    def publish(self):
        try:
            geonode_layer = None
            layer = self.esri_to_postgis()
            if not layer:
                raise Exception("failed to dump layer")
            self.update_task("Publishing to Geoserver")
            gs_pub = GeoserverPublisher()
            geonode_pub = GeonodePublisher(owner=self.config_obj.get_user())
            published = gs_pub.publish_postgis_layer(
                self.config_obj.name, layername=self.config_obj.name)
            if published:
                agsURL, agsId = self._layer_url.rsplit('/', 1)
                tmp_dir = get_new_dir()
                ags_layer = AgsLayer(agsURL + "/", int(agsId), dump_folder=tmp_dir)
                try:
                    ags_layer.dump_sld_file()
                except Exception as e:
                    logger.error(e)
                sld_path = None
                icon_paths = []
                for file in os.listdir(tmp_dir):
                    if file.endswith(".sld"):
                        sld_path = os.path.join(tmp_dir, file)
                icons_dir = os.path.join(tmp_dir, ags_layer.name)
                if os.path.exists(icons_dir):
                    for file in os.listdir(icons_dir):
                        if file.endswith(".png"):
                            icon_paths.append(os.path.join(tmp_dir, ags_layer.name, file))
                        if file.endswith(".svg"):
                            icon_paths.append(os.path.join(tmp_dir, ags_layer.name, file))
                if len(icon_paths) > 0:
                    for icon_path in icon_paths:
                        uploaded = gs_pub.upload_file(open(icon_path, 'rb'),
                                                      rel_path=urljoin(ICON_REL_PATH, ags_layer.name))
                        if not uploaded:
                            logger.error("Failed To Upload SLD Icon {}".format(icon_path))
                if sld_path:
                    self.update_task("Creating Style")
                    style = gs_pub.create_style(
                        self.config_obj.name, sld_path, overwrite=True)
                    if style:
                        gs_pub.set_default_style(self.config_obj.name, style)
            self.update_task("Publishing to GeoNode")
            geonode_layer = geonode_pub.publish(self.config_obj)
            if geonode_layer:
                logger.info(geonode_layer.alternate)
                gs_pub.remove_cached(geonode_layer.alternate)

        except Exception as e:
            logger.error(e)
            if self.task:
                self.task.status = ImportStatus.FAILED
                self.task.task_result = e
                self.task.save()
        finally:
            if geonode_layer:
                layer_url = reverse_lazy('layer_detail', kwargs={'layername': geonode_layer.alternate})
                msg = "your layer title is {} and url is {}".format(
                    geonode_layer.title,
                    urljoin(settings.SITEURL, layer_url.lstrip('/'))
                )
                if self.task:
                    self.task.status = ImportStatus.FINISHED
                    self.task.task_result = msg
                    self.task.save()
            return geonode_layer

    # delete all data exist and import it again from the ArcGIS service.
    def reload_data(self, geonode_layer):
        if self.task:
            self.task.status = ImportStatus.IN_PROGRESS
            self.task.save()
        # To get layer name from alternate as it is the same as DB table name and geoserver layer name
        self.config_obj.name = geonode_layer.alternate.split(':')[-1]
        self.config_obj.overwrite = True
        feature_iter = iter(self)
        gtype = self.esri_serializer.get_geometry_type()
        store = get_store(gs_catalog, geonode_layer.store, geonode_layer.workspace)
        # get database name and schema name from layer datastore
        # TODO: get all parameters for the datastore
        # TODO: find a way to pass the database password also , as it is encrypted in the datastore.
        db_connection = get_connection(database_name=store.connection_parameters['database'],
                                       schema=store.connection_parameters.get('schema', 'public'))
        with OSGEOManager.open_source(db_connection, update_enabled=1) as source:
            geoserver_layer = gs_catalog.get_layer(geonode_layer.alternate)
            # pass native_name to GetLayer as it represents the table name
            layer = source.GetLayer(geoserver_layer.resource.native_name)
            try:
                layer.StartTransaction()
                # remove all features
                # Note: remove features one by one allow to rollback if the error raised
                # TODO: check if truncating the table is possible to enhance the performance
                old_feature = layer.GetNextFeature()
                while old_feature:
                    layer.DeleteFeature(old_feature.GetFID())
                    old_feature = layer.GetNextFeature()
                # TODO: reset FID sequence otherwise new FIDs will be generated

                # build fields is mandatory for domain fields and subtypes
                self.esri_serializer.build_fields()

                # set outSR by destination layer wkid, to retrieve the features with matched projection
                self.set_out_sr(int(layer.GetSpatialRef().GetAuthorityCode(None)))

                # importing the features again
                for next_feature in feature_iter:
                    self.create_feature(layer, next_feature, gtype)
                layer.CommitTransaction()

                geoserver_pub = GeoserverPublisher()
                # remove layer caching to update rendering.
                # otherwise changes will not be rendered until layer refreshed
                geoserver_pub.remove_cached(geonode_layer.typename)

                if self.task:
                    self.task.status = ImportStatus.FINISHED
                    self.task.save()
            # TODO: check the which exceptions should be handled
            except (StopIteration, EsriFeatureLayerException, ConnectionError, BaseException) as e:
                layer.RollbackTransaction()
                logger.error(e)
                return False
            else:
                return True

    # delete all data exist and import it again from the ArcGIS service.
    def append_new_data(self, geonode_layer):
        self.update_task("", ImportStatus.IN_PROGRESS)
        # To get layer name from alternate as it is the same as DB table name and geoserver layer name
        self.config_obj.name = geonode_layer.alternate.split(':')[-1]
        self.config_obj.overwrite = True
        feature_iter = iter(self)
        gtype = self.esri_serializer.get_geometry_type()
        store = get_store(gs_catalog, geonode_layer.store, geonode_layer.workspace)
        # get database name and schema name from layer datastore
        # TODO: get all parameters for the datastore
        # TODO: find a way to pass the database password also , as it is encrypted in the datastore.
        db_connection = get_connection(database_name=store.connection_parameters['database'],
                                       schema=store.connection_parameters.get('schema', 'public'))
        geoserver_layer = gs_catalog.get_layer(geonode_layer.alternate)

        with OSGEOManager.open_source(db_connection) as ds:
            # Get maximum value for update field
            result = ds.ExecuteSQL("select max({0}) from {1};".format(self.task.config_dict['update_field'],
                                                                          geoserver_layer.resource.native_name))
            update_value = result.GetNextFeature().GetFieldAsString(0)
            # TODO: check if time zone should be converted not truncated
            update_value = update_value[:19]  # Truncate the time zone part from date value
            # TODO: handle fields types(Date fields supported only).
            query_args = {"where": "{0}>DATE '{1}'".format(self.task.config_dict['update_field'], update_value)}
            self._query_params = self._build_query_args(query_args=query_args)

        with OSGEOManager.open_source(db_connection, update_enabled=1) as source:

            # pass native_name to GetLayer as it represents the table name
            layer = source.GetLayer(geoserver_layer.resource.native_name)
            try:
                layer.StartTransaction()

                # build fields is mandatory for domain fields and subtypes
                self.esri_serializer.build_fields()

                if self.esri_serializer.is_feature_layer:
                    # set outSR by destination layer wkid, to retrieve the features with matched projection
                    self.set_out_sr(int(layer.GetSpatialRef().GetAuthorityCode(None)))

                # importing new features
                for next_feature in feature_iter:
                    self.create_feature(layer, next_feature, gtype)
                layer.CommitTransaction()

                geoserver_pub = GeoserverPublisher()
                # remove layer caching to update rendering.
                # otherwise changes will not be rendered until layer refreshed
                geoserver_pub.remove_cached(geonode_layer.typename)

                self.update_task("", ImportStatus.FINISHED)
            # TODO: check the which exceptions should be handled
            except (StopIteration, EsriFeatureLayerException, ConnectionError, BaseException) as e:
                layer.RollbackTransaction()
                logger.error(e)
                return False
            else:
                return True
