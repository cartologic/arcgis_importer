# -*- coding: utf-8 -*-
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
from requests.exceptions import ConnectionError

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
        self.esri_serializer.get_data()
        if not self.config_obj.name:
            self.config_obj.name = self.esri_serializer.get_name()
        self.config_obj.get_new_name()

    @property
    def task(self):
        if self.task_id and not self._task:
            try:
                self._task = ArcGISLayerImport.objects.get(id=self.task_id)
            except ObjectDoesNotExist as e:
                logger.warn(e)
        return self._task

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
        import_obj, _created = ArcGISLayerImport.objects.get_or_create(
            url=url, config=config_obj.as_dict(),
            status="PENDING",
            user=config_obj.get_user())
        return import_obj.id

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
            geom_dict = featureDict["geometry"]
            if not geom_dict:
                raise EsriFeatureLayerException("No Geometry Information")
            geom_type = geom_dict["type"]
            feature = ogr.Feature(layer.GetLayerDefn())
            coords = self.get_geom_coords(geom_dict)
            f_json = json.dumps({"type": geom_type, "coordinates": coords})
            geom = ogr.CreateGeometryFromJson(f_json)
            if geom and srs:
                geom.Transform(srs)
            if geom and expected_type != geom.GetGeometryType():
                geom = ogr.ForceTo(geom, expected_type)
            if geom and expected_type == geom.GetGeometryType(
            ) and geom.IsValid():
                feature.SetGeometry(geom)
                for prop, val in featureDict["properties"].items():
                    name = str(SLUGIFIER(prop))
                    value = val
                    if value and layer.GetLayerDefn().GetFieldIndex(name) != -1:
                        feature.SetField(name, value)
                layer.CreateFeature(feature)
                created = True
        except Exception as e:
            logger.error(e)
            created = False
        return created

    @contextmanager
    def create_source_layer(self, source, name, projection, gtype, options):
        layer = source.CreateLayer(
            str(name), srs=projection, geom_type=gtype, options=options)
        if not layer:
            raise EsriFeatureLayerException("Failed to Create Layer Table")
        yield layer
        layer = None

    def esri_to_postgis(self,
                        geom_name='geom'):
        gpkg_layer = None
        try:
            self.esri_serializer.get_data()
            if not self.config_obj.name:
                self.config_obj.name = self.esri_serializer.get_name()
            self.config_obj.get_new_name()
            feature_iter = iter(self)
            if self.task:
                self.task.status = "IN_PROGRESS"
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
                coord_trans = None
                OSR_WGS84_REF = osr.SpatialReference()
                OSR_WGS84_REF.ImportFromEPSG(4326)
                projection = self.esri_serializer.get_projection()
                if projection != OSR_WGS84_REF:
                    coord_trans = osr.CoordinateTransformation(
                        OSR_WGS84_REF, projection)
                with self.create_source_layer(source,
                                              str(self.config_obj.name),
                                              projection, gtype,
                                              options) as layer:
                    for field in self.esri_serializer.build_fields():
                        layer.CreateField(field)
                    layer.StartTransaction()
                    gpkg_layer = OSGEOLayer(layer, source)
                    for next_feature in feature_iter:
                        self.create_feature(
                            layer, next_feature, gtype, srs=coord_trans)
                    layer.CommitTransaction()
        except (StopIteration, EsriFeatureLayerException,
                ConnectionError) as e:
            logger.debug(e)
        except BaseException as e:
            logger.error(e)
        finally:
            return gpkg_layer

    def publish(self):
        try:
            geonode_layer = None
            layer = self.esri_to_postgis()
            if not layer:
                raise Exception("failed to dump layer")
            gs_pub = GeoserverPublisher()
            geonode_pub = GeonodePublisher(owner=self.config_obj.get_user())
            published = gs_pub.publish_postgis_layer(
                self.config_obj.name, layername=self.config_obj.name)
            if published:
                agsURL, agsId = self._layer_url.rsplit('/', 1)
                tmp_dir = get_new_dir()
                ags_layer = AgsLayer(
                    agsURL + "/", int(agsId), dump_folder=tmp_dir)
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
                            icon_paths.append(
                                os.path.join(tmp_dir, ags_layer.name, file))
                        if file.endswith(".svg"):
                            icon_paths.append(
                                os.path.join(tmp_dir, ags_layer.name, file))
                if len(icon_paths) > 0:
                    for icon_path in icon_paths:
                        uploaded = gs_pub.upload_file(
                            open(icon_path, 'rb'),
                            rel_path=urljoin(ICON_REL_PATH, ags_layer.name))
                        if not uploaded:
                            logger.error("Failed To Upload SLD Icon {}".format(
                                icon_path))
                if sld_path:
                    style = gs_pub.create_style(
                        self.config_obj.name, sld_path, overwrite=True)
                    if style:
                        gs_pub.set_default_style(self.config_obj.name, style)

            geonode_layer = geonode_pub.publish(self.config_obj)
            if geonode_layer:
                logger.info(geonode_layer.alternate)
                gs_pub.remove_cached(geonode_layer.alternate)

        except Exception as e:
            logger.error()
            if self.task:
                self.task.status = "FINISHED"
                self.task.task_result = e
                self.task.save()
        finally:
            if geonode_layer:
                layer_url = reverse_lazy('layer_detail', kwargs={
                                 'layername': geonode_layer.alternate})
                msg = "your layer title is {} and url is {}".format(
                    geonode_layer.title,
                    urljoin(settings.SITEURL, layer_url.lstrip('/'))
                )
                if self.task:
                    self.task.status = "FINISHED"
                    self.task.task_result = msg
                    self.task.save()
            return geonode_layer
