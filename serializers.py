try:
    import ogr
    import osr
except ImportError:
    from osgeo import ogr, osr
import requests

from osgeo_manager.constants import SLUGIFIER
from osgeo_manager.exceptions import EsriFeatureLayerException


class EsriSerializer(object):
    field_types_mapping = {
        "esriFieldTypeInteger": ogr.OFTInteger64,
        "esriFieldTypeSmallInteger": ogr.OFSTInt16,
        "esriFieldTypeDouble": ogr.OFTReal,
        "esriFieldTypeSingle": ogr.OFSTFloat32,
        "esriFieldTypeString": ogr.OFTString,
        "esriFieldTypeDate": ogr.OFTString,
        "esriFieldTypeBlob": ogr.OFTBinary,
        "esriFieldTypeXML": ogr.OFTBinary,
        # "esriFieldTypeGUID": "XXXX",
    }
    ignored_fields = [
        "SHAPE_Length", "SHAPE_Area", "SHAPE.LEN", "Shape.STLength()",
        "Shape.STArea()"
    ]
    geometry_types_mapping = {
        "esriGeometryPolygon": ogr.wkbMultiPolygon,
        "esriGeometryPoint": ogr.wkbPoint,
        "esriGeometryMultipoint": ogr.wkbMultiPoint,
        "esriGeometryPolyline": ogr.wkbLineString,
        "esriGeometryLine": ogr.wkbLineString,
        "esriGeometryCircularArc": ogr.wkbCurve,
        "esriGeometryEllipticArc": ogr.wkbCircularString,
        "esriGeometryEnvelope": ogr.wkbPolygon,
        "esriGeometryRing": ogr.wkbPolygon,
        "esriGeometryPath": ogr.wkbLineString,
    }
    ogr_geometry_types_mapping = {
        "Polygon": ogr.wkbPolygon,
        "MultiPolygon": ogr.wkbMultiPolygon,
        "Point": ogr.wkbPoint,
        "Multipoint": ogr.wkbMultiPoint,
        "LineString": ogr.wkbLineString,
        "MultiLineString": ogr.wkbMultiLineString,
    }

    def __init__(self, url):
        self._url = url
        self._data = None
        self.fields_domains = {}
        self.subtypes = {}
        self.subtypes_fields = []  # maintain list of sub types fields to easily check which field has a subtype
        layer_type = self._url.split('/')[-2]
        if layer_type.lower() == 'mapserver':
            self.subtype_property_name = 'typeIdField'
            self.subtypes_dict_property_name = 'types'
            self.subtype_id_property_name = 'id'
        elif layer_type.lower() == 'featureserver':
            self.subtype_property_name = 'subtypeField'
            self.subtypes_dict_property_name = 'subtypes'
            self.subtype_id_property_name = 'code'
        else:
            raise EsriFeatureLayerException(
                "This URL {} Is Not A FeatureServer Nor MapServer URL".format(self._url))

    def get_data(self):
        req = requests.get(self._url + "?f=json")
        if not self._data:
            self._data = req.json()
            self.validate_feature_layer()

    def validate_feature_layer(self):
        if not self.is_feature_layer:
            raise EsriFeatureLayerException(
                "This URL {} Is Not A Feature Layer".format(self._url))

    def get_fields_list(self):
        data_fields = self._data['fields']
        assert data_fields
        layer_fields = []

        def search_by_name(name):
            check = False
            for field in layer_fields:
                if SLUGIFIER(field["name"]) == SLUGIFIER(name):
                    check = True
                    break
            return check

        for field in data_fields:
            if field["type"] in self.field_types_mapping.keys(
            ) and field["name"] not in self.ignored_fields and \
                    not search_by_name(field["name"]):
                layer_fields.append(field)
        return layer_fields

    def build_subtypes(self):
        # build subtype data structure to facilitate replacing id/code with mapped value
        for sub_type in self._data[self.subtypes_dict_property_name]:
            sub_types = {}
            for field_name in sub_type['domains']:
                if sub_type['domains'][field_name]['type'] == 'codedValue':
                    if field_name not in self.subtypes_fields:
                        self.subtypes_fields.append(field_name)
                    sub_types[field_name] = \
                        {coded_value['code']: coded_value['name'] for coded_value in sub_type['domains'][field_name]['codedValues']}
            if len(sub_types.keys()):
                self.subtypes[sub_type[self.subtype_id_property_name]] = sub_types

    def build_fields(self):
        # check if a subtype is defined
        if self._data[self.subtype_property_name]:
            self.build_subtypes()
        data_fields = self.get_fields_list()
        field_defns = []
        for field in data_fields:
            field_type = field["type"]
            if str(SLUGIFIER(field["name"])).encode('utf-8'):
                # domain with coded values handling
                if field['domain'] and field['domain']['type'] == 'codedValue':
                    field_type = "esriFieldTypeString"  # enforce string type to accept the coded value
                    self.fields_domains[field["name"]] = {}
                    for coded_value in field['domain']['codedValues']:
                        self.fields_domains[field["name"]][coded_value['code']] = coded_value['name']
                elif field['name'] in self.subtypes_fields:
                    field_type = "esriFieldTypeString"  # enforce string type to accept the coded value

                field_defn = ogr.FieldDefn(
                    str(SLUGIFIER(field["name"])),
                    self.field_types_mapping[field_type])
                if field_type == "esriFieldTypeString" and field.get("length", None):
                    # NOTE: handle large text by WideString
                    # For Now set max length by default
                    # field_defn.SetWidth(field["length"])
                    field_defn.SetWidth(10485760)
                if field_type in "esriFieldTypeInteger":
                    field_defn.SetPrecision(64)
                if field_type != "esriFieldTypeDouble":
                    field_defn.SetNullable(1)
                field_defns.append(field_defn)
        return field_defns

    def get_geometry_type(self):
        geom_type = self.geometry_types_mapping.get(
            self._data.get("geometryType", None), None)
        if not geom_type:
            raise EsriFeatureLayerException("No Geometry Type")
        return geom_type

    @property
    def is_feature_layer(self):
        return self._data['type'] in ["Feature Layer", "Table"]

    @property
    def subtype_field_name(self):
        # convert field name to upper case to match fields definition.
        # "By observation" fields name in upper case.
        # TODO: check if there are cases that violate this observation.
        return self._data[self.subtype_property_name].upper()

    def attributes_convertor(self, attributes):
        raise NotImplementedError("To Be Implemented")

    def get_name(self):
        return SLUGIFIER(self._data["name"].lower())

    def get_projection(self):
        try:
            srs = self._data["extent"]["spatialReference"]
            if "latestWkid" in srs:
                projection_number = srs["latestWkid"]
            else:
                # older version of arcgis rest hasn't latestWkid
                # handle deprecated 102100 projection
                projection_number = 3857 if srs["wkid"] == 102100 else srs["wkid"]
        except BaseException:
            projection_number = 4326
        testSR = osr.SpatialReference()
        res = testSR.ImportFromEPSG(projection_number)
        if res != 0:
            testSR.ImportFromEPSG(4326)
        return testSR
