from django.conf.urls import url
from tastypie import fields, http
from tastypie.authentication import (ApiKeyAuthentication, BasicAuthentication,
                                     MultiAuthentication,
                                     SessionAuthentication)
from tastypie.authorization import DjangoAuthorization
from tastypie.constants import ALL, ALL_WITH_RELATIONS
from tastypie.resources import ModelResource
from tastypie.utils import trailing_slash
from tastypie.serializers import Serializer
from geonode.api.api import ProfileResource
from .serializers import EsriSerializer
from .esri import EsriManager
from osgeo_manager.config import LayerConfig
from .models import ArcGISLayerImport
from .tasks import background_import


class BaseModelResource(ModelResource):
    def get_err_response(self, request, message,
                         response_class=http.HttpApplicationError):
        data = {
            'error': message,
        }
        return self.error_response(
            request, data, response_class=response_class)


class ArcGISImportResource(BaseModelResource):
    user = fields.ForeignKey(ProfileResource, 'user', full=False, null=True)

    def prepend_urls(self):
        return [
            url(r"^(?P<resource_name>%s)/import%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('esri_import_layer'),
                name="esri_import_layer"),
        ]

    def esri_import_layer(self, request, **kwargs):
        self.method_check(request, allowed=['post'])
        self.is_authenticated(request)
        self.throttle_check(request)
        data = self.deserialize(request, request.body)
        url = data.get('url', None)
        permissions = data.get('permissions', None)
        config_dict = {}
        if not url:
            return self.get_err_response(request,
                                         "url of the layer is required")
        if permissions:
            config_dict.update({"permissions": permissions})
        try:
            es = EsriSerializer(url)
            es.get_data()
            em = EsriManager(url, config=LayerConfig(config=config_dict))
            background_import(em.publish)
            return self.create_response(request, {
                "id": em.import_obj.id,
            }, http.HttpAccepted)
        except BaseException as e:
            return self.get_err_response(request, e.message)

    class Meta:
        resource_name = "arcgis_import"
        queryset = ArcGISLayerImport.objects.all()
        always_return_data = True
        allowed_methods = ['get', 'post', 'put', 'delete']
        serializer = Serializer(formats=['json', 'plist'])
        filtering = {
            "id": ALL,
            "created_at": ALL,
            "updated_at": ALL,
            "user": ALL_WITH_RELATIONS
        }
        authorization = DjangoAuthorization()
        authentication = MultiAuthentication(SessionAuthentication(),
                                             BasicAuthentication(),
                                             ApiKeyAuthentication())
