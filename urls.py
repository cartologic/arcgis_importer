# -*- coding: utf-8 -*-
from django.conf.urls import include, url
from tastypie.api import Api
from . import APP_NAME
from .rest import ArcGISImportResource
from .views import index
api = Api(api_name=APP_NAME)
api.register(ArcGISImportResource())
urlpatterns = [
    url(r'^$', index, name="%s.index" % (APP_NAME)),
    url(r'^api/', include(api.urls)),
]
