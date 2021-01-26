# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from .models import ArcGISLayerImport, ImportedLayer
from django.contrib import admin

# Register your models here.


@admin.register(ArcGISLayerImport)
class ArcGISLayerImportAdmin(admin.ModelAdmin):
    pass


@admin.register(ImportedLayer)
class ImportedLayerAdmin(admin.ModelAdmin):
    pass
