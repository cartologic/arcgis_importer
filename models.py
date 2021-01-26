# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import json

from django.db import models
from django.utils.translation import ugettext as _

from geonode.people.models import Profile

URL_HELP = "Esri Feature Layer URL Example: https://xxx/ArcGIS/rest/services/xxx/xxx/MapServer/0"


class ArcGISLayerImport(models.Model):
    TASK_STATUS = (
        ("PENDING", _("pending")),
        ("IN_PROGRESS", _("in progress")),
        ("FINISHED", _("finished")),
    )
    url = models.URLField(
        null=False, verbose_name="Layer URL", help_text=_(URL_HELP),
        blank=False)
    config = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now=False, auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True, auto_now_add=False)
    user = models.ForeignKey(Profile, blank=False, null=False, on_delete=models.CASCADE)
    status = models.CharField(max_length=50, null=False, blank=False,
                              default="PENDING")
    task_result = models.TextField(null=True, blank=True)

    @property
    def config_obj(self):
        return self.config

    @property
    def config_dict(self):
        config = {}
        try:
            config = json.loads(self.config)
        except ValueError:
            pass
        return config

    def __str__(self):
        return self.url

    def __unicode__(self):
        return self.url

    @config_obj.setter
    def config_obj(self, value):
        if isinstance(value, dict):
            value = json.dumps(value)
        self.config = value

    class Meta:
        verbose_name = "ArcGIS Layer Import"
        verbose_name_plural = "ArcGIS Layer Imports"
        ordering = ['-created_at', ]


class ImportedLayer(models.Model):
    url = models.URLField(null=False, verbose_name="Layer URL", blank=False, help_text=_(URL_HELP))
    name = models.CharField(_('name'), max_length=128, null=True, blank=True)
    last_update_status = models.CharField(_('Last Update Status'), max_length=128, null=True, blank=True,
                                          choices=(('Failed', _('Failed')), ('Succeeded', _('Succeeded'))))
    created_at = models.DateTimeField(auto_now=False, auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True, auto_now_add=False)

    def __str__(self):
        return self.name
