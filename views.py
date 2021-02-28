# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib.auth.decorators import login_required
from django.shortcuts import render

from . import APP_NAME

# Create your views here.


@login_required
def index(request):
    context = {"APP_NAME": APP_NAME}
    return render(request, template_name="{}/index.html".format(APP_NAME),
                  context=context)
