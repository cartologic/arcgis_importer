import json
import os
import threading

from celery.schedules import crontab
from guardian.utils import get_anonymous_user

from cartoview.log_handler import get_logger

from geonode.celery_app import app
from geonode.layers.models import Layer
from geonode.security.views import _perms_info_json

from .esri import EsriManager
from .models import ArcGISLayerImport, ImportedLayer

logger = get_logger(__name__)


@app.task(bind=True)
def celery_import_task(self, task_id):
    task = ArcGISLayerImport.objects.get(id=task_id)
    em = EsriManager(task.url, task_id=task.id)
    layer = em.publish()
    if layer:
        ImportedLayer.objects.create(url=task.url, name=layer.alternate)


def background_import(task_id):
    t = threading.Thread(target=celery_import_task.delay,
                         args=(task_id,))
    t.setDaemon(True)
    t.start()


@app.task(bind=True, name='arcgis_importer.tasks.update_imported_layers', queue='default')
def update_imported_layers(*args, **kwargs):
    for imported_layer in ImportedLayer.objects.all():
        # TODO: run update_imported_layer async
        update_imported_layer(imported_layer)


@app.task(bind=True, name='arcgis_importer.tasks.update_imported_layer', queue='default')
def update_imported_layer_task(self, imported_layers_id):
    imported_layer = ImportedLayer.objects.get(id=imported_layers_id)
    update_imported_layer(imported_layer)


@app.task(bind=True, name='arcgis_importer.tasks.append_imported_layer', queue='default')
def append_imported_layer_task(self, arcgis_layer_import_id):
    task = ArcGISLayerImport.objects.get(id=arcgis_layer_import_id)
    config = json.loads(task.config.replace("'", '"'))
    logger.info('update layer {0} started'.format(config['name']))
    geonode_layer = Layer.objects.get(name=config['name'])
    query = "{0}>DATE '2018-05-31 00:00:00'".format(config['update_field'])
    em = EsriManager(task.url, task_id=task.id, extra_query_args={'where': query})
    success = em.append_new_data(geonode_layer)


def update_imported_layer(imported_layer):
    logger.info('update layer {0} started'.format(imported_layer.name))
    geonode_layer = Layer.objects.get(alternate=imported_layer.name)
    task = ArcGISLayerImport.objects.create(url=imported_layer.url, user=get_anonymous_user(),
                                            config=_perms_info_json(geonode_layer))
    em = EsriManager(task.url, task_id=task.id)
    success = em.reload_data(geonode_layer)
    imported_layer.last_update_status = 'Succeeded' if success else 'Failed'
    imported_layer.save()
    logger.info('update layer {0} {1}'.format(imported_layer.name, imported_layer.last_update_status))


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # update imported layers using a scheduled task
    sender.add_periodic_task(crontab(minute=os.environ.get('ARCGIS_IMPORTER_UPDATE_MINUTE', '0'),
                                     hour=os.environ.get('ARCGIS_IMPORTER_UPDATE_HOUR', '0'),
                                     day_of_week=os.environ.get('ARCGIS_IMPORTER_UPDATE_DAY_OF_WEEK', '*'),
                                     day_of_month=os.environ.get('ARCGIS_IMPORTER_UPDATE_DAY_OF_MONTH', '*'),
                                     month_of_year=os.environ.get('ARCGIS_IMPORTER_UPDATE_MONTH_OF_YEAR', '*')),
                             update_imported_layers.s())


app.config_from_object('django.conf:settings', namespace="CELERY")
app.autodiscover_tasks()
