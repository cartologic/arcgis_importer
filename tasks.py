import threading

from cartoview.log_handler import get_logger
from geonode.celery_app import app

from .esri import EsriManager
from .models import ArcGISLayerImport

logger = get_logger(__name__)


@app.task(bind=True)
def celery_import_task(self, task_id):
    task = ArcGISLayerImport.objects.get(id=task_id)
    em = EsriManager(task.url, task_id=task.id)
    em.publish()


def background_import(task_id):
    t = threading.Thread(target=celery_import_task.delay,
                         args=(task_id,))
    t.setDaemon(True)
    t.start()
