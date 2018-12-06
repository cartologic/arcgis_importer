import threading

from cartoview.log_handler import get_logger

logger = get_logger(__name__)


def background_import(task, *args, **kwargs):
    t = threading.Thread(target=task,
                         args=args, kwargs=kwargs)
    t.setDaemon(True)
    t.start()
