# -*- coding: utf-8 -*-
import socket
from kombu import Connection
from django.conf import settings


def check_broker_status():
    running = False
    broker_url = getattr(settings, 'CELERY_BROKER_URL', None)
    if 'memory' not in broker_url:
        try:
            conn = Connection(broker_url)
            conn.ensure_connection(max_retries=3)
            running = True
        except socket.error:
            pass
    return running
