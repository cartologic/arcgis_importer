# -*- coding: utf-8 -*-
import socket

from django.conf import settings
from kombu import Connection


def get_broker_url():
    broker_url = getattr(settings, 'CELERY_BROKER_URL', None)
    if not broker_url:
        broker_url = getattr(settings, 'BROKER_URL', None)
    return broker_url


def _check_async():
    return getattr(settings, 'ASYNC_SIGNALS', False)


def check_broker_status():
    running = False
    broker_url = get_broker_url()
    if 'memory' not in broker_url and _check_async():
        try:
            conn = Connection(broker_url)
            conn.ensure_connection(max_retries=3)
            running = True
        except socket.error:
            running = _check_async()

    return running
