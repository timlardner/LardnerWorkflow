#!/bin/sh
cd app
su -m app -c "celery -A Celery.tasks worker --loglevel WARNING"