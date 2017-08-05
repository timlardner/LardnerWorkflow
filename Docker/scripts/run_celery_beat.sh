#!/bin/sh
cd app
su -m app -c "-A Celery.tasks beat"