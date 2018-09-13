#!/bin/bash
exec gunicorn --workers 3 --bind 0.0.0.0:5000 -m 007 wsgi:app

