#!/bin/bash

virtualenv --no-site-packages .
source bin/activate
bin/easy_install pip
bin/pip install pip==9.0.1
