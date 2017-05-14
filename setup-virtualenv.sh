#!/bin/bash

virtualenv .
source bin/activate
bin/pip install --upgrade pip
bin/pip install setuptools==11.3
bin/pip pytest==3.0.7
bin/pip pluggy==0.4.0
