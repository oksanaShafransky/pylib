#!/bin/bash

virtualenv .
source bin/activate
bin/pip install --upgrade pip
bin/pip install setuptools==11.3
bin/pip pytest==2.8.1
bin/pip pluggy==0.3.1
