#!/bin/bash

virtualenv .
source bin/activate
bin/pip install --upgrade pip
bin/pip install setuptools==35.0.2
bin/pip install wheel==0.29.0