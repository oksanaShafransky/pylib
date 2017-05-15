#!/bin/bash

virtualenv --no-setuptools .
source bin/activate
# wget https://bootstrap.pypa.io/ez_setup.py -O - | python
bin/easy_install pip
# bin/pip install setuptools==35.0.2
