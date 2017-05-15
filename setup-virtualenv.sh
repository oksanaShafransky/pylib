#!/bin/bash

virtualenv --system-site-packages .
source bin/activate
# wget https://bootstrap.pypa.io/ez_setup.py -O - | python
bin/easy_install pip
bin/pip install pip==9.0.1
bin/pip install setuptools==35.0.2
bin/pip install boto==2.35.0


# virtualenv --no-setuptools .
# source bin/activate
# wget https://bootstrap.pypa.io/ez_setup.py -O - | python
# bin/easy_install pip
