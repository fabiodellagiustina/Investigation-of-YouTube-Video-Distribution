#!/bin/sh
sudo apt-get update
sudo apt-get install -y python
wget https://bootstrap.pypa.io/get-pip.py
sudo python get-pip.py
pip install --upgrade google-api-python-client
git clone https://github.com/fabiodellagiustina/Investigation-of-YouTube-Video-Distribution.git
