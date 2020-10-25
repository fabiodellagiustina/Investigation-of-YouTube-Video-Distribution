#!/bin/sh

sudo apt-get update
sudo apt-get install -y python
git clone https://github.com/fabiodellagiustina/Investigation-of-YouTube-Video-Distribution.git
crontab Investigation-of-YouTube-Video-Distribution/crontab
