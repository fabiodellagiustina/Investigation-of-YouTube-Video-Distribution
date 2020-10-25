#!/usr/bin/env python

import ftplib

CENTRALISATION_SERVER = 'pi4.fabiodellagiustina.it'
DIR_TO_STORE = 'pytomo_db_dir'

try:
    ftp = ftplib.FTP(CENTRALISATION_SERVER)
except ftplib.all_errors, mes:
    print('Could not connect to FTP server %s\n%s', CENTRALISATION_SERVER, mes)
try:
    ftp.login(user='pytomo', passwd='123panza')
except ftplib.all_errors, mes:
    print('Could not login to FTP server %s\n%s', CENTRALISATION_SERVER, mes)
try:
    ftp.cwd(DIR_TO_STORE)
except ftplib.all_errors, mes:
    print('Could not change directory to %s\n%s', DIR_TO_STORE, mes)
