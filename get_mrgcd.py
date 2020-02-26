# -*- coding: utf-8 -*-
"""
Created on Fri Jan 24 10:08:24 2020

@author: buriona
"""

import sys
import json
import logging
from ftplib import FTP
from pathlib import Path
from datetime import datetime
from os import path, makedirs
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
from requests import get as req_get

FWS_URL = 'https://www.fws.gov/gisdownloads/R2/Water_Resources/BDA/BDA_Report.txt'

def print_and_log(log_str, logger=None):
    print(log_str)
    if logger:
        logger.info(log_str)

def write_backup(bak_str, backup=None):
    if backup:
        backup.info(bak_str)

def create_log(log_path='get_mrgcd.log'):
    logger = logging.getLogger('get_esp rotating log')
    logger.setLevel(logging.INFO)

    handler = TimedRotatingFileHandler(
        log_path,
        when="W6",
        backupCount=1
    )

    logger.addHandler(handler)

    return logger  

def create_backup(log_path='mrgcd.txt'):
    backup = logging.getLogger('get_esp rotating log')
    backup.setLevel(logging.INFO)

    handler = RotatingFileHandler(
        log_path,
        backupCount=100
    )

    backup.addHandler(handler)

    return backup  

def get_mrgcd_config(schema='mrgcd', config_file='ftp_config.json'):
    config_path = Path(config_file).resolve()
    with config_path.open('r') as config:
        config_dict = json.load(config)
    return config_dict.get(schema, None)

def get_mrgcd_data(filename='mrgcddata.txt', schema='mrgccd', logger=None):
    local_path = Path('data', filename).resolve()
    config = get_mrgcd_config(schema='mrgcd', config_file='ftp_config.json')
    ip = config['ip']
    data_path = config['path']
    user = config['username']
    password = config['password']
    print_and_log(f'  Logging into {ip}...', logger)
    remote_dir = Path(data_path).as_posix()
    remote_file = Path(remote_dir, filename).as_posix()
    with FTP(ip) as ftp:
        ftp.login(user=user, passwd=password)
        print_and_log(f'  Saving {remote_file} to {local_path}', logger)
        with local_path.open('wb') as fp:
            ftp.retrbinary(f'RETR {remote_file}', fp.write)

def get_fws_data(url=FWS_URL, filename='fwsdata.txt', logger=None):
    local_path = Path('data', filename).resolve()
    print_and_log(f'  Downloading data from {url}...', logger)
    fws_data = req_get(url)
    if fws_data.status_code == 200:
        print_and_log(f'  Saving {url} to {local_path}', logger)
        with local_path.open('w') as fp:
            fp.write(fws_data.text)

if __name__ == "__main__":
    
    import argparse
    cli_desc = '''
    Downloads MRGCD and FWS data formerly used by ET toolbox and pushes  
    data to UCHDB, uses a site's common name to map site IDs.
    '''
    parser = argparse.ArgumentParser(description=cli_desc)
    parser.add_argument("-V", "--version", help="show program version", action="store_true")
    parser.add_argument("-M", "--mrgcd", help="only write mrgcd data", action="store_true")
    parser.add_argument("-F", "--fws", help="only write fws data", action="store_true")
    args = parser.parse_args()
    
    if args.version:
        print('get_mrgcd.py v1.0')
    gather_all = False
    if not args.mrgcd and not args.fws:
        gather_all = True
        
    s_time = datetime.now()
    bak_date = s_time.strftime('%Y_%m_%d_%H_%M')
    this_dir = path.dirname(path.realpath(__file__)) 
    log_dir = path.join(this_dir, 'logs')
    makedirs(log_dir, exist_ok=True)
    logger = create_log(path.join(this_dir, 'logs', 'get_mrgcd.log'))
    backup = create_backup(path.join(this_dir, 'bak', f'mrgcd.txt'))
    print_and_log(
        f'Starting ET Toolbox to HDB process {s_time:%B %d, %Y %H:%M}...\n', 
        logger
    )
    
    print_and_log('Gathering site list from HDB...\n', logger)
    
    if args.mrgcd or gather_all:
        print_and_log('Working on MRGCD data...', logger)
        mrgcd_data_path = Path('data', 'mrgcddata.txt').resolve()
        get_mrgcd_data(filename='mrgcddata.txt', schema='mrgccd', logger=logger)

    if args.fws or gather_all:
        print_and_log('Working on FWS data...', logger)
        fws_data_path = Path('data', 'fwsdata.txt').resolve()
        get_fws_data(url=FWS_URL, filename='fwsdata.txt', logger=logger)
    
        e_time = datetime.now()
    print_and_log(
        f'\nFinished ET Toolbox to HDB process at {e_time:%B %d, %Y %H:%M}...\n', 
        logger
    )