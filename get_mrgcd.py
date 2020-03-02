# -*- coding: utf-8 -*-
"""
Created on Fri Jan 24 10:08:24 2020

@author: buriona
"""

import json
import logging
from ftplib import FTP
from pathlib import Path
from datetime import datetime
from os import path, makedirs
from logging.handlers import TimedRotatingFileHandler
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
    logger = logging.getLogger('mrgcd rotating log')
    logger.setLevel(logging.INFO)

    handler = TimedRotatingFileHandler(
        log_path,
        when="midnight",
        backupCount=14
    )

    logger.addHandler(handler)

    return logger  

def create_backup(bak_path='mrgcddata.bak.txt'):
    backup = logging.getLogger('mrgcd bak log')
    backup.setLevel(logging.INFO)
    
    handler = TimedRotatingFileHandler(
        bak_path,
        when="midnight",
        backupCount=14
    )

    backup.addHandler(handler)

    return backup  

def get_mrgcd_config(schema='mrgcd', config_file='ftp_config.json'):
    config_path = Path(config_file).resolve()
    with config_path.open('r') as config:
        config_dict = json.load(config)
    return config_dict.get(schema, None)

def get_mrgcd_data(filename='mrgcd.txt', schema='mrgccd', logger=None):
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

def lf_to_crlf(file_path, logger=None):
    WINDOWS_LINE_ENDING = b'\r\n'
    UNIX_LINE_ENDING = b'\n'
    DOUBLE_LINE_ENDING = b'\r\r'
    fp = Path(file_path).resolve()
    if fp.is_file():
        with fp.open('rb') as lf:
            lf_str = lf.read().replace(DOUBLE_LINE_ENDING, b'')
            lf_arr = lf_str.split(UNIX_LINE_ENDING)
            lf_arr[:] = [i for i in lf_arr if not i == '']
            crlf_str = WINDOWS_LINE_ENDING.join(lf_arr)
        with fp.open('wb') as crlf:
            crlf.write(crlf_str)
        print_and_log(f'  Replaced LF with CRLF for {file_path}\n', logger)
    else:
        print_and_log(
            f'  Could not replace LF with CRLF, {file_path} does not exist.\n', 
            logger
        )

def move_data(data_path, to_dir, logger=None):
    src = Path(data_path).resolve()
    dest = Path(to_dir).resolve()
    if src.is_file() and dest.is_dir():
        dest = Path(dest, src.name)
        dest.write_bytes(src.read_bytes())
    print_and_log(
        f'  Succesfully moved data from {src} to {dest}.', 
        logger
    )
    
if __name__ == "__main__":
    
    import argparse
    cli_desc = '''
    Downloads MRGCD and FWS data formerly used by ET toolbox and writes  
    files to folder after formatting for CRLF and CRCRLF line endings
    '''
    parser = argparse.ArgumentParser(description=cli_desc)
    parser.add_argument("-V", "--version", help="show program version", action="store_true")
    parser.add_argument("-M", "--mrgcd", help="only write mrgcd data", action="store_true")
    parser.add_argument("-F", "--fws", help="only write fws data", action="store_true")
    parser.add_argument("-B", "--backup", help="write backup files", action="store_true")
    parser.add_argument("-p", "--path", help="path to write formatted files to")
    
    args = parser.parse_args()
    
    if args.version:
        print('get_mrgcd.py v1.0')
    export_path = None
    if args.path:
        if Path(args.path).is_dir():
            export_path = Path(args.path).resolve()
    s_time = datetime.now()
    bak_date = s_time.strftime('%Y_%m_%d_%H_%M')
    this_dir = path.dirname(path.realpath(__file__)) 
    log_dir = path.join(this_dir, 'logs')
    bak_dir = path.join(this_dir, 'bak')
    data_dir = path.join(this_dir, 'data')
    makedirs(log_dir, exist_ok=True)
    makedirs(bak_dir, exist_ok=True)
    makedirs(data_dir, exist_ok=True)
    logger = create_log(path.join(log_dir, 'get_mrgcd.log'))
    
    if args.mrgcd and not args.fws:
        gather_str = 'MRGCD'
    if args.fws and not args.mrgcd:
        gather_str = 'FWS'
    gather_all = False
    if not args.mrgcd and not args.fws:
        gather_str = 'MRGCD and FWS'
        gather_all = True
        
    print_and_log(
        f'Starting gathering {gather_str} data at {s_time:%B %d, %Y %H:%M}...\n', 
        logger
    )
    
    if args.mrgcd or gather_all:
        print_and_log('Working on MRGCD data...', logger)
        mrgcd_data_path = Path(data_dir, 'mrgcddata.txt').resolve()
        get_mrgcd_data(filename='mrgcddata.txt', schema='mrgcd', logger=logger)
        lf_to_crlf(mrgcd_data_path, logger=logger)
        if export_path:
            print_and_log(
                f'  Moving MRGCD data files to {export_path}...\n', 
                logger
            )
            try:
                move_data(mrgcd_data_path, export_path, logger=logger)
            except Exception as err:
                print_and_log(
                    f'  Error - could not copy file to {export_path} - {err}', 
                    logger
                )
        if args.backup:
            backup_mrgcd = create_backup(
                path.join(bak_dir, f'mrgcddata.bak.txt')
            )
            with mrgcd_data_path.open('r') as bak:
                print_and_log(
                    '  Writting bak file for MRGCD data.', 
                    logger
                )
                write_backup(bak.read(), backup_mrgcd)
                    
    if args.fws or gather_all:
        print_and_log('Working on FWS data...', logger)
        fws_data_path = Path(data_dir, 'fwsdata.txt').resolve()
        get_fws_data(url=FWS_URL, filename='fwsdata.txt', logger=logger)
        lf_to_crlf(fws_data_path, logger=logger)
        if export_path:
            print_and_log(
                f'  Moving data files to {export_path}...\n', 
                logger
            )
            try: 
                move_data(fws_data_path, export_path, logger=logger)
                
            except Exception as err:
                print_and_log(
                    f'  Error - could not copy file to {export_path} - {err}', 
                    logger
                )
        if args.backup:
            backup_fws = create_backup(
                path.join(bak_dir, f'fwsdata.bak.txt')
            )
            with fws_data_path.open('r') as bak:
                print_and_log(
                    '  Writting bak file for FWS data.', 
                    logger
                )
                write_backup(bak.read(), backup_fws, logger=logger)

    e_time = datetime.now()
    print_and_log(
        f'\nFinished gathering {gather_str} data at {e_time:%B %d, %Y %H:%M}...\n', 
        logger
    )