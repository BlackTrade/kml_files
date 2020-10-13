#! /usr/bin/env python
# -*- coding: utf-8 -*-

import simplekml
from config.config import Config_Kqis, Config
from oracle import OraConnection
from kml_layers.layers import Layers
from processing.processing import Create_processing
from processing.translit import Transliterate
import pandas as pd
import pysftp
import paramiko
import argparse
import logging
import logging.config
import json
import datetime
import sys
import os



def log_init():
    logging.basicConfig(level=logging.INFO)


def log_prepare(path, dir):

    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
            config["handlers"]["info_file_handler"]["filename"] = dir
            logging.config.dictConfig(config)


def log_info(data):
    logger = logging.getLogger('general')
    logger.info(data)


def log_exception(data):
    logger = logging.getLogger('general')
    logger.exception(data)


def args_calculate(args):
    parser = argparse.ArgumentParser(description = '')
    parser.add_argument('-n', type=str, help='Run date format 2020-01-20_00:00:00')
    parser.add_argument('-b', type=str, help='Begin date format YYYYmmdd')
    parser.add_argument('-w', type=str, help='Type work : prod, develop, test')
    parser.add_argument('-g', type=str, help='Path to write log')
    parser.add_argument('-u', type=str, help='User')
    parser.add_argument('-p', type=str, help='Password')
    parser.add_argument('-t', type=str, help='Table name')
    parser.add_argument('-f', type=str, help='Username_sft', default='999')
    parser.add_argument('-a', type=str, help='Password_sft', default='999')

    args_data = parser.parse_args(args=args)

    run_date = ''
    beg_date = ''
    type_work = ''
    log_path = ''
    user = ''
    password = ''
    table_name = ''
    username_sft = ''
    password_sft = ''

    if args_data.n:
        run_date = args_data.n
    if args_data.b:
        beg_date = args_data.b
    if args_data.w:
        type_work = args_data.w
    if args_data.g:
        log_path = args_data.g
    if args_data.u:
        user = args_data.u
    if args_data.p:
        password = args_data.p
    if args_data.t:
        table_name = args_data.t
    if args_data.f:
        username_sft = args_data.f
    if args_data.a:
        password_sft = args_data.a

    return run_date, beg_date, type_work, log_path, user, password, table_name, username_sft, password_sft


def sftp_connection(username_sft, password_sft, path):

    hostname = 'sftp.megafon.ru'

    paramiko.Transport._preferred_kex = ('diffie-hellman-group-exchange-sha256',
                                         'diffie-hellman-group14-sha256',
                                         'diffie-hellman-group-exchange-sha1',
                                         'diffie-hellman-group14-sha1',
                                         'diffie-hellman-group1-sha1')

    cnopts = pysftp.CnOpts(knownhosts=None)
    cnopts.hostkeys = None
    transport = paramiko.Transport((hostname, 22))
    transport.default_window_size = paramiko.common.MAX_WINDOW_SIZE
    transport.packetizer.REKEY_BYTES = pow(2, 40)  # 1TB max, this is a security degradation!
    transport.packetizer.REKEY_PACKETS = pow(2, 40)  # 1TB max, this is a security degradation!
    with pysftp.Connection(host=hostname, username=username_sft, password=password_sft, log=True, port=22,
                           cnopts=cnopts) as sftp:
        print("Connection succesfully stablished ... ")

        local_path = "{path}kml_files/export_kml".format(path=path)
        remote_path = '/kml_files/'

        sftp.put_r(local_path, remote_path)
        #sftp.get(remoteFilePath, localFilePath)
        print(sftp.listdir('/kml_files/'))

def create_kml_parallel(df):

    NAME_REGION = df.REGION_NAME_MF.iloc[0]

    kml = simplekml.Kml(name=NAME_REGION)
    layers = Layers(df, kml)

    layers.create_kqi01_kqi1007_operator()
    layers.create_kqi01_kqi1007_net_type()
    layers.create_bk8(ro_porog)
    layers.create_kqi203_3G_kqi204_4G()
    layers.create_kqi37_kqi207()
    layers.create_kqi46_kqi1052()
    kml.savekmz("{0}/{1}.kmz".format(dirName, Transliterate.transliterate(NAME_REGION)), format=False, )
    log_info("ready kml {0}{1}".format(dirName, NAME_REGION))

    if username_sft !='999':
        sftp_connection(username_sft, password_sft, path)
    else:
        print("test create_kml_parallel")

    return "ready kml {}".format(NAME_REGION)


if __name__ == '__main__':

    result = 0
    try:
        log_init()
        # python kml_files/main.py  -n 2020-01-20_00:00:00 -b 20191028 -w test -g kml_files/logs -u MF_BIGDATA_INFRA_DEV -p DevOraPass -t  pub_ds.f_vigo_clusters_ext

        run_date, beg_date, type_work, log_path, user, password, table_name, username_sft, \
        password_sft = args_calculate(sys.argv[1:])
        run_date_prepared = datetime.datetime.strptime(run_date, '%Y-%m-%d_%H:%M:%S')
        log_conf = sys.path[0]
        gen_in_data_logging_config_path = os.path.join(log_conf, 'log_main.json')
        path_date = datetime.datetime.today().strftime('%Y%m%d_%H%M%S')
        log_dir = log_path + '/' + 'log_main.log'
        path = ''
        if type_work == 'prod':
            path = '/d/app_data/'
        dirName = '{path}kml_files/export_kml/{path_date}'.format(path=path, path_date=path_date)
        try:
            os.makedirs(dirName)
            os.makedirs(log_dir)
            print("Directory ", dirName, " Created ")
        except FileExistsError:
            print("Directory ", dirName, " already exists")

        log_prepare(gen_in_data_logging_config_path, log_dir)

        log_info('Start. {0}'.format(run_date_prepared))
        conf_kqis = Config_Kqis(path)
        conf = Config(path, type_work)
        ora = OraConnection(conf.database, user, password, conf.oracle_home, conf.tns_admin)
        create_processing = Create_processing(ora, conf_kqis, beg_date, conf, path, type_work, table_name)

        df_vigo_mt_final = create_processing.get_df()
        ro_porog = create_processing.get_ro_porog()
        log_info('Finish processing')

        list_region_name = df_vigo_mt_final.REGION_NAME_MF.unique()
        list_df_vigo_mt_final = []
        log_info('Finish split region_name')

        for list_region_name_ in list_region_name:
            df_vigo_mt_final_ = df_vigo_mt_final.query("REGION_NAME_MF == '{}'".format(list_region_name_)).copy()
            list_df_vigo_mt_final.append(df_vigo_mt_final_)

        ######################### multiprocessing #######################
        log_info('Start multiprocessing')
        multiprocessing_type = False

        if multiprocessing_type:
            import multiprocessing
            process_cnt = multiprocessing.cpu_count()
        else:
            import multiprocessing.dummy as multiprocessing
            process_cnt = 4

        processes = min(process_cnt, len(list_df_vigo_mt_final))
        p = multiprocessing.Pool(processes=processes)
        result = p.map(create_kml_parallel, list(list_df_vigo_mt_final))
        p.close()
        p.join()

        log_info('Finish. {0}'.format(run_date_prepared))

    except Exception as e:
        log_exception(e)

