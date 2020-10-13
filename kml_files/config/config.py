#! /usr/bin/env python
# -*- coding: utf-8 -*-

import yaml
import sys
#print('\n'.join(k for k in (sys.path)))

class Config(object):
    def __init__(self, path, type_work):
        self.type_work = type_work
        self.path = path
        self.conf = self._loadConf()

        if self.type_work == 'prod':
            self.database = self.conf['type_work_prod']['oracle']['database']
            self.oracle_home = self.conf['type_work_prod']['oracle']['oracle_home']
            self.tns_admin = self.conf['type_work_prod']['oracle']['tns_admin']
            self.path_result = self.conf['type_work_prod']['path_result']

        elif self.type_work == 'test':
             self.database = self.conf['type_work_test']['oracle']['database']
             self.oracle_home = self.conf['type_work_test']['oracle']['oracle_home']
             self.tns_admin = self.conf['type_work_test']['oracle']['tns_admin']
             self.path_result = self.conf['type_work_test']['path_result']

        elif self.type_work == 'develop':
             self.database = self.conf['type_work_develop']['oracle']['database']
             self.oracle_home = self.conf['type_work_develop']['oracle']['oracle_home']
             self.tns_admin = self.conf['type_work_develop']['oracle']['tns_admin']
             self.path_result = self.conf['type_work_develop']['path_result']


        else:
             print('Config path not found')

    def _loadConf(self):

        stream = open('{path}kml_files/config/config.yaml'.format(path=self.path), 'r')
        conf = yaml.safe_load(stream)
        stream.close()
        return conf

class Config_Kqis(object):
    def __init__(self, path):
        self.path = path
        self.conf = self._loadConfKqis()
        self.kqis_list = self.conf['kqis_list']

    def _loadConfKqis(self):
        print(self.path)
        stream = open('{path}kml_files/config/kqis.yaml'.format(path=self.path), 'r')
        conf = yaml.safe_load(stream)
        stream.close()
        return conf
