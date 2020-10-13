#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os
import cx_Oracle
import pandas as pd
import re
import datetime

class OraConnection(object):
    def __init__(self,
                 database,
                 username,
                 password,
                 oracle_home,
                 tns_admin,
                 nls_lang="AMERICAN_CIS.UTF8",
                 oracle_sid="DWSTGDB"):
        self.database = database
        self.username = username
        self.password = password
        self.oracle_home = oracle_home
        self.nls_lang = nls_lang
        self.tns_admin = tns_admin
        self.oracle_sid = oracle_sid
        self.con = None

        os.environ["ORACLE_HOME"] = self.oracle_home
        os.environ["NLS_LANG"] = self.nls_lang
        os.environ["TNS_ADMIN"] = self.tns_admin
        os.environ["ORACLE_SID"] = self.oracle_sid


    def get_comments(self, table_name):
        (schema, table) = self.get_schema_table(table_name)
        sql = "select * from DBA_COL_COMMENTS where table_name = '{0}' and owner='{1}'".format(table, schema)
        d = self.read_sql(sql)
        if len(d) == 0:
            return None
        d = d.set_index('COLUMN_NAME')['COMMENTS']
        return dict(d)

    def rename_cols_by_comments(self, df, table_name):
        dc = self.get_comments(table_name)
        if dc is None:
            return
        df.rename(columns=dc, inplace=True)

    def read_table(self, table_name, columns_by_comment=False, *arg, **args):
        df = self._read_sql('select * from {}'.format(table_name), *arg, **args)
        if columns_by_comment:
            self.rename_cols_by_comments(df, table_name)
        return df

    def clear_cache(self):
        import os

        if not os.path.exists(self.cache_folder):
            os.makedirs(self.cache_folder)
        dic = {}
        for file in os.listdir(self.cache_folder):
            if os.path.isfile(self.cache_folder + '/' + file):
                mc = re.match(r'(\d+)_([-a-fA-F0-9]+)\.feather',file)
                if mc:
                    try:
                        print('remove ' + file)
                        os.remove(self.cache_folder + '/' + file)
                    except:
                        pass


    def _cache_check(self):
        import os

        if not os.path.exists(self.cache_folder):
            os.makedirs(self.cache_folder)
        dic = {}
        for file in os.listdir(self.cache_folder):
            if os.path.isfile(self.cache_folder + '/' + file):
                mc = re.match(r'(\d+)_([-A-Fa-f0-9]+)\.feather',file)
                if mc:
                    date_str = mc.group(1)
                    hash_str = mc.group(2)
                    date = datetime.datetime.strptime(date_str, '%Y%m%d%H%M')
                    if date<datetime.datetime.now():
                        try:
                            #print('remove old ' + file)
                            os.remove(self.cache_folder + '/' + file)
                        except:
                            pass
                    else:
                        #print(f'lst {hash_str}')
                        dic[hash_str] = self.cache_folder + '/' + file
        return dic


    def _get_hash(self,sql):
        import hashlib
        hl = hashlib.sha256()
        hl.update(sql.encode('utf-8'))
        return hl.hexdigest()[-16:]


    def _to_cache(self, sql, df, days):
        dic = self._cache_check()
        hash_str = self._get_hash(sql)
        if hash_str in dic.keys():
            os.remove(dic[hash_str])
        new_name = (datetime.datetime.now()+datetime.timedelta(days=days)).strftime('%Y%m%d%H%M') + '_' + hash_str + '.feather'
        df.to_feather(self.cache_folder + '/' + new_name)
        #print('save table to ' + new_name)

    def _from_cache(self, sql):
        dic = self._cache_check()
        hash_str = self._get_hash(sql)
        if hash_str in dic.keys():
            #print('load from cache '+dic[hash_str])
            return pd.read_feather(dic[hash_str])

    def GetConnection(self):
        if self.con is None:
            self.con = cx_Oracle.connect(self.username, self.password, self.database)
        return self.con

    def CloseConnection(self):
        if self.con is not None:
            try:
                self.con.close()
                self.con = None
                return True
            except Exception as ex:
                print('Error on connection closing')
                print(ex)
                return False


    def _read_sql(self, sql:str = None, lower_case = False, use_cache = 0, lob_to_str = True, table_name:str = None):

        if sql is None and table_name is not None:
            sql = 'select * from ' + table_name

        if use_cache > 0:
            df = self._from_cache(sql)
        if use_cache == 0 or df is None:
            try:
                self.con = cx_Oracle.connect(self.username, self.password, self.database)
                df = pd.read_sql(sql, self.con)
                if len(df)>0:
                    if lob_to_str:
                        for col in df.columns:
                            s = df[col]
                            if type(s[~s.isna()][0]) == cx_Oracle.LOB:
                                #print(f'{col} LOB -> str')
                                df[col] = df[col].astype(str)
                if table_name is not None:
                    self.rename_cols_by_comments(df, table_name)
            finally:
                self.CloseConnection()

        if use_cache > 0:  # save if cache use
            self._to_cache(sql, df, use_cache)

        if lower_case:
            df.columns = [c.lower() for c in df.columns]
        return df


    def read_sql(self, sql):
        con = cx_Oracle.connect(self.username, self.password, self.database)
        try:
            df = pd.read_sql(sql, con)
            return df
        finally:
            con.close()


