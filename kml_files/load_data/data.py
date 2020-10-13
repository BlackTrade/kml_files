#! /usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import datetime
from datetime import timedelta
import logging

class Get_data(object):
    def __init__(self, ora, vigo_date, path, table_name):
        self.ora = ora
        self.path = path
        self.vigo_date = vigo_date
        self.table_name = table_name

    def get_region_matching(self):

        df = pd.read_csv('{path}kml_files/load_data/reference/region_matching.csv'.format(path=self.path),  sep=',')
        df.set_index("REGION_NAME_VIGO", inplace=True)
        return df


    def get_ro_mt_matching(self):
        df = pd.read_csv('{path}kml_files/load_data/reference/ro_mt_matching_final.csv'
                         .format(path=self.path), sep=',')
        return df

    def get_bk8_porog(self):
        ro_porog = pd.read_csv("{path}kml_files/load_data/reference/bk8_porog.csv".format(path=self.path), sep='\t')
        ro_porog["TRG"] = ro_porog["TRG"].apply(lambda x: float(str(x).replace(',', '.')))
        ro_porog["TRG"] = ro_porog["TRG"].astype('float64')

        # нужно следить за тем какие поле в отданном файле от заказчика

        region_matching = self.get_region_matching()
        ro_porog = ro_porog.merge(region_matching, left_on="REGION_NAME", right_index=True)
        ro_porog.set_index("REGION_NAME_KML", inplace=True)
        return ro_porog


    def get_mt(self):
        #df = self.ora.read_table("tmp_pk_shp_file")
        df = pd.read_pickle("{path}kml_files/load_data/reference/df_mt.pkl".format(path=self.path))
        return df

    def get_vigo_kqis(self, vigo_date, list_net_type, list_kqis):
        # pub_ds.f_vigo_clusters_ext
        query = """select  /*+parallel(8)*/
                                KQI_NAME,
                                KQI_CODE,
                                VIGO_CLUSTER_ID,                        
                                LAT1,LNG1,LAT2,LNG2,
                                OPERATOR_NAME,
                                NET_TYPE,
                                BAND,
                                SUMMARY,
                                REGION_NAME,
                                P_START_DATE 
                    from {0}
                    where P_START_DATE  = '{1}'
                    and  OPERATOR_NAME in ('Мегафон','Теле2', 'Билайн', 'МТС','Йота' )
                    and  BAND = 'all' and  NET_TYPE in ('{2}')
                    and  KQI_CODE in ('{3}')"""

        list_kqis_str = "','".join(list_kqis)
        list_net_type_str = "','".join(list_net_type)
        try:
            df = self.ora.read_sql(query.format(self.table_name, vigo_date, list_net_type_str, list_kqis_str))
            return df
        except:
            logger = logging.getLogger('general')
            logger.info('data.py/ get_vigo_kqis error')
            raise


    def get_data_vigo(self):
        try:
            date_time_obj = datetime.datetime.strptime(self.vigo_date, '%Y%m%d')

            days_to_subtract = 7
            date_time_obj_ = date_time_obj - timedelta(days=days_to_subtract)
            vigo_data_start = date_time_obj_.strftime('%Y%m%d')

            # pub_ds.f_vigo_clusters_ext
            list_period = self.ora.read_sql( """select /*+parallel(8)*/ distinct P_START_DATE 
                                  from {0} 
                                  where P_START_DATE >= '{1}' and P_START_DATE < '{2}'
                                  and  KQI_CODE = 'kqi01'""".format(self.table_name, vigo_data_start, self.vigo_date))

            return [x[0] for x in list_period.values][0]
        except:
            logger = logging.getLogger('general')
            logger.info('data.py/ get_data_vigo error')
            raise




