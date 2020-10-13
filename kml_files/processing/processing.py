#! /usr/bin/env python
# -*- coding: utf-8 -*-

import geopandas as gpd
import pandas as pd
import shapely.wkt
from shapely.geometry.polygon import Polygon
from load_data.data import Get_data
import logging
import sys


class Create_processing(object):
    def __init__(self, ora, conf_kqis, beg_date, conf, path, type_work, table_name):
        self.ora = ora
        self.conf_kqis = conf_kqis
        self.beg_date = beg_date
        self.conf = conf
        self.path = path
        self.type_work = type_work
        self.table_name = table_name
        self.get_data = Get_data(ora, self.beg_date, path, self.table_name)


    def get_ro_porog(self):
        df = self.get_data.get_bk8_porog()
        return df


    def get_df(self):

        ro_mt_matching = self.get_data.get_ro_mt_matching()
        df_mt = self.get_data.get_mt()

        df_mt = df_mt.merge(ro_mt_matching, left_on="NAME_MT", right_on="NAME_MT_KML", how="inner")

        df_mt.rename(columns={"GEOMETRY": "geometry"}, inplace=True)
        df_mt['geometry'] = df_mt['geometry'].apply(lambda x: shapely.wkt.loads(x))
        gdf_mt = gpd.GeoDataFrame(df_mt)


        list_kqis_ = [x for x in self.conf_kqis.kqis_list if x not in ['kqi203', 'kqi204']]

        logger = logging.getLogger('general')

        if self.type_work == 'prod' :
            vigo_data = self.get_data.get_data_vigo()
            df_vigo = self.get_data.get_vigo_kqis(vigo_data, ["all", "4G", "3G"], list_kqis_)
            df_vigo_203_3G = self.get_data.get_vigo_kqis(vigo_data, ["3G"], ["kqi203"])
            df_vigo_204_4G = self.get_data.get_vigo_kqis(vigo_data, ["4G"], ["kqi204"])

            logger.info('processing.py/ get_df  prod')

        elif self.type_work == 'develop':
            vigo_data = self.get_data.get_data_vigo()
            df_vigo = self.get_data.get_vigo_kqis(vigo_data, ["all", "4G", "3G"], list_kqis_)
            df_vigo_203_3G = self.get_data.get_vigo_kqis(vigo_data, ["3G"], ["kqi203"])
            df_vigo_204_4G = self.get_data.get_vigo_kqis(vigo_data, ["4G"], ["kqi204"])


            df_vigo = df_vigo.query(
                "REGION_NAME =='Карачаево-Черкесская Республика' or REGION_NAME =='Ивановская область'") \
                .reset_index(drop=True)

            df_vigo_203_3G = df_vigo_203_3G.query("REGION_NAME =='Карачаево-Черкесская Республика' or REGION_NAME =='Ивановская область'") \
                .reset_index(drop=True)

            df_vigo_204_4G = df_vigo_204_4G.query("REGION_NAME =='Карачаево-Черкесская Республика' or REGION_NAME =='Ивановская область'") \
                .reset_index(drop=True)

            logger.info('processing.py/ get_df  develop')

        elif  self.type_work == 'test':
            df_vigo = pd.read_csv('kml_files/test_data/df_vigo_test.csv')
            df_vigo_203_3G = pd.read_csv('kml_files/test_data/df_vigo_203_3G_test.csv')
            df_vigo_204_4G = pd.read_csv('kml_files/test_data/df_vigo_204_4G_test.csv')

            logger.info('processing.py/ get_df  test')

        try:
            df_vigo["OPERATOR_NAME"].replace(to_replace='Мегафон', value='МегаФон', inplace=True)
            df_vigo_203_3G["KQI_CODE"].replace(to_replace="kqi203", value="kqi203_3G", inplace=True)
            df_vigo_203_3G["OPERATOR_NAME"].replace(to_replace='Мегафон', value='МегаФон', inplace=True)
            df_vigo_204_4G["KQI_CODE"].replace(to_replace="kqi204", value="kqi204_4G", inplace=True)
            df_vigo_204_4G["OPERATOR_NAME"].replace(to_replace='Мегафон', value='МегаФон', inplace=True)

            df_vigo = pd.concat([df_vigo.copy(), df_vigo_203_3G, df_vigo_204_4G])

            df_vigo = self._preprocessing_vigo(df_vigo)
            df_vigo_none_mf = self._get_df_vigo_none_mf(df_vigo)

            df_vigo = pd.concat([df_vigo.copy(), df_vigo_none_mf], sort=True)
            gdf_vigo_cluster_id  = self._get_vigo_cluster_id(df_vigo)
            gdf_cluster_id_mt = gpd.sjoin(gdf_vigo_cluster_id, gdf_mt, how="left", op='intersects')

            region_matching = self.get_data.get_region_matching()
            gdf_cluster_id_mt["NAME_MT_MF"].fillna("НЕ попавшие в полигоны МТ", inplace=True)
            gdf_cluster_id_mt["REGION_NAME_MF"] = gdf_cluster_id_mt.apply(lambda row: \
                                                            region_matching.loc[row["REGION_NAME"]][0] \
                                                            if pd.isnull(row["REGION_NAME_MF"]) else row["REGION_NAME_MF"], axis=1)

            df_vigo_mt = df_vigo.merge(gdf_cluster_id_mt[['VIGO_CLUSTER_ID', 'REGION_NAME_MF', 'NAME_MT_MF']], on='VIGO_CLUSTER_ID')

            df_vigo_mt_grp =  df_vigo_mt.groupby(['KQI_CODE', 'REGION_NAME_MF'], as_index=False)['SUMMARY'] \
                                        .agg(['min', 'max']) \
                                        .reset_index()

            df_vigo_mt_scaler = df_vigo_mt.merge(df_vigo_mt_grp, on=['KQI_CODE', 'REGION_NAME_MF'])
            df_vigo_mt_scaler['value_scaler'] = df_vigo_mt_scaler.apply(lambda row: \
                                                1000. * row['SUMMARY'] / (row['max'] - row['min']+0.001),axis=1).copy()

            df_vigo_mt_pivot = df_vigo_mt.pivot_table(index=['KQI_CODE', 'VIGO_CLUSTER_ID', 'NET_TYPE'], \
                                                      columns='OPERATOR_NAME', values='SUMMARY', ) \
                                         .reset_index()


            df_vigo_mt_final = df_vigo_mt_scaler.merge(df_vigo_mt_pivot, on=["KQI_CODE", "VIGO_CLUSTER_ID", "NET_TYPE"])

            df_vigo_mt_final["LAT1"] = df_vigo_mt_final["LAT1"] + 0.0001
            df_vigo_mt_final["LNG1"] = df_vigo_mt_final["LNG1"] + 0.0001
            df_vigo_mt_final["LAT2"] = df_vigo_mt_final["LAT2"] + 0.0001
            df_vigo_mt_final["LNG2"] = df_vigo_mt_final["LNG2"] + 0.0001


            df_vigo_mt_final.drop(["P_START_DATE", "REGION_NAME", "min", "max", "P_START_DATE"], axis=1, inplace=True)
            custom_dict = {'МегаФон': 0, 'Не МегаФон/Йота': 1, 'МТС': 2, 'Билайн': 3, 'Теле2': 4, 'Йота': 5}
            df_vigo_mt_final["sort_OPERATOR_NAME"] = df_vigo_mt_final["OPERATOR_NAME"].apply(lambda x: custom_dict[x])

            df_vigo_mt_final.sort_values(by=["KQI_CODE", "REGION_NAME_MF", "NAME_MT_MF", "sort_OPERATOR_NAME", "NET_TYPE"],
                                             inplace=True)

            df_vigo_mt_final.reset_index(inplace=True, drop=True)
            return df_vigo_mt_final

        except:
            logger = logging.getLogger('general')
            logger.info('processing.py/ get_df  error')
            raise


    def _preprocessing_vigo(self, df):
        try:
            df['LAT1'] = df['LAT1'].apply(lambda x: float(str(x).replace(',', '.')))
            df['LAT2'] = df['LAT2'].apply(lambda x: float(str(x).replace(',', '.')))
            df['LNG1'] = df['LNG1'].apply(lambda x: float(str(x).replace(',', '.')))
            df['LNG2'] = df['LNG2'].apply(lambda x: float(str(x).replace(',', '.')))
            df['SUMMARY'] = df['SUMMARY'].apply(lambda x: float(str(x).replace(',', '.')))

            df[['LAT1', 'LNG1', 'LAT2', 'LNG2', 'SUMMARY']] = df[['LAT1', 'LNG1', 'LAT2', 'LNG2', 'SUMMARY']].astype('float64')
            df.drop(axis=1, columns=['KQI_NAME', 'BAND'], inplace=True)
            df.reset_index(inplace=True, drop=True)
            # df.fillna(0, inplace=True )
            return df

        except:
            logger = logging.getLogger('general')
            logger.info('processing.py/ _preprocessing_vigo  error')
            raise


    def _get_df_vigo_none_mf(self,df):
        try:
            col_agg = ['KQI_CODE', 'VIGO_CLUSTER_ID', 'LAT1', 'LNG1', 'LAT2', 'LNG2', 'NET_TYPE', 'REGION_NAME', 'P_START_DATE']

            df_vigo_none_mf = df.query("KQI_CODE == 'kqi01' or KQI_CODE == 'kqi1007'") \
                                .query("OPERATOR_NAME != 'МегаФон' and OPERATOR_NAME != 'Йота'") \
                                .groupby(col_agg, as_index=False)['SUMMARY'] \
                                .sum(skipna=True)

            df_vigo_none_mf.reset_index(inplace=True, drop=True)
            df_vigo_none_mf["OPERATOR_NAME"] = "Не МегаФон/Йота"
            df_vigo_none_mf.rename(columns={'sum': 'SUMMARY'}, inplace=True)
            return df_vigo_none_mf
        except:
            logger = logging.getLogger('general')
            logger.info('processing.py/ _get_df_vigo_none_mf  error')
            raise


    def _poligon_vigo(self,locs):
        try:
            Lat1, Lng1, Lat2, Lng2 = locs
            geometry = Polygon(
                [(Lng1, Lat1),
                 (Lng1, Lat2),
                 (Lng2, Lat2),
                 (Lng2, Lat1)])
            return geometry
        except:
            logger = logging.getLogger('general')
            logger.info('processing.py/ _poligon_vigo  error')
            raise

    def _get_vigo_cluster_id(self,df):
        try:
            df_ = df[['VIGO_CLUSTER_ID', 'LAT1', 'LNG1', 'LAT2', 'LNG2', 'REGION_NAME']] \
                                        .drop_duplicates() \
                                        .reset_index(drop=True)

            df_['geometry'] = df_.apply(lambda row: self._poligon_vigo(row[['LAT1', 'LNG1', 'LAT2', 'LNG2']]), axis=1)
            df_.drop(columns=['LAT1', 'LNG1', 'LAT2', 'LNG2'], inplace=True, axis=1)
            gdf = gpd.GeoDataFrame(df_)
            return gdf
        except:
            logger = logging.getLogger('general')
            logger.info('processing.py/ _get_vigo_cluster_id  error')
            raise

