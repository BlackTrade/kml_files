#! /usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import simplekml
import logging


level1_kml_dict = {}
level2_kml_dict = {}
level3_kml_dict = {}
level4_kml_dict = {}
level5_kml_dict = {}

HIGH = 250
VALUE_COL_NAME = "value_scaler"

kqi_name_dict = {
    "kqi01": "kqi01  Кол-во просмотров (шт.)",
    "kqi1007": "kqi1007 Уник. абонентов (шт.)",
    "bk-8": "bk-8   Длит-ть первой буферизации (сек.): Общая выше 3",
    "kqi37": "kqi37  Длит-ть первой буферизации (сек.)",
    "kqi46": "kqi46  Трафик всего (Гб)",
    "kqi203_3G": "kqi203_3G  Доля измерений RSSI >= -97 dBm",
    "kqi204_4G": "kqi204_4G  Доля измерений RSRP >= -110 dBm",
    "kqi207": "kqi207 Доля уник. 4G устр-в, которые были в 4G слое",
    "kqi1052": "kqi1052 Кол-во уникальных сот (шт.)",
    "kqi1054": "kqi1054 Недоступность сети (мин.)"
}

kqi_color_dict = {
    "kqi01": "99f4e3c9",
    "kqi1007": "99f8ecdb",
    "bk-8": "99aa9fe3",
    "kqi37": "99bbb2e8",
    "kqi203_3G": "99dd8cdc",
    "kqi204_4G": "99e39fe2",
    "kqi207": "99e3a09f",
    "kqi46": "99e8b3b2",
    "kqi1052": "99dee39f",
    "kqi1054": "99e4e8b2"
}

class Layers(object):
    def __init__(self, df, kml):
        self.df = df
        self.kml = kml


    def create_kqi46_kqi1052(self):

        df_ = self.df.query("KQI_CODE == 'kqi46' or KQI_CODE == 'kqi1052'") \
            .query("NET_TYPE == 'all'") \
            .query("OPERATOR_NAME == 'МегаФон'")

        for i, row in df_.iterrows():
            level1 = row['REGION_NAME_MF']
            level2 = row['NAME_MT_MF']
            level3 = kqi_name_dict[row['KQI_CODE']]

            if level1 in level1_kml_dict.keys():
                level1_kml = level1_kml_dict[level1]
            else:
                level1_kml = self.kml.newfolder(name=level1)
                level1_kml_dict.update({level1_kml.name: level1_kml})

            if level1 + level2 in level2_kml_dict.keys():
                level2_kml = level2_kml_dict[level1 + level2]
            else:
                level2_kml = level1_kml.newfolder(
                    name=level2)  # REGION_kml.newfolder создает вложенную папку внурти REGION_kml
                level2_kml_dict.update({level1_kml.name + level2_kml.name: level2_kml})

            if level1 + level2 + level3 in level3_kml_dict.keys():
                level3_kml = level3_kml_dict[level1 + level2 + level3]
            else:
                level3_kml = level2_kml.newfolder(name=level3)
                level3_kml_dict.update({level1_kml.name + level2_kml.name + level3_kml.name: level3_kml})

            pol = level3_kml.newpolygon(name=row['VIGO_CLUSTER_ID'])

            Lat1, Lng1, Lat2, Lng2, value = row[['LAT1', 'LNG1', 'LAT2', 'LNG2', VALUE_COL_NAME]]
            # если строим скалированные значения то нужно увеличить их что не строить куб высотой в 100 метров + HIGH

            value = value * 15 + HIGH
            list_geometry = [(Lng1, Lat1, value),
                             (Lng1, Lat2, value),
                             (Lng2, Lat2, value),
                             (Lng2, Lat1, value),
                             (Lng1, Lat1, value)]

            color_ = kqi_color_dict[row['KQI_CODE']]
            xml_text_ = self._create_xml_text(row)

            pol = self._make_polygon(pol, list_geometry, 1, simplekml.AltitudeMode.relativetoground, color_, xml_text_)


    def create_kqi37_kqi207(self):

        df_ = self.df.query("KQI_CODE == 'kqi37' or KQI_CODE == 'kqi207'") \
            .query("NET_TYPE == 'all'") \
            .query("OPERATOR_NAME == 'МегаФон'")

        for i, row in df_.iterrows():
            value_Megafon, value_MTS, value_Beeline, value_Tele2 = \
            self._get_value_operator(row, "МегаФон", np.nan), \
            self._get_value_operator(row, "МТС", np.nan), \
            self._get_value_operator(row, "Билайн", np.nan), \
            self._get_value_operator(row, "Теле2", np.nan)

            if row['KQI_CODE'] == "kqi37":
                lider_kqi = np.nanmin([value_Megafon, value_MTS, value_Beeline, value_Tele2])
            else:
                lider_kqi = np.nanmax([value_Megafon, value_MTS, value_Beeline, value_Tele2])

            if np.isnan(row['SUMMARY']):
                if row['KQI_CODE'] == "kqi37":
                    mf_kqi = np.inf
                else:
                    mf_kqi = 0
            else:
                mf_kqi = row['SUMMARY']

            if mf_kqi == lider_kqi:  # row['SUMMARY'] > lider_kqi*1.10 or
                level4 = "МФ лидер"
                color_ = "CC00ff00"
            elif mf_kqi < lider_kqi * 0.90:
                level4 = "Хуже лидера на 10%"
                color_ = "CC0000ff"
            else:
                level4 = "В пределах +/-10% с лидером"
                color_ = "CC00ffff"

            pol = self._create_level_schema(self.kml, row, kqi_name_dict[row['KQI_CODE']], level4, row['VIGO_CLUSTER_ID'])

            Lat1, Lng1, Lat2, Lng2, value = row[['LAT1', 'LNG1', 'LAT2', 'LNG2', VALUE_COL_NAME]]
            # если строим скалированные значения то нужно увеличить их что не строить куб высотой в 100 метров + HIGH

            value = 0  # плоские
            list_geometry = [(Lng1, Lat1, value),
                             (Lng1, Lat2, value),
                             (Lng2, Lat2, value),
                             (Lng2, Lat1, value),
                             (Lng1, Lat1, value)]

            xml_text_ = self._create_xml_text(row)
            pol = self._make_polygon(pol, list_geometry, 0, simplekml.AltitudeMode.clamptoground, color_, xml_text_)


    def create_kqi203_3G_kqi204_4G(self):
        xml_text = """<b><font size="+2">{0}</font></b>
                      <br/><font face="Arial Black">{1} </font>{2}</b>"""

        df_ = self.df.query("KQI_CODE == 'kqi203_3G' or KQI_CODE == 'kqi204_4G'")

        for i, row in df_.iterrows():

            level1 = row['REGION_NAME_MF']
            level2 = row['NAME_MT_MF']
            level3 = kqi_name_dict[row['KQI_CODE']]

            if level1 in level1_kml_dict.keys():
                level1_kml = level1_kml_dict[level1]
            else:
                level1_kml = self.kml.newfolder(name=level1)
                level1_kml_dict.update({level1_kml.name: level1_kml})

            if level1 + level2 in level2_kml_dict.keys():
                level2_kml = level2_kml_dict[level1 + level2]
            else:
                level2_kml = level1_kml.newfolder(
                    name=level2)  # REGION_kml.newfolder создает вложенную папку внурти REGION_kml
                level2_kml_dict.update({level1_kml.name + level2_kml.name: level2_kml})

            if level1 + level2 + level3 in level3_kml_dict.keys():
                level3_kml = level3_kml_dict[level1 + level2 + level3]
            else:
                level3_kml = level2_kml.newfolder(name=level3)
                level3_kml_dict.update({level1_kml.name + level2_kml.name + level3_kml.name: level3_kml})

            level4 = row['OPERATOR_NAME']
            if level1 + level2 + level3 + level4 in level4_kml_dict.keys():
                level4_kml = level4_kml_dict[level1 + level2 + level3 + level4]
            else:
                level4_kml = level3_kml.newfolder(name=level4)
                level4_kml_dict.update({level1_kml.name + level2_kml.name + level3_kml.name + level4_kml.name: level4_kml})

                # 50 красный <= 80 жёлтый <= 100 зелёный
            if row['SUMMARY'] > 80:
                level5 = "kqi>80"
                color_ = "CC00ff00"
            elif row['SUMMARY'] < 50:
                level5 = "kqi<50"
                color_ = "CC0000ff"
            else:
                level5 = "50<kqi<80"
                color_ = "CC00ffff"

            if level1 + level2 + level3 + level4 + level5 in level5_kml_dict.keys():
                level5_kml = level5_kml_dict[level1 + level2 + level3 + level4 + level5]
            else:
                level5_kml = level4_kml.newfolder(name=level5)
                level5_kml_dict.update(
                    {level1_kml.name + level2_kml.name + level3_kml.name + level4_kml.name + level5_kml.name: level5_kml})

            pol = level5_kml.newpolygon(name=row['VIGO_CLUSTER_ID'])

            Lat1, Lng1, Lat2, Lng2, value = row[['LAT1', 'LNG1', 'LAT2', 'LNG2', VALUE_COL_NAME]]
            # если строим скалированные значения то нужно увеличить их что не строить куб высотой в 100 метров + HIGH

            value = 0  # плоские
            list_geometry = [(Lng1, Lat1, value),
                             (Lng1, Lat2, value),
                             (Lng2, Lat2, value),
                             (Lng2, Lat1, value),
                             (Lng1, Lat1, value)]

            xml_text_ = xml_text.format(row['VIGO_CLUSTER_ID'], row['OPERATOR_NAME'], \
                                        "н/д" if np.isnan(row['SUMMARY']) else np.round(row['SUMMARY'], 2))

            pol = self._make_polygon(pol, list_geometry, 0, simplekml.AltitudeMode.clamptoground, color_, xml_text_)


    def create_bk8(self, ro_porog):
        xml_text = """<b><font size="+2">{0}</font></b>
                      <br/><font face="Arial Black">{1} </font>{2}</b>
                      <br/><font face="Arial Black">{3} </font>{4}</b>"""

        df_ = self.df.query("KQI_CODE == 'bk-8'") \
            .query("NET_TYPE == 'all'") \
            .query("OPERATOR_NAME == 'МегаФон'")

        for i, row in df_.iterrows():

            level3 = kqi_name_dict[row['KQI_CODE']]
            ro_porog_ = ro_porog.loc[row["REGION_NAME_MF"]]["TRG"]

            if np.isnan(row['SUMMARY']):
                mf_kqi = np.inf
            else:
                mf_kqi = row['SUMMARY']

            # красим
            if mf_kqi < ro_porog_:
                color_ = "CC00ff00"  # "МФ выполняет таргет"
                level4 = "МФ лучше порога для РО"  # "МФ выполняет таргет"
            else:
                color_ = "CC0000ff"
                level4 = "МФ хуже порога для РО"

            pol = self._create_level_schema(self.kml, row, level3, level4, row['VIGO_CLUSTER_ID'])

            Lat1, Lng1, Lat2, Lng2, value = row[['LAT1','LNG1','LAT2','LNG2',VALUE_COL_NAME]]
            # если строим скалированные значения то нужно увеличить их что не строить куб высотой в 100 метров + HIGH

            value = value * 15 + HIGH
            list_geometry = [(Lng1, Lat1, value),
                             (Lng1, Lat2, value),
                             (Lng2, Lat2, value),
                             (Lng2, Lat1, value),
                             (Lng1, Lat1, value)]

            xml_text_ = xml_text.format(row['VIGO_CLUSTER_ID'], \
                                        "МегаФон", "н/д" if np.isnan(row['МегаФон']) else np.round(row['МегаФон'], 2),
                                        "Порог РО", "н/д" if np.isnan(ro_porog_) else np.round(ro_porog_, 2))

            pol = self._make_polygon(pol, list_geometry, 1, simplekml.AltitudeMode.relativetoground, color_, xml_text_)


    def create_kqi01_kqi1007_net_type(self):

        df_ = self.df.query("KQI_CODE == 'kqi01' or KQI_CODE == 'kqi1007'") \
            .query("OPERATOR_NAME == 'МегаФон'")

        for i, row in df_.iterrows():
            pol = self._create_level_schema(self.kml, row, kqi_name_dict[row['KQI_CODE']] + " net_type", \
                                      row['NET_TYPE'], row['VIGO_CLUSTER_ID'])

            Lat1, Lng1, Lat2, Lng2, value = row[['LAT1', 'LNG1', 'LAT2', 'LNG2', VALUE_COL_NAME]]
            # если строим скалированные значения то нужно увеличить их что не строить куб высотой в 100 метров + HIGH

            value = value * 15 + HIGH
            list_geometry = [(Lng1, Lat1, value),
                             (Lng1, Lat2, value),
                             (Lng2, Lat2, value),
                             (Lng2, Lat1, value),
                             (Lng1, Lat1, value)]

            color_ = kqi_color_dict[row['KQI_CODE']]

            xml_text_ = self._create_xml_text(row)
            pol = self._make_polygon(pol, list_geometry, 1, simplekml.AltitudeMode.relativetoground, color_, xml_text_)


    def create_kqi01_kqi1007_operator(self):

        xml_text1 = """<b><font size="+2">{0}</font></b>
                      <br/><font face="Arial Black">{1} </font>{2}</b>"""

        xml_text2 = """<b><font size="+2">{0}</font></b>
                      <br/><font face="Arial Black">{1} </font>{2}</b>
                      <br/><font face="Arial Black">{3} </font>{4}</b>
                      <br/><font face="Arial Black">{5} </font>{6}</b>
                      <br/><font face="Arial Black">{7} </font>{8}</b>"""

        df_ = self.df.query("KQI_CODE == 'kqi01' or KQI_CODE == 'kqi1007'") \
                              .query("NET_TYPE =='all'")

        for i, row in df_.iterrows():

            pol = self._create_level_schema(self.kml, row, kqi_name_dict[row['KQI_CODE']] +" operator",\
                                      row['OPERATOR_NAME'], row['VIGO_CLUSTER_ID'])

            Lat1, Lng1, Lat2, Lng2, value = row[['LAT1','LNG1','LAT2','LNG2',VALUE_COL_NAME]]
            # если строим скалированные значения то нужно увеличить их что не строить куб высотой в 100 метров + HIGH

            value = value*15 + HIGH
            list_geometry = [(Lng1, Lat1, value ),
                             (Lng1, Lat2, value ),
                             (Lng2, Lat2, value ),
                             (Lng2, Lat1, value ),
                             (Lng1, Lat1, value )]

            if  row['OPERATOR_NAME'] == "МегаФон": # row['SUMMARY'] > lider_kqi*1.10 or
                color_ = "CC00ff00"
            elif row['OPERATOR_NAME'] == "МТС":
                color_ = "CC0000ff"
            elif row['OPERATOR_NAME'] == "Билайн":
                color_ = "CC00ffff"
            elif row['OPERATOR_NAME'] == "Йота":
                color_ = "80ff0000"
            elif row['OPERATOR_NAME'] == "Теле2":
                color_ = "80000000"
            else:  # НеМегаФон/Йота
                color_ = "80d7ebfa"

            #print (row)
            if row['OPERATOR_NAME'] == "Не МегаФон/Йота":
                xml_text_ = xml_text2.format(row['VIGO_CLUSTER_ID'], \
                              "Не МегаФон/Йота", "н/д" if np.isnan(row['SUMMARY']) else np.round(row['SUMMARY'],2),
                              "МТС",      self._get_value_operator(row, "МТС"),
                              "Билайн",   self._get_value_operator(row, "Билайн"),
                              "Теле2",    self._get_value_operator(row, "Теле2") )
            else:
                xml_text_ = xml_text1.format(row['VIGO_CLUSTER_ID'], row['OPERATOR_NAME'], \
                                             "н/д" if np.isnan(row['SUMMARY']) else np.round(row['SUMMARY'],2))

            pol = self._make_polygon (pol, list_geometry, 1, simplekml.AltitudeMode.relativetoground, color_, xml_text_)


    def _create_level_schema(self, kml, row, level3_, level4_, name_pol):
        level1 = row['REGION_NAME_MF']
        level2 = row['NAME_MT_MF']

        if level1 in level1_kml_dict.keys():
            level1_kml = level1_kml_dict[level1]
        else:
            level1_kml = kml.newfolder(name=level1)
            level1_kml_dict.update({level1_kml.name: level1_kml})

        if level1 + level2 in level2_kml_dict.keys():
            level2_kml = level2_kml_dict[level1 + level2]
        else:
            level2_kml = level1_kml.newfolder(name=level2)  # REGION_kml.newfolder создает вложенную папку внурти REGION_kml
            level2_kml_dict.update({level1_kml.name + level2_kml.name: level2_kml})

        level3 = level3_
        if level1 + level2 + level3 in level3_kml_dict.keys():
            level3_kml = level3_kml_dict[level1 + level2 + level3]
        else:
            level3_kml = level2_kml.newfolder(name=level3)
            level3_kml_dict.update({level1_kml.name + level2_kml.name + level3_kml.name: level3_kml})

        level4 = level4_
        if level1 + level2 + level3 + level4 in level4_kml_dict.keys():
            level4_kml = level4_kml_dict[level1 + level2 + level3 + level4]
        else:
            level4_kml = level3_kml.newfolder(name=level4)
            level4_kml_dict.update({level1_kml.name + level2_kml.name + level3_kml.name + level4_kml.name: level4_kml})

        pol = level4_kml.newpolygon(name=name_pol)
        return pol


    def _make_polygon(self, pol, list_geometry_, extrude_, altitudemode_, color_, xml_text_):
        pol.outerboundaryis = list_geometry_
        pol.extrude = extrude_
        pol.altitudemode = altitudemode_
        pol.tessellate = 1

        pol.stylemap.normalstyle.polystyle.color = color_
        pol.stylemap.normalstyle.linestyle.color = color_
        pol.stylemap.normalstyle.linestyle.width = 1

        pol.stylemap.highlightstyle.polystyle.color = color_
        pol.stylemap.highlightstyle.linestyle.color = simplekml.Color.yellow
        pol.stylemap.highlightstyle.linestyle.width = 4

        pol.stylemap.highlightstyle.balloonstyle.text = xml_text_
        pol.stylemap.highlightstyle.balloonstyle.bgcolor = simplekml.Color.whitesmoke
        pol.stylemap.highlightstyle.balloonstyle.textcolor = simplekml.Color.black

        return pol


    def _create_xml_text(self, row):
        try:
            xml_text_ = """<b><font size="+2">{0}</font></b>
                          <br/><font face="Arial Black">{1} </font>{2}</b>
                          <br/><font face="Arial Black">{3} </font>{4}</b>
                          <br/><font face="Arial Black">{5} </font>{6}</b>
                          <br/><font face="Arial Black">{7} </font>{8}</b>
                          <br/><font face="Arial Black">{9} </font>{10}</b>"""

            xml_text = xml_text_.format(row['VIGO_CLUSTER_ID'],
                                            "МегаФон", self._get_value_operator(row, "МегаФон"),
                                            "МТС",     self._get_value_operator(row, "МТС"),
                                            "Билайн",  self._get_value_operator(row, "Билайн"),
                                            "Йота",    self._get_value_operator(row, "Йота"),
                                            "Теле2",   self._get_value_operator(row, "Теле2"))
            return xml_text
        except:
            logger = logging.getLogger('general')
            logger.info('layers.py/ _create_xml_text error')
            raise


    def _get_value_operator(self, row, operator_name, replace_mode = "н/д"):
        try:
            if np.isnan(row[operator_name]):
                value_operator = replace_mode
            else:
                value_operator = np.round(row[operator_name], 2)
        except:
            value_operator = replace_mode
            pass

        return value_operator

