# -*- coding: UTF-8 -*-
# -------------------------------------------------------------------------------
# Name:          Approximation
# Purpose:
#
# Author:         ciluser
#
# Created:        2015/4/23
# Copyright:    (c) ciluser 2015
# Licence:        <your licence>
# -------------------------------------------------------------------------------
import json
import pyodbc

from ContingencyTable22 import *

connect_information = "Trusted_Connection=yes;driver={SQL Server};server=localhost"
characteristic_type = ["similarity", "tolerance"]
attribute_type = ["age", "gender", "global"]


class RSContingencyTable:
    def __init__(self):
        self.data = {"Xal": set(), "Xau": set(), "Xbl": set(), "Xbu": set(),
                     "Xcl": set(), "Xcu": set(), "Xdl": set(), "Xdu": set()}

    def add(self, target, relation):
        self.data[target].add(relation)

    def get_list(self):
        temp = {}
        for x in self.data.keys():
            temp[x] = list(self.data[x])
        return temp


def getContingencyTable(input_table=None, in_drug=None, in_symptom=None, in_age=None, in_gender=None):
    """2*2列聯表，在intput_table給定事先完成的JSON檔路徑，或輸入後方四個參數求得。
    :param input_table:JSON檔路徑
    :param in_drug:藥名，list
    :param in_symptom:不良反應，list
    :param in_age:年齡(~5、18~60、60~)，list
    :param in_gender:性別(Female、Male、None)
    :return:字典物件，包含a、b、c、d四個key，每個set裡包含其符合的id。
    """
    if input_table:
        with open(input_table, "r") as f:
            cont_table = json.loads(f.read())
    else:
        cont_table = find(in_drug, in_symptom, in_age, in_gender)
    print("===contingency table===")
    print("Xa:%d\tXb:%d\tXc:%d\tXd:%d\t" %
          (len(cont_table["Xa"]), len(cont_table["Xb"]), len(cont_table["Xc"]), len(cont_table["Xd"])))
    return cont_table


def getCharacteristicSet(charact, attr):
    """從資料庫中抓取事先建立好的age、gender的cube。
    :param charact: "similarity"或"tolerance"
    :param attr: "age"或"gender"或"global"
    :return: 字典物件，每個age與gender的組合。
    """
    if charact not in characteristic_type or attr not in attribute_type:
        raise AttributeError("ERROR:characteristic set")
    char_set = {}
    with pyodbc.connect(connect_information, database="RSADRs") as con:
        with con.cursor() as cursor:
            rows = cursor.execute("SELECT * FROM %s" % characteristic)
            for row in rows:
                attr_type = "%s_%s" % (row[0], row[1])
                char_set[attr_type] = set()
                for s in row[2:]:
                    for i in s.split(","):
                        try:
                            char_set[attr_type].add(int(i))
                        except ValueError:
                            pass
    print("===%s===" % characteristic)
    for k in char_set.keys():
        print("%s:\t%d" % (k, len(char_set[k])))
    return char_set


def singleton(characteristic, season, input_table=None,
              in_drug=None, in_symptom=None, in_age=None, in_gender=None):
    # get contingency table.
    if input_table:
        with open(input_table, "r") as f:
            cont_table = json.loads(f.read())
    else:
        cont_table = find(in_drug, in_symptom, in_age, in_gender)
    for k in cont_table.keys():
        cont_table[k] = set(cont_table[k])
    print("===contingency table===")
    print("Xa:%d\tXb:%d\tXc:%d\tXd:%d\t" %
          (len(cont_table["Xa"]), len(cont_table["Xb"]), len(cont_table["Xc"]), len(cont_table["Xd"])))

    # get characteristic set
    charact, attr = characteristic.split("_")
    if charact not in characteristic_type or attr not in attribute_type:
        raise AttributeError("ERROR:characteristic set")
    char_set = {}
    with pyodbc.connect(connect_information, database="RSADRs") as con:
        with con.cursor() as cursor:
            rows = cursor.execute("SELECT * FROM %s" % characteristic)
            for row in rows:
                attr_type = "%s_%s" % (row[0], row[1])
                char_set[attr_type] = set()
                for s in row[2:]:
                    for i in s.split(","):
                        try:
                            char_set[attr_type].add(int(i))
                        except ValueError:
                            pass
    print("===%s===" % characteristic)
    for k in char_set.keys():
        print("%s:\t%d" % (k, len(char_set[k])))
    # building RS table
    output = {}
    con = pyodbc.connect(connect_information, database="RSADRs")
    for s in season:
        print("RS table:%s" % s)
        output[s] = RSContingencyTable()
        with con.cursor() as search:
            rows = search.execute("""SELECT dp.ID,dp.relation,total.age,total.gender FROM DPCharacteristic as dp
            INNER JOIN totalFAERS as total ON dp.ID=total.ID AND total.season='%s'""" % s)
            for ID, relation, age, gender in rows:
                r_set = set([int(i) for i in relation.split(",")]) & char_set["%s_%s" % (age, gender)]
                print("%d:%d\r" % (ID, len(r_set))),
                for x in cont_table.keys():
                    if r_set <= cont_table[x]:  # relation set is subset of X in contingency table
                        output[s].add(x+'l', ID)
                    if r_set & cont_table[x]:  # Intersection relation set and X in contingency table isn't empty
                        output[s].add(x+'u', ID)
    con.close()
    result = {}
    for s in season:
        result[s] = output[s].get_list()
    return result


def concept(characteristic, season, input_table=None,
            in_drug=None, in_symptom=None, in_age=None, in_gender=None):
    # get contingency table.
    if input_table:
        with open(input_table, "r") as f:
            cont_table = json.loads(f.read())
    else:
        cont_table = find(in_drug, in_symptom, in_age, in_gender)
    for k in cont_table.keys():
        cont_table[k] = set(cont_table[k])
    print("===contingency table===")
    print("Xa:%d\tXb:%d\tXc:%d\tXd:%d\t" %
          (len(cont_table["Xa"]), len(cont_table["Xb"]), len(cont_table["Xc"]), len(cont_table["Xd"])))

    # get characteristic set
    charact, attr = characteristic.split("_")
    if charact not in characteristic_type or attr not in attribute_type:
        raise AttributeError("ERROR:characteristic set")
    char_set = {}
    with pyodbc.connect(connect_information, database="RSADRs") as con:
        with con.cursor() as cursor:
            rows = cursor.execute("SELECT * FROM %s" % characteristic)
            for row in rows:
                attr_type = "%s_%s" % (row[0], row[1])
                char_set[attr_type] = set()
                for s in row[2:]:
                    for i in s.split(","):
                        try:
                            char_set[attr_type].add(int(i))
                        except ValueError:
                            pass
    print("===%s===" % characteristic)
    for k in char_set.keys():
        print("%s:\t%d" % (k, len(char_set[k])))
    # building concept RS table
    output = {}
    con = pyodbc.connect(connect_information, database="RSADRs")
    for s in season:
        print("RS table:%s" % s)
        output[s] = RSContingencyTable()
        with con.cursor() as search:
            rows = search.execute("""SELECT dp.ID,dp.relation,total.age,total.gender FROM DPCharacteristic as dp
            INNER JOIN totalFAERS as total ON dp.ID=total.ID AND total.season='%s'""" % s)
            temp = {}
            for ID, relation, age, gender in rows:
                temp[ID] = ([int(i) for i in relation.split(",")], age, gender)
            season_ids = set(temp.keys())
        for x in cont_table.keys():  # Xa, Xb, Xc, Xd
            season_cont = season_ids & cont_table[x]
            for case in list(season_cont):
                relation, age, gender = temp[case]
                r_set = set(relation) & char_set["%s_%s" % (age, gender)]
                print("%d:%d\r" % (ID, len(r_set))),
                if r_set <= cont_table[x]:  # relation set is subset of X in contingency table
                    for i in r_set:
                        output[s].add(x+'l', i)
                if r_set & cont_table[x]:  # Intersection relation set and X in contingency table isn't empty
                    for i in r_set:
                        output[s].add(x+'u', i)
    con.close()
    result = {}
    for s in season:
        result[s] = output[s].get_list()
    return result


def main():
    season = ["04Q2"]
    approximation = "singleton"   # singleton & concept
    characteristic = "similarity"  # similarity & tolerance
    attribute = "global"             # global & age & gender

    if approximation == "singleton":
        target_method = singleton
    elif approximation == "concept":
        target_method = concept
    output = target_method("%s_%s" % (characteristic, attribute), season, input_table="H:\\AVANDIA_MYOCARDIAL.json")
    # output = singleton("similarity_global", ["04Q1"],
    #                   in_drug=["AVANDIA", "ROSIGLITAZONE"], in_symptom=["DEATH"], in_age=["18~60", "60~"])
    # output = target_method("%s_%s" % (characteristic, attribute), season, in_drug=["AVANDIA"], in_symptom=["MYOCARDIAL INFARCTION"], in_age=["18~60", "60~"])
    with open("D:\\log\\%s\\RS_AVANDIA1_%s_%s_%s.json" % (season[0], approximation, characteristic, attribute), "w") as f:
        f.write(json.dumps(output))
    print("===Result===")
    output = output[season[0]]
    key = output.keys()
    key.sort()
    for x in key:
        print("%s:%d" % (x, len(output[x])))
if __name__ == "__main__":
    main()