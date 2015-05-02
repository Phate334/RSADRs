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
    characteristic = "tolerance"  # similarity & tolerance
    attribute = "global"             # global & age & gender

    if approximation == "singleton":
        target_method = singleton
    elif approximation == "concept":
        target_method = concept
    output = target_method("%s_%s" % (characteristic, attribute), season, input_table="D:\\log\\AVANDIA_MYOCARDIAL.json")
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