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
            rows = cursor.execute("SELECT age,gender,%s FROM %s" % (",".join(["S_"+s for s in season]), characteristic))
            for row in rows:
                char_set["%s_%s" % (row[0], row[1])] = {}
                for i in range(len(season)):
                    char_set["%s_%s" % (row[0], row[1])][season[i]] = set(row[i+2].split(","))

    print("===%s:%s===" % (characteristic, "\t".join(season)))
    for k in char_set.keys():
        attr_type = char_set[k]
        print("%s:\t%s" % (k, "\t".join([str(len(attr_type[s])) for s in season])))
    # check id range in every season
    id_range = {}
    with pyodbc.connect(connect_information, database="RSADRs") as con:
        for s in season:
            with con.cursor() as cursor:
                rows = cursor.execute("SELECT MIN(ID),MAX(ID) FROM totalFAERS WHERE season='%s'" % s)
                min_id, max_id = rows.fetchone()
            id_range[s] = (min_id, max_id)
    print("===ID range===")
    for s in season:
        print("%s:%s" % (s, str(id_range[s])))
    # building RS table
    output = {}
    con = pyodbc.connect(connect_information, database="RSADRs")
    for s in season:
        output[s] = RSContingencyTable()
        with con.cursor() as search:
            rows = search.execute("SELECT ID,relation FROM DPCharacteristic WHERE ID>=%d AND ID<=%d" %
                                  (id_range[s][0], id_range[s][1]))
            for ID, relation in rows:
                pass
    con.close()


def main():
    singleton("similarity_global", ["04Q1", "04Q2", "09Q2"], input_table="D:\\log\\AVANDIA_MYOCARDIAL.json")


if __name__ == "__main__":
    main()