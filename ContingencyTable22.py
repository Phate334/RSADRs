# -*- coding: UTF-8 -*-
# -------------------------------------------------------------------------------
# Name:         ContingencyTable22
# Purpose:
#
# Author:       Phate
#
# Created:      2015/4/18
# Copyright:    (c) user 2015
# -------------------------------------------------------------------------------
import pyodbc
import json

connect_info = "Trusted_Connection=yes;driver={SQL Server};server=localhost"
source_database = "LAN_PREDATA"
destination_database = "RSADRs"
LOG_DIR = "D:\\log\\"


def get_timeline(start=None, end=None):
    with pyodbc.connect(connect_info, database=source_database) as con:
        with con.cursor() as cursor:
            rows = cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.Tables")
            tables = [r for r, in rows]
    if start:
        start = tables.index(start)
    if end:
        end = tables.index(end)
    tables = tables[start:end]
    tables.sort()
    return tables


def contingency_type(in_drug, in_symptom, drugs, pts):
    in_drug = set(in_drug)
    in_symptom = set(in_symptom)
    drugs = set(drugs)
    pts = set(pts)
    if in_drug & drugs:
        if in_symptom & pts:
            return "Xa"
        else:
            return "Xb"
    else:
        if in_symptom & pts:
            return "Xc"
        else:
            return "Xd"


def find(in_drug, in_symptom, timeline, in_age=None, in_gender=None):
    """output contingency table.
    """
    result = {"Xa": [], "Xb": [], "Xc": [], "Xd": []}
    with pyodbc.connect(connect_info, database=destination_database) as con:
        for season in timeline:
            print("search %s..." % season)
            cursor = con.cursor()
            rows = cursor.execute("SELECT ID,age,gender,drug,PT FROM totalFAERS WHERE season='%s'" % season[2:])
            for ID, age, gender, drug, PT in rows:
                print("%s\r" % ID),
                if in_age and age not in in_age:
                    continue
                if in_gender and gender not in in_gender:
                    continue
                drugs = drug.split(",")
                pts = PT.split(",")
                result[contingency_type(in_drug, in_symptom, drugs, pts)].append(ID)
            print("Done")
            cursor.close()
    print("drugs:%s" % str(in_drug))
    print("symptoms:%s" % str(in_symptom))
    print("a:%d\tb:%d\tc:%d\td:%d\t" %
          (len(result["Xa"]), len(result["Xb"]), len(result["Xc"]), len(result["Xd"])))
    return result


def main():
    seasons = get_timeline()
    result = find(["AVANDIA", "ROSIGLITAZONE"], ["DEATH"], seasons, in_age=["18~60", "60~"])
    with open(LOG_DIR+"%s_%s.json" % ("AVANDIA", "DEATH"), "w") as log:
        log.write(json.dumps(result))

if __name__ == "__main__":
    main()