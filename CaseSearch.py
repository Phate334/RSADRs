# -*- coding: UTF-8 -*-
# -------------------------------------------------------------------------------
# Name:          CaseSearch
# Purpose:       search case by drug or pt in all data.
#
# Author:         Phate
#
# Created:        2015/3/10
# Copyright:    (c) Phate 2015
# Licence:        <your licence>
# -------------------------------------------------------------------------------
import pyodbc

connect_information = "Trusted_Connection=yes;driver={SQL Server};server=localhost"
AGE_TYPE = ["~5", "18~60", "60~"]
GENDER_TYPE = ["Male", "Female"]
LOG_DIR = "D:\\log\\"


def do(in_drug, in_pts, in_ages=None, in_genders=None):
    output = {}
    with pyodbc.connect(connect_information, database="RSADRs") as con:
        cursor = con.cursor()
        rows = cursor.execute("SELECT season,age,gender,drug,PT FROM totalFAERS")
        for season, age, gender, drug, PT in rows:  # list all data.
            target_season = output.setdefault(season, [0, 0, 0, 0])
            if in_ages is not None and age not in in_ages:
                continue
            if in_genders is not None and gender == in_genders:
                continue
            case_drug = drug.split(",")
            case_pt = PT.split(",")
            flag_d = False
            flag_p = False
            for d in in_drug:
                if d in case_drug:
                    flag_d = True
            for p in in_pts:
                if p in case_pt:
                    flag_p = True
            # classify this cass in 2*2 contingency table
            if flag_d:
                if flag_p:  # A
                    target_season[0] += 1
                else:  # B
                    target_season[1] += 1
            else:
                if flag_p:  # C
                    target_season[2] += 1
                else:  # D
                    target_season[3] += 1
        cursor.close()
    seasons = output.keys()
    seasons.sort()
    with open("%ssearch\\%s_%s.txt" % (LOG_DIR, in_drug[0], in_pts[0]), "w") as output_file:
        for s in seasons:
            output_file.write(s)
            for i in range(4):
                output_file.write("\t%d" % output[s][i])
            output_file.write("\n")


def main():
    pass


if __name__ == "__main__":
    main()