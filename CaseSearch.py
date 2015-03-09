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


def do(in_drug, in_pts, in_ages="NULL", in_genders="NULL"):
    with pyodbc.connect(connect_information, database="RSADRs") as con:
        cursor = con.cursor()
        rows = cursor.execute("SELECT season,age,gender,drug,PT FROM totalFAERS")
        for season, age, gender, drug, PT in rows:
            if in_ages != "NULL" and age not in in_ages:
                continue
            if in_genders != "NULL" and gender not in in_genders:
                continue
            drug = drug.split(",")
            PT = PT.split(",")
            flag_d = False
            flag_p = False
            for d in in_drug:
                if d in in_drug:
                    flag_d = True
            for p in in_pts:
                if p in in_pts:
                    flag_p = True

        cursor.close()

def main():
    pass


if __name__ == "__main__":
    main()