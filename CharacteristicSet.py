# -*- coding: UTF-8 -*-
# -------------------------------------------------------------------------------
# Name:          CharacteristicSet
# Purpose:       Preparing Data for analysis step by characteristic relation.
# Source pre-data is combined as follows FAERS tables:
#                DEMO:Patient's basic information contain fields like id,age and gender.
#                DRUG:Drugs used in each case.
#                REAC:Symptoms which is produced from DRUG table.
#
# Author:        Phate
# Created:        2015/2/5
# Copyright:    (c) Phate 2015
# Licence:        <your licence>
# -------------------------------------------------------------------------------
import pyodbc


class CharacteristicSet:
    def __init__(self, connectInfo):
        self.conInfo = connectInfo
        with pyodbc.connect(self.conInfo) as con:
            with con.cursor() as cursor:
                rows = cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.Tables")
                self.tables = [r for r, in rows]

    def test(self):
        print self.tables


def main():
    cs = CharacteristicSet("Trusted_Connection=yes;driver={SQL Server};server=localhost;database=LAN_PREDATA")
    cs.test()


if __name__ == "__main__":
    main()