# -*- coding: UTF-8 -*-
# -------------------------------------------------------------------------------
# Name:         ContingencyTable22
# Purpose:
#
# Author:       Phate
#
# Created:      2015/4/18
# Copyright:    (c) user 2015
# Licence:      <your licence>
# -------------------------------------------------------------------------------
import pyodbc

connect_info = "Trusted_Connection=yes;driver={SQL Server};server=localhost"
source_database = "LAN_PREDATA"
destination_database = "RSADRs"


def get_timeline(start=None, end=None):
    with open(connect_info, database="source_database") as con:
        with con.cursor() as cursor:
            rows = cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.Tables")
            tables = [r for r, in rows]
    print tables


def find(drugs, symptoms, age=None, gender=None):
    """output contingency table.
    """
    pass


def main():
    get_timeline()


if __name__ == "__main__":
    main()