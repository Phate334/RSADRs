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


def singleton(characteristic_set, season, input_table=None):
    # get contingency table.
    if input_table:
        with open(input_table, "r") as f:
            cont_table = json.loads(f.read())
    else:
        cont_table = find(["AVANDIA", "ROSIGLITAZONE"], ["MYOCARDIAL INFARCTION"], in_age=["18~60", "60~"])
    charact, attr = characteristic_set.split("_")
    if charact not in characteristic_type or attr not in attribute_type:
        raise AttributeError("ERROR:characteristic set")
    output = {"Xal": set(), "Xau": set(), "Xbl": set(), "Xbu": set(),
              "Xcl": set(), "Xcu": set(), "Xdl": set(), "Xdu": set()}


def main():
    singleton("similarity_global", "D:\\log\\AVANDIA_MYOCARDIAL.json")


if __name__ == "__main__":
    main()