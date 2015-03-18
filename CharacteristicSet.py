# -*- coding: UTF-8 -*-
# -------------------------------------------------------------------------------
# Name:          CharacteristicSet
# Purpose:       Preparing Data for analysis step by characteristic relation.
#                Source pre-data is combined as follows FAERS tables:
#                DEMO:Patient's basic information contain fields like id,age and gender.
#                DRUG:Drugs used in each case.
#                REAC:Symptoms which is produced from DRUG table.
#
# Author:        Phate
# Created:        2015/2/5
# Copyright:    (c) Phate 2015
# Licence:        <your licence>
# -------------------------------------------------------------------------------
from multiprocessing import Manager, Process
import os
import pyodbc

from fu_timer import timer_seconds

# static information define.
connect_information = "Trusted_Connection=yes;driver={SQL Server};server=localhost"
source_database = "LAN_PREDATA"
destination_database = "RSADRs"
CREATE_SET_TABLE = "CREATE TABLE %s (ID int,%s);"
CREATE_CONFIG_TABLE = "CREATE TABLE config (NAME varchar(30),VALUE int)"
AGE_TYPE = ["~5", "18~60", "60~"]
GENDER_TYPE = ["Male", "Female"]
LOG_DIR = "D:\\log\\"
# characteristic type
SIMILARITY_GLOBAL = 1
SIMILARITY_AGE = 2
SIMILARITY_GENDER = 3
TOLERANCE_GLOBAL = 4
TOLERANCE_AGE = 5
TOLERANCE_GENDER = 6
CHARACTERISTIC_TYPE = {SIMILARITY_GLOBAL: ("similarity", "global"),
                       SIMILARITY_AGE: ("similarity", "age"),
                       SIMILARITY_GENDER: ("similarity", "gender"),
                       TOLERANCE_GLOBAL: ("tolerance", "global"),
                       TOLERANCE_AGE: ("tolerance", "age"),
                       TOLERANCE_GENDER: ("tolerance", "gender")}


def create_table():  # Create table
    with pyodbc.connect(connect_information, database=source_database) as con:
        with con.cursor() as cursor:
            rows = cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.Tables")
            tables = [r+" varchar(max)" for r, in rows]
            tables.sort()

    with pyodbc.connect(connect_information, database=destination_database) as con:
        # create output table and config.
        with con.cursor() as cursor:
            for i in CHARACTERISTIC_TYPE:
                cursor.execute(CREATE_SET_TABLE % ("_".join(CHARACTERISTIC_TYPE[i]), ",".join(tables)))
                cursor.commit()


@timer_seconds
def pull_data(temp, target="totalFAERS"):
    """pull data from database to dictionary.
    Args:
        temp: multiprocessing.Manager().dict()
        target: which table you want read from destination_database.
    """
    with pyodbc.connect(connect_information, database=destination_database) as con:
        with con.cursor() as cursor:
            rows = cursor.execute("SELECT ID,ISR,season,age,gender,drug,PT FROM %s" % target)
            for ID, isr, season, age, gender, drug, PT in rows:
                if age not in AGE_TYPE:
                    age = None
                if gender not in GENDER_TYPE:
                    gender = None
                print(season+"\r"),
                temp[ID] = (isr, age, gender, season, drug, PT)


def find_characteristic_set(ctype, data,
                            connect_info=connect_information, db=destination_database,
                            start_id=0):
    """calculating characteristic set.
    This method is calculating every case y and other case x which accord the relationship.
    Args:
        ctype: type of this thread will process,
               it's about characteristic(similarity or tolerance) and attribute (global or local).
               In local case,we focus on two attributes-age and gender,so there are three cases need to consider.
               So we have six cases need to process.It's all define in this py file first.
        data:  all case which you want to process.it's a dictionary,the key is custom id.
        start_id: if work is stopped,can start from this id.
    """
    if ctype < 1 or ctype > 6:
        raise AttributeError("bad input, please check the type define.")
    # Initialization
    characteristic, attribute = CHARACTERISTIC_TYPE[ctype]
    if characteristic == "similarity":
        char_method = similarity
    elif characteristic == "tolerance":
        char_method = tolerance
    attr_type = [types for types in os.listdir(LOG_DIR+"\\type")]  # all attribute combination
    seasons = ["S_"+season for season in os.listdir(LOG_DIR+"\\season")]
    predata = {}  # Preparing all data which want insert to database,ex.{"~5_Female":{"S_04Q1":[],...},...}
    for attr in attr_type:
        predata[attr] = dict(zip(seasons, [[] for i in range(len(seasons))]))
    for case_y in attr_type:
        attr_y = case_y.split("_")
        for case_x in attr_type:
            attr_x = case_x.split("_")
            flag = False
            if attribute == "global":
                flag = char_method(attr_y, attr_x)  # ex. ["~5","Male"]
            elif attribute == "age":
                flag = char_method(attr_y[:1], attr_x[:1])  # ex.["~5"]
            elif attribute == "gender":
                flag = char_method(attr_y[1:], attr_x[1:])  # ex.["Male"]
            # predata which will put into database
            if flag:
                with open(LOG_DIR+"type\\"+case_x, "r") as f:
                    for case in f:
                        case = case.split("_")
                        predata[case_y]["S_"+case[2].replace("\n", "")].append(case[0])

    with open(LOG_DIR+"\\"+"_".join(CHARACTERISTIC_TYPE[ctype]), "w") as log:
        log.write(str(predata))
    print("start process"+characteristic + "_" + attribute)
    total = len(data) * len(data)
    ks = data.keys()
    count = 0
    for y in ks:
        pass


def similarity(y_attr, x_attr):
    """Lost case.
    Input two list which you want to compare fields.
    Args:
        y_attr: main case.
        x_attr: secondary case, it compare to case y.
    :return: True, if x and y have similarity relation.
    """
    for i in range(len(y_attr)):
        if y_attr[i]:
            if y_attr[i] != x_attr[i]:
                return False
    return True


def tolerance(y_attr, x_attr):
    """Don't care case.
    Args:
        y_attr: main case.
        x_attr: secondary case, it compare to case y.
    :return: True, if x and y have tolerance relation.
    """
    for i in range(len(y_attr)):
        if y_attr[i] and x_attr[i]:
            if y_attr[i] != x_attr[i]:
                return False
    return True


def main():
    man = Manager()
    srcdata = man.dict()
    pull_data(srcdata, "test_data")
    # pull_data(srcdata)
    processes = []
    for i in range(1, 7):
        p = Process(target=find_characteristic_set, args=(i, srcdata))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()


if __name__ == "__main__":
    main()