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
import threading
import pyodbc

connect_information = "Trusted_Connection=yes;driver={SQL Server};server=localhost"
source_database = "LAN_PREDATA"
destination_database = "RSADRs"
CREATE_SET_TABLE = "CREATE TABLE %s (ISR bigint,CASES varchar(max));"
CREATE_TOTAL_TABLE = "CREATE TABLE %s " \
                     "(ISR bigint,age varchar(10),gender varchar(10),drug varchar(max),PT varchar(max))"
DROP_TABLE = "DROP TABLE %s;"
AGE_TYPE = ["~5", "18~60", "60~"]
GENDER_TYPE = ["Male", "Female"]
LOG_DIR = "D:\\log\\"

source_data = {}


def create_table():  # Create table
    with pyodbc.connect(connect_information, database=destination_database) as con:
        with con.cursor() as cursor:
            cursor.execute(CREATE_SET_TABLE % ("similarity_global"))
            cursor.execute(CREATE_SET_TABLE % ("similarity_age"))
            cursor.execute(CREATE_SET_TABLE % ("similarity_gender"))
            cursor.execute(CREATE_SET_TABLE % ("tolerance_global"))
            cursor.execute(CREATE_SET_TABLE % ("tolerance_age"))
            cursor.execute(CREATE_SET_TABLE % ("tolerance_gender"))
            cursor.execute(CREATE_TOTAL_TABLE % ("totalFAERS"))
            cursor.commit()


def pull_data():  # pull data　into.
    with pyodbc.connect(connect_information, database=destination_database) as con:
        with con.cursor() as cursor:
            rows = cursor.execute("SELECT ISR,season,age,gender,drug,PT FROM totalFAERS")
            temp = {}
            for isr, season, age, gender, drug, PT in rows:
                if age not in AGE_TYPE:
                    age = None
                if gender not in GENDER_TYPE:
                    gender = None
                print(season+"\r"),
                temp[isr] = (age, season, gender, drug, PT)
    return temp


class CharacteristicThread(threading.Thread):
    """Define a thread to process predata from FAERS database.
    Args:

    """
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        print(len(source_data))

    def similarity(self):
        """Lost case.
        """
        pass

    def tolerance(self):
        """Don't care case.
        """
        pass


def main():
    global source_data
    source_data = pull_data()
    threads = [CharacteristicThread() for i in range(5)]
    for t in threads:
        t.setDaemon(True)
        t.start()
    while threading.active_count() > 1:
        pass

if __name__ == "__main__":
    main()