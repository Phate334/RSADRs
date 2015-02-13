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

from fu_timer import timer_seconds

# static information define.
connect_information = "Trusted_Connection=yes;driver={SQL Server};server=localhost"
source_database = "LAN_PREDATA"
destination_database = "RSADRs"
CREATE_SET_TABLE = "CREATE TABLE %s (ISR bigint,CASES varchar(max));"
CREATE_TOTAL_TABLE = "CREATE TABLE %s " \
                     "(ISR bigint,age varchar(10),gender varchar(10),drug varchar(max),PT varchar(max))"
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
source_data = {}  # pull data from database.
isrs = None


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
        ctype: type of this thread will process,
               it's about characteristic(similarity or tolerance) and attribute (global or local),
               and because we focus on two attributes-age and gender,so there are two cases we need to consider.
               Final,we have six cases need to process.
    """
    CHARACTERISTIC_TYPE = {SIMILARITY_GLOBAL: ("similarity", "global"),
                           SIMILARITY_AGE: ("similarity", "age"),
                           SIMILARITY_GENDER: ("similarity", "gender"),
                           TOLERANCE_GLOBAL: ("tolerance", "global"),
                           TOLERANCE_AGE: ("tolerance", "age"),
                           TOLERANCE_GENDER: ("tolerance", "gender")}

    def __init__(self, ctype):
        threading.Thread.__init__(self)
        if ctype < 1 or ctype > 6:
            raise AttributeError("bad input, please check the type define.")
        self.relation, self.attr = self.CHARACTERISTIC_TYPE[ctype]

    def run(self):
        """Thread of calculating characteristic set.
        There is some define of two characteristic relations.
        Similarity set:
        """
        print(self.relation + "_" + self.attr)
        for y in isrs:
            for x in isrs:
                pass

    def similarity(self):
        """Lost case.
        """
        pass

    def tolerance(self):
        """Don't care case.
        """
        pass


@timer_seconds
def main():
    global source_data,isrs
    source_data = pull_data()
    isrs = source_data.keys()
    threads = [CharacteristicThread(i) for i in range(1, 7)]
    for t in threads:
        t.setDaemon(True)
        t.start()
    while threading.active_count() > 1:
        pass

if __name__ == "__main__":
    main()