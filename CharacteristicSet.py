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
import Queue
import pyodbc

connect_information = "Trusted_Connection=yes;driver={SQL Server};server=localhost"
source_database = "LAN_PREDATA"
destination_database = "RSADRs"
CREATE_TABLE = "CREATE TABLE %s (ISR varchar(20),CASES varchar(max));"
DROP_TABLE = "DROP TABLE %s;"
AGE_TYPE = ["~5", "18~60", "60~"]
GENDER_TYPE = ["Male", "Female"]
LOG_DIR = "D:\\log\\"


class CharacteristicThread(threading.Thread):
    """Define a thread to process predata from FAERS database.
    Args:
        queue: already process srouce tables.
    """
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.table = None
        self.season = None

    def run(self):
        while True:
            self.table = self.queue.get(block=True)
            self.season = self.table[2:]
            print("start process table %s"%(self.table))
            try:  # if fail create table, then drop all.
                self.create_table()
                src_data = self.pull_data(self.table)
                print len(src_data)
            except:
                print("%s fail create,try to drop them."%(self.season))
                self.drop_table()
            self.queue.task_done()

    def pull_data(self, table_name):  # pull this season data into ram.
        with pyodbc.connect(connect_information, database=source_database) as con:
            with con.cursor() as cursor:
                rows = cursor.execute("SELECT ISR,age,gender,drug,PT FROM %s"%(table_name))
                temp = {}
                for isr, age, gender, drug, PT in rows:
                    if age not in AGE_TYPE:
                        age = None
                    if gender not in GENDER_TYPE:
                        gender = None
                    temp[isr] = (age, gender, drug, PT)
        return temp
    def similarity(self):
        """Lost case.
        """
        pass

    def tolerance(self):
        """Don't care case.
        """
        pass

    def create_table(self):  # Create table by season
        with pyodbc.connect(connect_information, database=destination_database) as con:
            with con.cursor() as cursor:
                cursor.execute(CREATE_TABLE % ("similarity_global_"+self.season))
                cursor.execute(CREATE_TABLE % ("similarity_age_"+self.season))
                cursor.execute(CREATE_TABLE % ("similarity_gender_"+self.season))
                cursor.execute(CREATE_TABLE % ("tolerance_global_"+self.season))
                cursor.execute(CREATE_TABLE % ("tolerance_age_"+self.season))
                cursor.execute(CREATE_TABLE % ("tolerance_gender_"+self.season))
                cursor.commit()

    def drop_table(self):  # Drop table if fail to create it.
        try:
            with pyodbc.connect(connect_information, database=destination_database) as con:
                with con.cursor() as cursor:
                    cursor.execute(DROP_TABLE % ("similarity_global_"+self.season))
                    cursor.execute(DROP_TABLE % ("similarity_age_"+self.season))
                    cursor.execute(DROP_TABLE % ("similarity_gender_"+self.season))
                    cursor.execute(DROP_TABLE % ("tolerance_global_"+self.season))
                    cursor.execute(DROP_TABLE % ("tolerance_age_"+self.season))
                    cursor.execute(DROP_TABLE % ("tolerance_gender_"+self.season))
                    cursor.commit()
        except:
            print("please check %s in database"%(self.season))


def main():
    tables_queue = Queue.Queue()  # ready to process data.
    with pyodbc.connect(connect_information, database=source_database) as con:
        with con.cursor() as cursor:
            rows = cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.Tables")
            for r, in rows:
                tables_queue.put(r)
                season = r[2:]
    for i in range(5):
        t = CharacteristicThread(tables_queue)
        t.setDaemon(True)
        t.start()
    tables_queue.join()
    raw_input(">>")

if __name__ == "__main__":
    main()