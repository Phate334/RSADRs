# -*- coding: UTF-8 -*-
# -------------------------------------------------------------------------------
# Name:          mergeFAERSdata
# Purpose:       merge FAERS all data to one table.
#                put the target table name into queue, and send to
#                UpdateTableThread,it will push them to destination.
#
# Author:        Phate
#
# Created:       2015/2/10
# Copyright:     (c) Phate 2015
# -------------------------------------------------------------------------------
import threading
import Queue
import pyodbc

connect_information = "Trusted_Connection=yes;driver={SQL Server};server=localhost"
source_database = "LAN_PREDATA"
destination_database = "RSADRs"
AGE_TYPE = ["~5", "18~60", "60~"]
GENDER_TYPE = ["Male", "Female"]
CREATE_TABLE = "CREATE TABLE %s (ISR varchar(20),CASES varchar(max));"
INSERT_DATA = "INSERT INTO totalFAERS (ISR,age,gender,drug,PT) VALUES('%s','%s','%s','%s','%s')"
LOG_DIR = "D:\\log\\"


class UpdateTableThread(threading.Thread):
    """push to total FAERS data table.
    Args:
        queue: ready process source tabkles.
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
            print("start process table %s"%(self.season))
            srccon = pyodbc.connect(connect_information, database=source_database)
            descon = pyodbc.connect(connect_information, database=destination_database)
            srccursor = srccon.cursor()
            descursor = descon.cursor()
            rows = srccursor.execute("SELECT ISR,age,gender,drug,PT FROM %s"%(self.table))
            for isr, age, gender, drug, PT in rows:
                if age not in AGE_TYPE:
                    age = "NULL"
                if gender not in GENDER_TYPE:
                    gender = "NULL"
                isr += "@"+self.season
                drug = drug.replace("'", "''")
                PT = PT.replace("'", "''")
                descursor.execute(INSERT_DATA%(isr, age, gender, drug, PT))
            descursor.commit()
            descursor.close()
            srccursor.close()
            descon.close()
            srccon.close()
            self.queue.task_done()


def create_table():  # Create table
    with pyodbc.connect(connect_information, database=destination_database) as con:
        with con.cursor() as cursor:
            cursor.execute(CREATE_TABLE % ("similarity_global"))
            cursor.execute(CREATE_TABLE % ("similarity_age"))
            cursor.execute(CREATE_TABLE % ("similarity_gender"))
            cursor.execute(CREATE_TABLE % ("tolerance_global"))
            cursor.execute(CREATE_TABLE % ("tolerance_age"))
            cursor.execute(CREATE_TABLE % ("tolerance_gender"))
            cursor.commit()


def main():
    tables_queue = Queue.Queue()
    with pyodbc.connect(connect_information, database=source_database) as con:
        with con.cursor() as cursor:
            rows = cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.Tables")
            for r, in rows:
                tables_queue.put(r)
    for i in range(5):
        t = UpdateTableThread(tables_queue)
        t.setDaemon(True)
        t.start()
    tables_queue.join()
    raw_input(">>")


if __name__ == "__main__":
    main()
    print("check parameters in file begin,and put target data into queue.")