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
INSERT_DATA = "INSERT INTO totalFAERS (ISR,season,age,gender,drug,PT) VALUES(%d,'%s','%s','%s','%s','%s')"
LOG_DIR = "D:\\log\\"


class UpdateTableThread(threading.Thread):
    """push to total FAERS data table.
    Args:
        queue: ready process source table's name.
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
                isr = int(isr)
                drug = drug.replace("'", "''")
                PT = PT.replace("'", "''")
                descursor.execute(INSERT_DATA % (isr, self.season, age, gender, drug, PT))
            descursor.commit()
            descursor.close()
            srccursor.close()
            descon.close()
            srccon.close()
            self.queue.task_done()


def merge_data():
    """
    merge all data from source database, it define in this .py file.
    """
    # get table name, and put in queue.
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


def output_data():
    """output database to txt file.
    1.isr data by season.
    2.total data.
    """
    # output ever isr by season
    with pyodbc.connect(connect_information, database=source_database) as con:
        with con.cursor() as cursor:
            rows = cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.Tables")
            tables = [r for r, in rows]
        for t in tables:
            pass
    # output total data.
    with pyodbc.connect(connect_information, database=destination_database) as con:
        with con.cursor() as cursor:
            pass


if __name__ == "__main__":
    # merge_data()
    print("check parameters in file begin,and put target data into queue.")