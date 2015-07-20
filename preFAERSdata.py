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
    1.custom id data by season.
    2.total data.
    """
    with pyodbc.connect(connect_information, database=destination_database) as con:
        with con.cursor() as cursor:
            rows = cursor.execute("SELECT ID,ISR,season,age,gender FROM totalFAERS")
            count = 0
            for ID, isr, season, age, gender in rows:
                # ID by season
                try:
                    with open(LOG_DIR+"season\\"+season, "a") as s:
                        s.write(str(ID)+"\n")
                except IOError:
                    with open(LOG_DIR+"season\\"+season, "w") as s:
                        s.write(str(ID)+"\n")
                # 12 types
                try:
                    with open(LOG_DIR+"type\\"+str(age)+"_"+str(gender), "a") as t:
                        t.write("%d_%d_%s\n" % (ID, isr, season))
                except IOError:
                    with open(LOG_DIR+"type\\"+str(age)+"_"+str(gender), "w") as t:
                        t.write("%d_%d_%s\n" % (ID, isr, season))
                # total data
                try:
                    with open(LOG_DIR+"total", "a") as total:
                        total.write("%d$%d$%s$%s$%s\n" % (ID, isr, season, age, gender))
                except IOError:
                    with open(LOG_DIR+"total", "w") as total:
                        total.write("%d$%d$%s$%s$%s\n" % (ID, isr, season, age, gender))
                count += 1
                print(str(count)+"\r"),


def build_dp_characteristic():
    con = pyodbc.connect(connect_information, database=destination_database)
    # get number of case
    with con.cursor() as cursor:
        id_count, = cursor.execute("select count(id) from total_sort").fetchone()
    ids = set(range(id_count))
    print("number of cases:%d" % len(ids))
    # check non-process case
    with con.cursor() as cursor:
        rows = cursor.execute("SELECT id FROM DPCharacteristic")
        preids = set([i for i, in rows])
    print("find %d cases" % len(preids))
    ids = list(ids-preids)
    print("%d cases ready to process" % len(ids))
    # insert data
    same_dp_case = "SELECT ID FROM total_sort " \
                   "WHERE drug = (select drug from total_sort where ID=%d) AND " \
                   "PT = (select PT from total_sort where ID=%d)"
    insert_same_case = "INSERT INTO DPCharacteristic (ID,relation)" \
                       "VALUES (%d,'%s')"
    ids.sort()
    copy_ids = ids[:]
    for i in ids:
        if i in copy_ids:  # find the case which have same drug and PT.
            with con.cursor() as cursor:
                rows = cursor.execute(same_dp_case % (i, i))
                same_case = [i for i, in rows]
            with con.cursor() as cursor:
                for target in same_case:
                    cursor.execute(insert_same_case % (target, ",".join([str(s) for s in same_case])))
                    copy_ids.remove(target)
                cursor.commit()
            print("ID:%d,same case :%d,total:%d" % (i, len(same_case), len(copy_ids)))
    with con.cursor() as cursor:
        rows = cursor.execute("SELECT count(ID) FROM DPCharacteristic")
        print("\n======\nTOTAL DPCharacteristic number %d\n======" % (rows.fetchone(),))
    con.close()

if __name__ == "__main__":
    # merge_data()
    # output_data()
    build_dp_characteristic()
    print("check parameters in file begin,and put target data into queue.")