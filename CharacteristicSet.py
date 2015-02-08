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

connect_infomation = "Trusted_Connection=yes;driver={SQL Server};server=localhost"
source_database = "LAN_PREDATA"
destination_database = "RSADRs"


class CharacteristicThread(threading.Thread):
    """Define a thread to process predata from FAERS database.
    Args:
        queue: already process srouce tables.
    """
    def __init__(self,queue):
        pass


def similarity():
    """Lost case.
    """
    pass


def tolerance():
    """Don't care case.
    """
    pass


def main():
    pass


if __name__ == "__main__":
    main()