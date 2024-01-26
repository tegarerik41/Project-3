#!/usr/bin/python3

import os
import json
import time
import gspread
import psycopg2
import connection

import pandas as pd
import numpy as np

from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine

import warnings
warnings.filterwarnings('ignore')

if __name__ == '__main__':
    print(f"[INFO] Service ETL is Starting .....")

    # extract & transform
    print(f"[INFO] Extract & Tranform Process is Running .....")
    start = time.time()

    conn_source, engine_source  = connection.source_conn()
    cursor_source = conn_source.cursor()

    path = os.getcwd()
    folder_path = os.path.join(path, 'sql')
    filename = os.path.join(folder_path, 'dml_dwh.sql')

    with open(filename, "r") as sql_file:
        sql_script = sql_file.read()
        df = pd.read_sql_query(sql_script, engine_source)

    end = time.time()
    print(f"[INFO] {end-start} Extract & Tranform Process is Done .....")

    # # load
    print(f"[INFO] Load Load is Running .....")
    start = time.time()

    conn_dwh, engine_dwh  = connection.dwh_conn()
    cursor_dwh = conn_dwh.cursor()

    df.to_sql('dim_orders', engine_dwh, index=False, if_exists='replace')
    
    end = time.time()
    print(f"[INFO] {end-start} Load Process is Done .....")
