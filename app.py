#!/usr/bin/python3

import os
import json
import sqlparse

import pandas as pd
import numpy as np

import connection
import conn_warehouse

if __name__ == '__main__':
    print(f"[INFO] Service ETL is Starting .....")
    conn_dwh, engine_dwh  = conn_warehouse.conn()
    cursor_dwh = conn_dwh.cursor()

    conf = connection.config('postgresql')
    conn, engine = connection.psql_conn(conf)
    cursor = conn.cursor()

    path_query = os.getcwd()+'/query/'
    query1 = sqlparse.format(
        open(
            path_query+'query_dim_users.sql','r'
            ).read(), strip_comments=True).strip()

    query_dwh1 = sqlparse.format(
        open(
            path_query+'dwh_design_dim_users.sql','r'
            ).read(), strip_comments=True).strip()

    path_query = os.getcwd()+'/query/'
    query2 = sqlparse.format(
        open(
            path_query+'query_fact_orders.sql','r'
            ).read(), strip_comments=True).strip()

    query_dwh2 = sqlparse.format(
        open(
            path_query+'dwh_design_fact_orders.sql','r'
            ).read(), strip_comments=True).strip()
    try:
        print(f"[INFO] Service ETL is Running .....")
        df = pd.read_sql(query1, engine)
        
        cursor_dwh.execute(query_dwh1)
        conn_dwh.commit()

        df.to_sql('dim_users', engine_dwh, if_exists='replace', index=False)

        df = pd.read_sql(query2, engine)
        
        cursor_dwh.execute(query_dwh2)
        conn_dwh.commit()

        df.to_sql('fact_orders', engine_dwh, if_exists='replace', index=False)

        print(f"[INFO] Service ETL is Success .....")
    except:
        print(f"[INFO] Service ETL is Failed .....")

    

    