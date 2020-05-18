import psycopg2
import os
import glob
import pandas as pd

from sql_queries import create_table_queries, drop_table_queries
from create_database import *


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)

        except psycopg2.Error as e:
            print("Error: Failed to drop table")

        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print("Error: Failed to create table")
            print(e)
            print(query)

        conn.commit()


def getfiles(filepath, file_ext):
    """
    This function finds the filepaths of each file in the directory specified which includes subdirectories and then
    returns a list of these filepaths
    """
    allfiles = []
    for root, folder, file in os.walk(filepath):
        files = glob.glob(os.path.join(root, f"*.{file_ext}"))

        for f in files:
            allfiles.append(f)

    return allfiles


def main():
    """
    - Drops (if exists) and Creates the sparkify database by running the script create_database
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    # dbname = input("Enter the name of the database you want to create \n")
    conn, cur = create_database("127.0.0.1", "sparkifydb", "postgres", "1234")

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
