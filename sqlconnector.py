#Perform the pip installations and setup for the files to be used
import os
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import subprocess

#The function 'transfer' that deals with pipelining the entire data transfer process and the creation of data tables as required
def transfer(path, configuration, x, table_prefix='s1_sales_database'):
    connection = mysql.connector.connect(**configuration)
    
    #creation of the virtual connection to interact with the database and make all necessary changes
    cursor = connection.cursor()

    #creating the database using the cursor object
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {x}")
    connection.database = x


    #Accessing the excel
    xls = pd.ExcelFile(path)

    #Creation of a database engine with the usage of 'create_engine' imported from sqlalchemy class
    engine = create_engine(f"mysql+mysqlconnector://{configuration['user']}:{configuration['password']}@{configuration['host']}/{x}")


    #This loop is what adds the time complexity of O(n)
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name)
        table_name = f'{table_prefix}{sheet_name.replace(" ", "_").lower()}'
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f'Data from Excelsheet "{sheet_name}" transferred to MySQL table: {table_name}')

    connection.commit()
    connection.close()

    #Here we create a list of command-line codes to setup the configuration
    mysql_args = [
        'C:/Program Files/MySQL/MySQL Server 8.0/bin/mysql.exe',
        '-u' + configuration['user'],
        '-p' + configuration['password'],
        '-h' + configuration['host'],
        x
    ]

    #Implementation in Command Line
    subprocess.run(mysql_args)

#The details of host, here it being me.
config = {
    'host': 'localhost',
    'user': '@@@@',
    'password': '#####',
}

path = r'C:\Users\axham\OneDrive\Documents\SQL Project Folder\s1_sales_database.xlsx'
x = 'test_database'

#Execution
transfer(path, configuration, x)