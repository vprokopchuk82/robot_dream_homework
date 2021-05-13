from hdfs import InsecureClient
import os
import psycopg2 as p
import yaml
from datetime import datetime, date, time
import json


def load_config():
    test_filename = os.path.join(os.path.dirname(__file__), 'config.yaml')
    with open(test_filename, mode='r') as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config



def create_or_get_storage_dir(table):
    current_date = datetime.now()
    year = str(current_date.year)
    month = str(current_date.month)
    day = str(current_date.day)
    folder = f'/data/db/dshop/{year}/{month}/{day}/{table}'
    hdfs_client = get_hdfs_client()
    hdfs_client.makedirs(folder)
    return folder


def main(config):
    with p.connect(**config['db']) as pg_connection:
        cursor = pg_connection.cursor()
        tables = get_all_tables(cursor)
        hdfs_client = get_hdfs_client()
        for table in tables:
            storage_dir=create_or_get_storage_dir(table)
            with hdfs_client.write(storage_dir) as csv_file:
                cursor.copy_expert(f'COPY {table} TO STDOUT CSV', csv_file)




def get_hdfs_client():
    global hdfs_client
    hadoop_config = load_config()['hadoop']
    return InsecureClient(f'http://{hadoop_config["host"]}:{hadoop_config["port"]}')




def get_all_tables(cursor):
    cursor.execute("""SELECT table_name FROM information_schema.tables
           WHERE table_schema = 'public'""")
    table_names = []
    for table in cursor.fetchall():
        table_names.append(table[0])
    return table_names


