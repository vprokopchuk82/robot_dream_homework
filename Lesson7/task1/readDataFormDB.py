import os
import psycopg2 as p
import yaml

def load_config():
    test_filename = os.path.join(os.path.dirname(__file__), 'config.yaml')
    with open(test_filename, mode='r') as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config

def create_or_get_data_dir():
    current_directory = os.getcwd()
    data_folder = os.path.join(current_directory, 'data')
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
    return data_folder


def main(config):
    with p.connect(**config) as pg_connection:
        cursor = pg_connection.cursor()
        tables=get_all_tables(cursor)
        for table in tables:
            with open(os.path.join(create_or_get_data_dir(), f'{table}'), 'w') as csv_file:
                cursor.copy_expert(f'COPY {table} TO STDOUT CSV', csv_file)


def get_all_tables(cursor):
    cursor.execute("""SELECT table_name FROM information_schema.tables
           WHERE table_schema = 'public'""")
    table_names=[]
    for table in cursor.fetchall():
        table_names.append(table[0])
    return table_names

if __name__ == '__main__':
    config = load_config()
    main(config)