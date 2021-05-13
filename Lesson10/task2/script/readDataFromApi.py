import json
import requests
import yaml
import os
from datetime import datetime, date, time
from hdfs import InsecureClient

from requests import HTTPError


def load_config():
    test_filename = os.path.join(os.path.dirname(__file__), 'config.yaml')
    print(test_filename)
    with open(test_filename, mode='r') as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config

def get_hdfs_client():
    hadoop_config = load_config()['hadoop']
    print(hadoop_config)
    return InsecureClient(f'http://{hadoop_config["host"]}:{hadoop_config["port"]}')



def create_or_get_storage_dir():
    current_date = datetime.now()
    year = str(current_date.year)
    month = str(current_date.month)
    day = str(current_date.day)
    folder = f'/data/api/{year}/{month}/{day}'
    hdfs_client = get_hdfs_client()
    hdfs_client.makedirs(folder)
    return folder



def main(config):

    base_url = config['url']
    api = config['api']
    auth = config['auth']

    headers = {'content-type': 'application/json'}
    auth_token = ''
    api_res=None
    try:
        res = requests.post(f'{base_url}{auth["endpoint"]}', data=json.dumps(auth["credential"]), headers=headers)
        res.raise_for_status()
        resp = res.json()
        auth_token = f'JWT {resp["access_token"]}'
    except HTTPError as e:
        print(e)

    headers["Authorization"] = auth_token

    dates = api['date']
    for date in dates:
        try:
            api_res = requests.get(f'{base_url}{api["endpoint"]}', data=json.dumps({"date": date}), headers=headers)
            api_res.raise_for_status()
        except HTTPError as e:
            print(e)
        storage_dir = create_or_get_storage_dir()
        get_hdfs_client().write(f'{storage_dir}/{date}.json', json.dumps(api_res.json()), True)


if __name__ == '__main__':
    config = load_config()
    main(config)





