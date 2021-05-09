import json
import requests
import yaml
import os
from requests import HTTPError


def load_config():
    test_filename = os.path.join(os.path.dirname(__file__), 'config.yaml')
    # test_filename = os.path.join('.', 'config.yaml')
    print(test_filename)
    with open(test_filename, mode='r') as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config


def app(config):

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
        data_folder = os.path.join('.', 'data')
        if not os.path.exists(data_folder):
            os.makedirs(data_folder)

        path_to_file = os.path.join(data_folder, f'{date}.json')
        with open(path_to_file, mode='w+') as date_file:
            json.dump(api_res.json(), date_file)


if __name__ == '__main__':
    config = load_config()
    app(config)
