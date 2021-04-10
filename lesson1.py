import json
import requests
import yaml
from requests import HTTPError


def load_config(config_path):

    with open(config_path, mode='r') as yaml_file:
       config = yaml.safe_load(yaml_file)
    return config


def app(config):
    base_url  = config['url']
    api = config['api']
    auth = config['auth']

    headers={'content-type':'application/json'}
    auth_token=''
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

            with open(f'./data/{date}.json', mode='w+') as date_file:
                json.dump(api_res.json(), date_file)

        except HTTPError as e:
            print(e)


if __name__ == '__main__':
    config = load_config('./config.yaml')
    app(config)




