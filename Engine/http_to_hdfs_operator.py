from pathlib import Path
from datetime import datetime
from Engine import config
import requests
import os
import json
from airflow.hooks.webhdfs_hook import WebHDFSHook
import logging


class HttpToHDFSOperator():
    def __init__(self, config_path: Path, app_name: str, date: datetime.date, timeout: int, hdfs_conn_id: str,
                 hdfs_path: Path):
        self.config_path = config_path
        self.app_name = app_name
        self.date = date
        self.timeout = timeout
        self.hdfs_conn_id = hdfs_conn_id
        self.hdfs_path = hdfs_path
        self.log = logging.getLogger(self.__class__.__module__ + '.' + self.__class__.__name__)

    def execute(self):
        self.app_config = self.load_config()
        token = None
        if self.app_config.get('auth'):
            token = self.get_token()

        self.call_api(token=token)

    def load_config(self):
        try:
            return config.Config(self.config_path).get_app_config(self.app_name)
        except FileNotFoundError as e:
            e.strerror = 'Config file for app not found. ' + e.strerror
            raise e
        except KeyError as e:
            raise KeyError(f'Can not find aplication: "{self.app_name}" in config file "{str(self.config_path)}".')

    def get_token(self):
        if self.app_config['auth']['type'] == "JWT TOKEN":
            r = requests.post(url=self.app_config['url'] + self.app_config['auth']['endpoint'],
                              headers=self.app_config['headers'], json=self.app_config['auth']['parameters'],
                              timeout=self.timeout)
            if r.status_code == 200:
                if r.json().get('access_token'):
                    return "JWT " + r.json()['access_token']
                else:
                    raise Exception(f"Auth request do not return access_token.")
            else:
                raise Exception(
                    f"Auth request return bad response state_code {str(r.status_code)} with message: {r.text}")
        else:
            return ""

    def call_api(self, token: str):
        headers = self.app_config['headers']
        if token:
            headers['Authorization'] = token

        parameters = {"date": self.date}

        try:
            r = requests.get(url=self.app_config['url'] + self.app_config['endpoint'], headers=headers, json=parameters,
                             timeout=self.timeout)
            if r.status_code == 200:
                if len(r.json()) > 0:
                    self.save_to_file(parameters=parameters, data=r.json())
                else:
                    logging.error(
                        f" Request to url: {self.app_config['url'] + self.app_config['endpoint']} with parameters: {parameters} return empty JSON answer")
            else:
                print(
                    f" Request to url: {self.app_config['url'] + self.app_config['endpoint']} with parameters: {parameters} return bad response state_code {str(r.status_code)} with message: {r.text}")
        except Exception as e:
            logging.error(
                f" Request to url: {self.app_config['url'] + self.app_config['endpoint']} with parameters: {self.app_config['parameters']} return Exception: {str(e)}")

    def save_to_file(self, parameters: json, data: json):
        temp_path = './temp'

        os.makedirs(temp_path, exist_ok=True)
        directory_path = os.path.join(temp_path, self.app_name)
        os.makedirs(directory_path, exist_ok=True)
        directory_path = os.path.join(directory_path, parameters[self.app_config['data parameter']])
        os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, self.app_name + '.json')
        with open(file_path, 'w') as f:
            json.dump(data, f)

        hdfs_path = os.path.join(self.hdfs_path, self.app_name, self.date[0:4], self.date[0:7])
        hdfs_file_path = os.path.join(hdfs_path,
                                      self.app_name + '_' + parameters[self.app_config['data parameter']] + '.json')

        whh = WebHDFSHook(self.hdfs_conn_id)
        whdfs_client = whh.get_conn()
        whdfs_client.makedirs(hdfs_path)
        self.log.info(f'End write data from file {file_path} to hdfs {hdfs_file_path}')
        whh.load_file(file_path, hdfs_file_path, overwrite=True)
