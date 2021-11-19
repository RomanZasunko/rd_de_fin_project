import yaml


class Config:

    def __init__(self, path: str):
        with open(path, 'r') as yaml_file:
            self.__config = yaml.safe_load(yaml_file)

    def get_config(self, application: str) -> dict:
        return self.__config.get(application)