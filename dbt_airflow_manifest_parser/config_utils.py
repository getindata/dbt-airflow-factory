import logging
from os import path

import yaml


def read_config(dag_path, env, file_name):
    config = read_env_config(dag_path, "base", file_name)
    config.update(read_env_config(dag_path, env, file_name))
    return config


def read_env_config(dag_path, env, file_name):
    config_file_path = path.join(dag_path, "config", env, file_name)
    if path.exists(config_file_path):
        logging.info("Reading config from " + config_file_path)
        return read_yaml_file(config_file_path)
    logging.warning("Missing config file: " + config_file_path)
    return {}


def read_yaml_file(file_path):
    with open(file_path, "r") as f:
        return yaml.safe_load(f)
