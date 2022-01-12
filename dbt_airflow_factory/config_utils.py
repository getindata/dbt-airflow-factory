from __future__ import annotations

import logging
import os
from typing import Union

import yaml


def read_config(
    dag_path: Union[str, os.PathLike[str]], env: str, file_name: str
) -> dict:
    config = read_env_config(dag_path, "base", file_name)
    config.update(read_env_config(dag_path, env, file_name))
    return config


def read_env_config(
    dag_path: Union[str, os.PathLike[str]], env: str, file_name: str
) -> dict:
    config_file_path = os.path.join(dag_path, "config", env, file_name)
    if os.path.exists(config_file_path):
        logging.info("Reading config from " + config_file_path)
        return read_yaml_file(config_file_path)
    logging.warning("Missing config file: " + config_file_path)
    return {}


def read_yaml_file(file_path: Union[str, os.PathLike[str]]) -> dict:
    with open(file_path, "r") as f:
        return yaml.safe_load(f)
