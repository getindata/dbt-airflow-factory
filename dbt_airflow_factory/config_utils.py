"""Utilities for configuration files reading."""

from __future__ import annotations

import logging
import os
from typing import Union

import yaml


def read_config(
    dag_path: Union[str, os.PathLike[str]], env: str, file_name: str
) -> dict:
    """
    Reads dictionaries out of *file_name* in both `base` and *env* directories,
    and compiles them into one. Values from *env* directory get precedence over
    `base` ones

    :param dag_path: Path to the directory containing ``config`` directory.
    :type dag_path: Union[str, os.PathLike[str]]
    :param env: Name of the environment.
    :type env: str
    :param file_name: Name of the config file.
    :type file_name: str
    :return: Dictionary representing the config file.
    :rtype: dict
    """
    config = read_env_config(dag_path, "base", file_name)
    config.update(read_env_config(dag_path, env, file_name))
    return config


def read_env_config(
    dag_path: Union[str, os.PathLike[str]], env: str, file_name: str
) -> dict:
    """
    Read config file, depending on the ``env``.

    :param dag_path: Path to the directory containing ``config`` directory.
    :type dag_path: Union[str, os.PathLike[str]]
    :param env: Name of the environment.
    :type env: str
    :param file_name: Name of the config file.
    :type file_name: str
    :return: Dictionary representing the config file.
    :rtype: dict
    """
    config_file_path = os.path.join(dag_path, "config", env, file_name)
    if os.path.exists(config_file_path):
        logging.info("Reading config from " + config_file_path)
        return read_yaml_file(config_file_path)
    logging.warning("Missing config file: " + config_file_path)
    return {}


def read_yaml_file(file_path: Union[str, os.PathLike[str]]) -> dict:
    """
    Load `yaml` file to dictionary.

    :param file_path: Path to the file.
    :type file_path: Union[str, os.PathLike[str]]
    :return: Loaded dictionary.
    :rtype: dict
    """
    with open(file_path, "r") as f:
        return yaml.safe_load(f)
