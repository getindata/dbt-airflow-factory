"""Utilities for configuration files reading."""

from __future__ import annotations

import logging
import os
import pathlib
from typing import Any, Union

import yaml
from airflow.models import Variable
from jinja2 import FileSystemLoader
from jinja2.nativetypes import NativeEnvironment


def read_config(
    dag_path: Union[str, os.PathLike[str]],
    env: str,
    file_name: str,
    replace_jinja: bool = False,
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
    :param replace_jinja: Whether replace Airflow vars using Jinja templating.
    :type replace_jinja: bool
    :return: Dictionary representing the config file.
    :rtype: dict
    """
    config = read_env_config(dag_path, "base", file_name, replace_jinja)
    config.update(read_env_config(dag_path, env, file_name, replace_jinja))
    return config


def read_env_config(
    dag_path: Union[str, os.PathLike[str]],
    env: str,
    file_name: str,
    replace_jinja: bool = False,
) -> dict:
    """
    Read config file, depending on the ``env``.

    :param dag_path: Path to the directory containing ``config`` directory.
    :type dag_path: Union[str, os.PathLike[str]]
    :param env: Name of the environment.
    :type env: str
    :param file_name: Name of the config file.
    :type file_name: str
    :param replace_jinja: Whether replace Airflow vars using Jinja templating.
    :type replace_jinja: bool
    :return: Dictionary representing the config file.
    :rtype: dict
    """
    config_file_path = os.path.join(dag_path, "config", env, file_name)
    if os.path.exists(config_file_path):
        logging.info("Reading config from " + config_file_path)
        return read_yaml_file(config_file_path, replace_jinja)
    logging.warning("Missing config file: " + config_file_path)
    return {}


def read_yaml_file(file_path: Union[str, os.PathLike[str]], replace_jinja: bool) -> dict:
    """
    Load `yaml` file to dictionary.

    :param file_path: Path to the file.
    :type file_path: Union[str, os.PathLike[str]]
    :param replace_jinja: Whether replace Airflow vars using Jinja templating.
    :type replace_jinja: bool
    :return: Loaded dictionary.
    :rtype: dict
    """
    if replace_jinja:
        return yaml.safe_load(_jinja_replace_airflow_vars(file_path))

    with open(file_path, "r") as f:
        return yaml.safe_load(f)


def _jinja_replace_airflow_vars(file_path: Union[str, os.PathLike[str]]) -> str:
    # Copied from airflow.models.taskinstance
    class VariableAccessor:
        def __init__(self) -> None:
            self.var = None

        def __getattr__(self, item: str) -> Any:
            self.var = Variable.get(item)
            return self.var

        def __repr__(self) -> str:
            return str(self.var)

        @staticmethod
        def get(item: str) -> Any:
            return Variable.get(item)

    file_path = pathlib.Path(file_path)
    jinja_loader = FileSystemLoader(str(file_path.parent))
    jinja_env = NativeEnvironment(loader=jinja_loader)

    return jinja_env.get_template(file_path.name).render(var={"value": VariableAccessor()})
