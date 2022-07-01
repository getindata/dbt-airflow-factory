``DBT Airflow Factory``
=======================

.. image:: https://img.shields.io/badge/python-3.7%20%7C%203.8%20%7C%203.9-blue.svg
   :target: https://github.com/getindata/dbt-airflow-factory
   :alt: Python Version

.. image:: https://badge.fury.io/py/dbt-airflow-factory.svg
   :target: https://pypi.org/project/dbt-airflow-factory/
   :alt: PyPI Version

.. image:: https://pepy.tech/badge/dbt-airflow-factory
   :target: https://pepy.tech/project/dbt-airflow-factory
   :alt: Downloads

.. image:: https://api.codeclimate.com/v1/badges/47fd3570c858b6c166ad/maintainability
   :target: https://codeclimate.com/github/getindata/dbt-airflow-factory/maintainability
   :alt: Maintainability

.. image:: https://api.codeclimate.com/v1/badges/47fd3570c858b6c166ad/test_coverage
   :target: https://codeclimate.com/github/getindata/dbt-airflow-factory/test_coverage
   :alt: Test Coverage

Introduction
------------
The factory is a library for parsing DBT manifest files and building Airflow DAG.

The library is expected to be used inside an Airflow environment with a Kubernetes image referencing **dbt**.

**dbt-airflow-factory**'s main task is to parse ``manifest.json`` and create Airflow DAG out of it. It also reads config
`YAML` files from ``config`` directory and therefore is highly customizable (e.g., user can set path to ``manifest.json``).
DAG building is an on-the-fly process without materialization. Also, the process may use  Airflow Variables as a way of configuration.

Community
------------
Although the tools was created by GetInData and used in their project it is open-sourced and every one is welcome to use and contribute to make it better and even more usefull.

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   installation
   usage
   configuration
   cli
   features
   api
   changelog
