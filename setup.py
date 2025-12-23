"""dbt_airflow_factory module."""

from setuptools import find_packages, setup

with open("README.md") as f:
    README = f.read()

# Runtime Requirements.
INSTALL_REQUIRES = [
    "pytimeparse>=1.1, <2",
    "astronomer-cosmos>=1.10.0, <2.0",
    "apache-airflow>=2.5, <3",
    "apache-airflow-providers-cncf-kubernetes",
    "apache-airflow-providers-docker",
    "apache-airflow-providers-slack",
    "apache-airflow-providers-airbyte>=3.1",
]

# Dev Requirements
EXTRA_REQUIRE = {
    "tests": [
        "pytest>=6.2.2, <7.0.0",
        "pytest-cov>=2.8.0, <3.0.0",
        "tox==3.21.1",
        "pre-commit==2.9.3",
        "pandas>=1.2.5, <2.0.0",
    ],
    "docs": [
        "sphinx==4.3.1",
        "sphinx-rtd-theme==1.0.0",
        "sphinx-click>=3.0,<3.1",
        "myst-parser>=0.16, <0.17",
        "docutils<0.17",
    ],
}

setup(
    name="dbt-airflow-factory",
    version="1.0.0",
    description="Library to convert DBT manifest metadata to Airflow tasks",
    long_description=README,
    long_description_content_type="text/markdown",
    license="Apache Software License (Apache 2.0)",
    python_requires=">=3.9",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    keywords="dbt airflow manifest parser python",
    author="Piotr Pekala",
    author_email="piotr.pekala@getindata.com",
    url="https://github.com/getindata/dbt-airflow-factory/",
    packages=find_packages(exclude=["ez_setup", "examples", "tests", "docs"]),
    include_package_data=True,
    zip_safe=False,
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRA_REQUIRE,
)
