"""kedro_airflow_k8s module."""

from setuptools import find_packages, setup

with open("README.md") as f:
    README = f.read()

# Runtime Requirements.
INSTALL_REQUIRES = []

# Dev Requirements
EXTRA_REQUIRE = {
    "tests": [
        "pytest>=6.2.2, <7.0.0",
        "pytest-cov>=2.8.0, <3.0.0",
        "tox==3.21.1",
        "pre-commit==2.9.3",
        "pandas==1.2.5",
        "apache-airflow[kubernetes]==1.10.14",
    ],
    "docs": [
        "sphinx==3.4.2",
        "recommonmark==0.7.1",
        "sphinx_rtd_theme==0.5.2",
    ],
}

setup(
    name="dbt-airflow-manifest-parser",
    version="0.2.0",
    description="Library to convert DBT manifest metadata to Airflow tasks",
    long_description=README,
    long_description_content_type="text/markdown",
    license="Apache Software License (Apache 2.0)",
    python_requires=">=3",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    keywords="dbt airflow manifest parser python",
    author=u"Piotr Pekala",
    author_email="piotr.pekala@getindata.com",
    url="https://github.com/getindata/dbt-airflow-manifest-parser/",
    packages=find_packages(exclude=["ez_setup", "examples", "tests", "docs"]),
    include_package_data=True,
    zip_safe=False,
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRA_REQUIRE,
)
