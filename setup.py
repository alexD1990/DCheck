from setuptools import setup, find_packages

setup(
    name="dcheck",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.1",
    ],
    tests_require=[
        "pytest>=6.0",
    ],
)
