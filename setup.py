from setuptools import setup, find_packages

setup(
    name='shared-utils',
    version='0.1.4',
    description='Shared utility functions for MS Fabric',
    packages=find_packages(),
    install_requires=[
        'duckdb',
'deltalake',
'loguru',
'cuallee'
    ]
)