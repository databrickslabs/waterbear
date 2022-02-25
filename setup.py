import setuptools
from setuptools import find_packages

setuptools.setup(
    name='lh4fs-json',
    version='1.0',
    author='Antoine Amend',
    author_email='antoine.amend@databricks.com',
    description='Provisioning Lakehouse for Financial Services with JSON data model',
    long_description_content_type='text/markdown',
    url='https://github.com/databrickslabs/lh4fs-json',
    packages=find_packages(where='.', include=['databricks']),
    extras_require=dict(tests=["pytest"]),
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
)
