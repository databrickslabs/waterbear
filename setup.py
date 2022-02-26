import setuptools
from setuptools import find_packages

setuptools.setup(
    name='watergrade',
    version='1.0',
    author='Antoine Amend',
    author_email='antoine.amend@databricks.com',
    description='Automated provisioning of the Lakehouse for Financial Services with data model',
    long_description_content_type='text/markdown',
    url='https://github.com/databrickslabs/watergrade',
    packages=find_packages(where='.', include=['watergrade']),
    extras_require=dict(tests=["pytest"]),
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
)
