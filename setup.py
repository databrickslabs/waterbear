import setuptools
from setuptools import find_packages

setuptools.setup(
    name='lh4fs-schema',
    version='1.0',
    author='Antoine Amend',
    author_email='antoine.amend@databricks.com',
    description='Automated provisioning of the Lakehouse for Financial Services with data model',
    long_description_content_type='text/markdown',
    url='https://github.com/databrickslabs/lh4fs-schema',
    packages=find_packages(where='.', include=['lh4fs']),
    extras_require=dict(tests=["pytest"]),
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
)
