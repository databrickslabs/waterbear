import setuptools
from setuptools import find_packages

setuptools.setup(
    name='dbl-waterbear',
    version='0.1',
    author='Antoine Amend',
    author_email='antoine.amend@databricks.com',
    description='Automated provisioning of an industry Lakehouse with enterprise data model',
    long_description_content_type='text/markdown',
    url='https://github.com/databrickslabs/waterbear',
    packages=find_packages(where='.'),
    extras_require=dict(tests=["pytest"]),
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
        'License :: Other/Proprietary License',
    ],
)
