from ez_setup import use_setuptools
use_setuptools()
from setuptools import setup, find_packages

setup(name="RecordSilo",
      version="0.2.7",
      description="An adaptation of a pairtree store, each object with simple JSON keyvalue manifest and crude versioning.",
      long_description="An adaptation of a pairtree store, each object with simple JSON keyvalue manifest and crude versioning. Designed to be used as a repository of harvested records from OAI-PMH based services and the like.",
      author="Ben O'Steen",
      author_email="bosteen@gmail.com",
      packages=find_packages(exclude='tests'),
      install_requires=['pairtree>0.4.12', 'simplejson', 'datetime'],
      )
