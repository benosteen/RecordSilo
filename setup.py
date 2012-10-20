from ez_setup import use_setuptools
use_setuptools()
from setuptools import setup, find_packages

setup(name="RecordSilo",
      version="0.4.16",
      description="An adaptation of a pairtree store, each object with simple JSON keyvalue manifest and crude versioning.",
      long_description="""An adaptation of a pairtree store, each object with simple JSON keyvalue manifest and crude versioning. 
Designed to be used as a repository of harvested records from OAI-PMH based services and the like. 
As of version 0.3, it now includes an RDF-enhanced version of the Silo - RDFSilo.""",
      author="Ben O'Steen, Anusha Ranganathan",
      author_email="bosteen@gmail.com / anusha3@gmail.com",
      packages=find_packages(exclude='tests'),
      #install_requires=['pairtree>0.5.4', 'rdfobject>=0.4', 'simplejson', 'datetime'],
      install_requires=['pairtree>0.5.4', 'simplejson'],
      )
