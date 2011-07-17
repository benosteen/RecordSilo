from ez_setup import use_setuptools
use_setuptools()
from setuptools import setup, find_packages

setup(name="RecordSilo",
      version="0.4.13",
      description="An adaptation of a pairtree store, each object with simple JSON keyvalue manifest and crude versioning.",
      long_description="""An adaptation of a pairtree store, each object with simple JSON keyvalue manifest and crude versioning. 
Designed to be used as a repository of harvested records from OAI-PMH based services and the like. 
As of version 0.3, it now includes an RDF-enhanced version of the Silo - RDFSilo.
Version 0.4.9: Extended the versioning capabalities a bit. Added functionality to use symlinks when using copy, clone or increment of version. 
Version 0.4.10: Added a function to calculate the disk usage.
Version 0.4.11: Can customize the start version for datasets in silos. The default startversion is 1.
Version 0.4.12: Wrote a helper function del_dir in the RDFRecord class, to be able to recursively delete directories
Version 0.4.13: Removed the dependency on rdfobject and instead have added a simple manifesthelper class (which is based on rdfobject)""",
      author="Ben O'Steen, Anusha Ranganathan",
      author_email="bosteen@gmail.com / anusha3@gmail.com",
      packages=find_packages(exclude='tests'),
      #install_requires=['pairtree>0.5.4', 'rdfobject>=0.4', 'simplejson', 'datetime'],
      install_requires=['pairtree>0.5.4', 'simplejson', 'datetime'],
      )
