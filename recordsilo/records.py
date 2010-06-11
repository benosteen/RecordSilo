#!/usr/bin/env python

from __future__ import with_statement

from persiststate import PersistentState
from rdfmanifest import RDFManifest

from pairtree import id_encode, id_decode, ppath
from pairtree import FileNotFoundException, ObjectNotFoundException

from datetime import datetime

from os import mkdir, rename

import os

from shutil import copy2

import simplejson

import logging

import sys, traceback

import re

logger = logging.getLogger("RecordSilo")
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)

logger.addHandler(ch)

PAIRTREE_ROOT_DIR = "pairtree_root"

NAMASTE_PATTERN = re.compile(r"[^0=|1=|2=|3=|4=|5=]")  # Must try hard to better this regex

class HarvestedRecord(object):
    """Convenience class, handling the persistence of some basic metadata about a harvest item, as well as organising the items files, metadata or otherwise."""
    def __init__(self, pairtree_object, date=None, manifest_filename="__manifest.json"):
        self.po = pairtree_object
        self.item_id = self.po.id
        self.uri = self.po.uri
        self.manifest_filename = manifest_filename
        if not date:
            date = datetime.now().isoformat()
        self.itempath = self.path_to_item()
        self.revert(date=date)
        self.files=None
        self.versions=None
        self.currentversion=None
    
    def __setattribute__(self,name, value):
        if name not in ['files', 'versions', 'currentversion', 'date']:
            if name == "metadata":
                self.manifest['metadata'] = value
            else:
                return object.__setattribute__(self, name, value)
        else:
            logger.error("Attribute %s cannot be set in this manner" % name)
            raise Exception
    
    def __getattribute__(self,name):
        if name=='files':
            return self.manifest['files'][self.manifest['currentversion']]
        elif name=='versions':
            return self.manifest['versions']
        elif name=='currentversion':
            return self.manifest['currentversion']
        elif name=='date':
            return self.manifest['date']
        elif name=='metadata':
            if not self.manifest.has_key('metadata'):
                self.manifest['metadata'] = {}
            return self.manifest['metadata']
        else:
            return object.__getattribute__(self, name)

    def __repr__(self): return repr(self.manifest)
    def __len__(self): return len(self.manifest['files'])
    
    def _setup_version_dir(self, version, date=None):
        if not date:
            if self.manifest['date']:
                date = self.manifest['date']
            else:
                date = datetime.now().isoformat()
        if version not in self.manifest['versions']:
            self.manifest['versions'].append(version)
        self.manifest['version_dates'][version] = date
        self.manifest['subdir'][version] = []
        self.manifest['metadata_files'][version] = []
        self.manifest['files'][version] = []
        self.set_version_date(version, date)
        self.po.add_bytestream_by_path(os.path.join("__"+str(version), "4=%s" % id_encode(self.item_id)), self.item_id)

    def _init_manifests_emptydatastructures(self):
        self.item_id = self.manifest['item_id']
        if not self.manifest.has_key('metadata_files'):
            self.manifest['metadata_files'] = {}
        if not self.manifest.has_key('subdir'):
            self.manifest['subdir'] = {}
        if not self.manifest.has_key('files'):
            self.manifest['files'] = {}
        for version in self.manifest['versions']:
            if not self.manifest['metadata_files'].has_key(version):
                self.manifest['metadata_files'][version] = []
            if not self.manifest['subdir'].has_key(version):
                self.manifest['subdir'][version] = []

    def _reload_filelist(self, version):
        if self.manifest['files'].has_key(version) and version in self.manifest['versions']:
            self.manifest['files'][version] = []
            self.manifest['subdir'][version] = []
            # init from disc
            for filename in [x for x in self.po.list_parts("__"+str(version)) if not (x.startswith("0=") or x.startswith("1=") or x.startswith("2=") or x.startswith("3=") or x.startswith("4=") or x.startswith("5=")) ]:
                logger.debug("Item %s has file: %s" % (self.item_id, x) )
                if self.po.isdir(os.path.join("__"+str(version), filename)):
                    self.manifest['subdir'][version].append(filename)
                self.manifest['files'][version].append(filename)

    def _init_manifest(self):
        """Set up the template for the item's manifest"""
        self.manifest['metadata_files'] = {}
        self.manifest['files'] = {}
        self.manifest['item_id'] = self.po.id
        self.manifest['versions'] = []
        self.manifest['version_dates'] = {}
        self.manifest['subdir'] = {}
        self.manifest['currentversion'] = "1"
        self._setup_version_dir("1", self.manifest['date'])
        self.manifest.sync()
    
    def _read_date(self, version = None):
        if not version:
            version = self.manifest['currentversion']
        date_namaste_tags = [x for x in self.po.list_parts("__"+str(version)) if x.startswith("3=")]
        if len(date_namaste_tags) >= 1:
            lmd = date_namaste_tags.pop()[2:]   # take the first tag and remove the '3='
            lmd = id_decode(lmd)                # reverse the 'pairtree' encoding of the date
            self.manifest['date'] = lmd

    def _incr_version(self, latest_version):
        try:
            v = int(latest_version)
            return str(v+1)
        except:
            return latest_version + "_new"

    def _copy_version(self, latest_version, new_version, exclude_filenames=[]):
        version_state = self.manifest['currentversion']
        self.set_version_cursor(latest_version)
        self._reload_filelist(new_version)
        for x in [y for y in self.manifest['files'][latest_version] if y not in exclude_filenames]:
            self._copy_file(x, latest_version, new_version, sync=False)
        self.set_version_cursor(version_state)
    
    def _copy_file(self, filename, latest_version, new_version, sync = True):
        with self.get_stream(filename, version=latest_version) as filetostream:
            metadata = False
            if filename in self.manifest['metadata_files'][latest_version]:
                metadata = True
            self.put_stream(filename, filetostream, version=new_version, metadata=metadata, sync=sync)

    def path_to_item(self):
        return self.po.fs._id_to_dirpath(self.po.id)

    def revert(self, **kw):
        if not os.path.isdir(self.itempath):
            logger.error("Path to harvested item does not exist")
            raise Exception("Path to harvested item does not exist")
        try:
            self.manifest = PersistentState(self.itempath, self.manifest_filename)
            self.manifest.revert()
            if not self.manifest:
                if kw.has_key('date'):
                    self.manifest['date'] = kw['date']
                else:
                    self.manifest['date'] = datetime.now().isoformat()
                self._init_manifest()
                logger.debug(self.manifest)
            self._init_manifests_emptydatastructures()
            logger.debug(self.manifest)
        except Exception, e:
            logger.error("Failed to setup on-disc stored state for this item.")
            traceback.print_exc(file=sys.stdout)

    # Some dict methods
    def __setitem__(self, key, item):
        if key not in ["metadata_files", "files"]:
            self.manifest[key]
    def __getitem__(self, key):
        try:
            return self.manifest[key]
        except KeyError:
            raise KeyError(key)
        
    def keys(self): return self.manifest.keys()
    def has_key(self, key): return self.manifest.has_key(key)
    def items(self): return self.manifest.items()
    def values(self): return self.manifest.values()

    def sync(self):
        self.manifest.sync()
        
    def revert_manifest(self):
        self.manifest.revert()
        self._init_manifests_emptydatastructures()
    
    def put_stream(self, filename, filetostream, version=None, metadata=False, sync=True):
        if not version:
            version = self.manifest['currentversion']
        if metadata and filename not in self.manifest['metadata_files'][version]:
            self.manifest['metadata_files'][version].append(filename)
        if filename not in self.manifest['files'][version]:
            self.manifest['files'][version].append(filename)
        resp = self.po.add_bytestream_by_path(os.path.join("__" + str(version), filename), filetostream)
        self._reload_filelist(version)
        if sync:
            self.sync()
        return resp
    def get_stream(self, filename, version=None, writeable=False):
        """NB If writeable is set to True, then the file is opened "wb+" and can accept writes.
        Otherwise, the file is opened read-only."""
        if not version:
            version = self.manifest['currentversion']
        if self.isfile(filename, version):
            return self.po.get_bytestream_by_path(os.path.join("__" + str(version), filename),
                                                  streamable=True, appendable=writeable)
        else:
            raise FileNotFoundException
    
    def del_stream(self, filename, versions=[]):
        if not versions:
            versions = [self.manifest['currentversion']]
        for version in versions:
            try:
                self.po.del_file_by_path(os.path.join("__" + str(version), filename))
                if self.isfile(filename, version):
                    self.manifest['metadata_files'][version].remove(filename)
                self._reload_filelist(version)
            except FileNotFoundException:
                logger.info("File %s not found at version %s and so cannot be deleted" % (filename, version))
        self.sync()

    def isfile(self, filepath, version=None):
        if not version:
            version = self.manifest['currentversion']
        return self.po.isfile(os.path.join("__"+str(version), filepath))

    def isdir(self, filepath, version=None):
        if not version:
            version = self.manifest['currentversion']
        return self.po.isdir(os.path.join("__"+str(version),filepath))

    def list_parts(self, subpath=""):
        if self.isdir(subpath):
            return self.po.list_parts(os.path.join("__"+str(self.manifest['currentversion']),subpath))
    
    def to_dirpath(self, filepath=None, version=None):
        if not version:
            version = self.currentversion
        if filepath:
            return os.path.join(ppath.id_to_dirpath(self.po.id, self.po.fs.pairtree_root), "__%s" % version, filepath)
        else:
            return os.path.join(ppath.id_to_dirpath(self.po.id, self.po.fs.pairtree_root), "__%s" % version)

    def stat(self, filepath, version=None):
        if not version:
            version = self.currentversion
        return self.po.stat(filepath)

    def set_version_cursor(self, version):
        if version in self.manifest['versions']:
            self.manifest['currentversion'] = version
            self._read_date()
            return True
        else:
            logger.error("Version %s does not exist" % version)
            return False
        
    def get_versions(self):
        return self.manifest['versions']

    def increment_version(self, date=None, clone_previous_version=False):
        if not date:
            date = datetime.now().isoformat()
        self.manifest['date'] = date
        latest_version = self.manifest['currentversion']
        new_version = self._incr_version(latest_version)
        self._setup_version_dir(new_version, date)
        self.set_version_cursor(new_version)
        self._read_date()
        if clone_previous_version:
            self._copy_version(latest_version, new_version)
        self.sync()
        return new_version

    def move_directory_as_new_version(self, src_directory, version=None, force=False, date=None, _sync=True):
        if not date:
            date = datetime.now().isoformat()
        self.manifest['date'] = date
        if not version:
            version = self._incr_version(self.currentversion)
        if version in self.get_versions():
            if force:
                self.del_version(version)
            else:
                raise Exception("Cannot move a directory onto an already existing version")
        version_path = os.path.join(ppath.id_to_dirpath(self.po.id, self.po.fs.pairtree_root), "__%s" % version)
        os.rename(src_directory, version_path)
        self._setup_version_dir(version, date)
        self._read_date()
        self._reload_filelist(version)
        self.set_version_cursor(version)
        if _sync:
            self.sync()
        return version

    def clone_version(self, original_version, new_version, exclude_filenames=[]):
        if original_version in self.manifest['versions']:
            date = self.manifest['version_dates'][original_version]
            self._setup_version_dir(new_version, date)
            self._read_date()
            self._copy_version(original_version, new_version, exclude_filenames)
            self.set_version_cursor(new_version)
            self.sync()
            return new_version
        else:
            logger.error("Version %s is not found in the object. Cannot be cloned" % original_version)
            return False

    def copy_file_between_versions(self, filename, from_version, to_version):
        if from_version in self.manifest['versions'] and to_version in self.manifest['versions'] and filename in self.manifest['files'][from_version]:
            self._copy_file(filename, from_version, to_version)

    def rename_version(self, original_version, new_name):
        if original_version in self.manifest['versions']:
            self.manifest['versions'].append(new_name)
            self.manifest['version_dates'][new_name] = self.manifest['version_dates'][original_version]
            self.manifest['files'][new_name] = self.manifest['files'][original_version]
            self.manifest['metadata_files'][new_name] = self.manifest['metadata_files'][original_version]
            rename(os.path.join(self.path_to_item(), "__"+str(original_version)), os.path.join(self.path_to_item(), "__" + str(new_name)))
            self.set_version_cursor(new_name)
            self.manifest['versions'].remove(original_version)
            del self.manifest['version_dates'][original_version]
            del self.manifest['files'][original_version]
            del self.manifest['metadata_files'][original_version]
            self.sync()
            return new_name
        else:
            logger.error("Version %s is not found in the object. Cannot be renamed" % original_version)
            return False

    def create_new_version(self, version, date=None):
        version = str(version)
        if version not in self.manifest['versions']:
            self._setup_version_dir(version, date)
            self.set_version_cursor(version)
            self.sync()
        else:
            logger.error("Cannot create new version %s - version directory already exists" % version)

    def del_version(self, version):
        if version not in self.manifest['versions']:
            logger.error("Version %s does not exist" % version)
            return False
        else:
            self.manifest['versions'].remove(version)
            if self.manifest['files'].has_key(version):
                del self.manifest['files'][version]
            if self.manifest['metadata_files'].has_key(version):
                del self.manifest['metadata_files'][version]
            if self.manifest['versions']:
                # TODO revert object manifest to previous version if current version is being deleted
                self.manifest['currentversion'] = self.manifest['versions'][-1]
                self._read_date()
            else:
                self.manifest['currentversion'] = "1"
                self._read_date()
                self._setup_version_dir(version, date)
            self.sync()
            return self.po.del_path("__"+str(version), recursive=True)

    def del_versions(self, versions=[]):
        results = []
        for version in versions:
            results.append(self.del_version(version))
        return results

    def set_version_date(self, version, date):
        if version in self.manifest['versions']:
            self.manifest['version_dates'][version] = date
            self.po.add_bytestream_by_path(os.path.join("__"+str(version), "3=%s" % id_encode(date)), date)
            return True
        else:
            logger.error("Version %s does not exist" % version)
            return False

    def get_version_cursor(self):
        return self.manifest['currentversion']

class RDFRecord(HarvestedRecord):
    def __init__(self, pairtree_object, date=None, rdf_manifest_filename="manifest.rdf", rdf_manifest_format="xml", manifest_filename="__manifest.json"):
        super(RDFRecord, self).__init__(pairtree_object, date=None, manifest_filename="__manifest.json")
        self.set_rdf_manifest_filename(rdf_manifest_filename, format=rdf_manifest_format)
        
    def set_rdf_manifest_filename(self, filename, format="xml"):
        self.manifest['rdffilename'] = filename
        self.manifest['rdffileformat'] = format
        self.sync()
    
    def _path_to_rdfmanifest(self, version=None):
        if not version:
            version = self.get_version_cursor()
        fn = self.manifest.get('rdffilename', 'manifest.rdf')
        return os.path.join(self.po.fs._id_to_dirpath(self.po.id), "__"+str(version), fn)

    def load_rdf_manifest(self, version=None):
        format = self.manifest.get('rdffileformat', 'xml')
        fpath = self._path_to_rdfmanifest()
        if self.manifest.has_key('rdffilename') and self.manifest['rdffilename'] not in self.manifest['files'][self.manifest['currentversion']]:
            self.manifest['files'][self.manifest['currentversion']].append(self.manifest['rdffilename'])
        self._rdfmanifest = RDFManifest(fpath, format=format, uri=self.po.uri)
    
    def get_rdf_manifest(self):
        if not self._rdfmanifest:
            self.load_rdf_manifest()
        return self._rdfmanifest

    def add_triple(self, s, p, o):
        return self._rdfmanifest.add_triple(s,p,o)
    def add_namespace(self, prefix, uri):
        return self._rdfmanifest.add_namespace(prefix, uri)
    def del_triple(self, s, p, o=None):
        return self._rdfmanifest.del_triple(s,p,o)
    def del_namespace(self, prefix):
        return self._rdfmanifest.del_namespace(prefix)
    def get_graph(self):
        return self._rdfmanifest.get_graph()
    def get_graph(self):
        return self._rdfmanifest.get_graph()
    def rdf_to_string(self, format="xml"):
        return self._rdfmanifest.to_string(format)
    
    ##############
    ## Classes to annotate with rdf manifest updating
    
    def put_stream(self, filename, filetostream, version=None, metadata=False, sync=True):
        super(RDFRecord, self).put_stream(self, filename, filetostream, version=version, metadata=metadata, sync=False)
        self.add_triple(self.uri, "ore:aggregates", "%s/%s" % (self.uri, filename))
        if sync:
            self.sync()
    
    def set_version_cursor(self, version):
        super(RDFRecord, self).set_version_cursor(version)
        self.load_rdf_manifest()
        
    def revert(self, **kw):
        super(RDFRecord, self).revert(**kw)
        if self.manifest.has_key('rdffilename') and self.manifest['rdffilename'] not in self.manifest['files'][self.manifest['currentversion']]:
            self.manifest['files'][self.manifest['currentversion']].append(self.manifest['rdffilename'])
        self.load_rdf_manifest()
        
    def sync(self):
        super(RDFRecord, self).sync()
        if self.manifest.has_key('rdffilename') and self.manifest['rdffilename'] not in self.manifest['files'][self.manifest['currentversion']]:
            self.manifest['files'][self.manifest['currentversion']].append(self.manifest['rdffilename'])
        if self._rdfmanifest: 
            self._rdfmanifest.sync()

    def _copy_version(self, latest_version, new_version, exclude_filenames=[]):
        super(RDFRecord, self)._copy_version(latest_version, new_version, exclude_filenames)
        self.load_rdf_manifest()
    
    def move_directory_as_new_version(self, src_directory, version=None, force=False, date=None, _sync=True):
        super(RDFRecord, self).move_directory_as_new_version(src_directory, version=version, force=force, date=date, _sync=False)
        self.load_rdf_manifest()
        if _sync:
            self.sync()
