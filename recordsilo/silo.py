#!/usr/bin/env python

# OAIPMH Scraper

from __future__ import with_statement

from persiststate import PersistentState

from pairtree import PairtreeStorageClient
from pairtree import id_encode, id_decode
from pairtree import FileNotFoundException, ObjectNotFoundException

from datetime import datetime

from os import path, mkdir, rename

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
        self.po.add_bytestream_by_path(path.join("__"+str(version), "4=%s" % id_encode(self.item_id)), self.item_id)

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
                if self.po.isdir(path.join("__"+str(version), filename)):
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
        version_state = self.manifest.currentversion
        self.set_version_cursor(new_version)
        for x in [y for y in self.manifest['files'][latest_version] if y not in exclude_filenames]:
            self._copy_file(x, latest_version, new_version, sync=False)
        self.set_version_cursor(version_state)
        self.sync()
    
    def _copy_file(self, filename, latest_version, new_version, sync = True):
        with self.get_stream(filename, version=latest_version) as stream:
            metadata = False
            if filename in self.manifest['metadata_files'][latest_version]:
                metadata = True
            self.put_stream(filename, stream, metadata=metadata)
        if sync:
            self.sync()

    def path_to_item(self):
        return self.po.fs._id_to_dirpath(self.po.id)

    def revert(self, **kw):
        if not path.isdir(self.itempath):
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
    
    def put_stream(self, filename, stream, metadata=False):
        version = self.manifest['currentversion']
        if metadata and filename not in self.manifest['metadata_files'][version]:
            self.manifest['metadata_files'][version].append(filename)
        if filename not in self.manifest['files'][version]:
            self.manifest['files'][version].append(filename)
        self.po.add_bytestream_by_path(path.join("__" + str(version), filename), stream)
        self._reload_filelist(version)
        self.sync()
        return 
    def get_stream(self, filename, version=None, writeable=False):
        """NB If writeable is set to True, then the file is opened "wb+" and can accept writes.
        Otherwise, the file is opened read-only."""
        if not version:
            version = self.manifest['currentversion']
        if self.isfile(filename, version):
            return self.po.get_bytestream_by_path(path.join("__" + str(version), filename),
                                                  streamable=True, appendable=writeable)
        else:
            raise FileNotFoundException
    
    def del_stream(self, filename, versions=[]):
        if not versions:
            versions = [self.manifest['currentversion']]
        for version in versions:
            try:
                self.po.del_file_by_path(path.join("__" + str(version), filename))
                if self.isfile(filename, version):
                    self.manifest['metadata_files'][version].remove(filename)
                self._reload_filelist(version)
            except FileNotFoundException:
                logger.info("File %s not found at version %s and so cannot be deleted" % (filename, version))
        self.sync()

    def isfile(self, filepath, version=None):
        if not version:
            version = self.manifest['currentversion']
        return self.po.isfile(path.join("__"+str(version), filepath))

    def isdir(self, filepath, version=None):
        if not version:
            version = self.manifest['currentversion']
        return self.po.isdir(path.join("__"+str(version),filepath))

    def list_parts(self, subpath):
        if self.isdir(subpath):
            return self.po.list_parts(path.join("__"+str(self.manifest['currentversion']),subpath))

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

    def clone_version(self, original_version, new_version, exclude_filenames=[]):
        if original_version in self.manifest['versions']:
            date = self.manifest['version_dates'][original_version]
            self._setup_version_dir(new_version, date)
            self.set_version_cursor(new_version)
            self._read_date()
            self._copy_version(original_version, new_version, exclude_filenames)
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
            rename(path.join(self.path_to_item(), "__"+str(original_version)), path.join(self.path_to_item(), "__" + str(new_name)))
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
            self.po.add_bytestream_by_path(path.join("__"+str(version), "3=%s" % id_encode(date)), date)
            return True
        else:
            logger.error("Version %s does not exist" % version)
            return False

    def get_version_cursor(self):
        return self.manifest['currentversion']

class Silo(object):
    """Item persistence layer - uses pairtree as a basis for storage."""
    def __init__(self, storage_dir, uri_base=None, **kw):
        self.state = PersistentState()
        self.state['storage_dir'] = storage_dir
        if not uri_base:
            uri_base = "info:"
        self.state['uri_base'] = uri_base
        self.state.update(kw)
        self._init_storage()
        
    def _init_storage(self):
        try:
            self._store = PairtreeStorageClient(self.state['uri_base'], self.state['storage_dir'], shorty_length=2)
            if not self.state.set_filepath(self.state['storage_dir']):
                raise Exception
            else:
                self.state.revert()
        except OSError:
            logger.error("Cannot make storage directory")
            raise Exception("Cannot make storage directory")
        except Exception:
            logger.error("Cannot setup the state persistence file at %s/%s" % (self.state['storage_dir'], PERSISTENCE_FILENAME))
            raise Exception("Cannot setup the state persistence file at %s/%s" % (self.state['storage_dir'], PERSISTENCE_FILENAME))

    def __iter__(self):
        return self.list_items()

    def iteritems(self):
        for item in self.list_items():
            yield self.get_item(item)

    def __getitem__(self, key):
        if self.exists(key):
            return self.get_item(key)

    def __len__(self): return len(self.keys())

    def keys(self): return [x for x in self.__iter__()]
    def has_key(self, key): return self.exists(key)
    def exists(self, item_id):
        return self._store.exists(item_id)

    def get_item(self, item_id, date=None, force=False):
        if self.exists(item_id):
            p_obj = self._store.get_object(item_id)
            return HarvestedRecord(p_obj, date)
        elif self.exists(self.state['uri_base'] + item_id) and not force:
            p_obj = self._store.get_object(self.state['uri_base'] + item_id)
            return HarvestedRecord(p_obj, date)
        else:
            p_obj = self._store.get_object(item_id)
            return HarvestedRecord(p_obj, date)

    def del_item(self, item_id):
        if self.exists(item_id):
            return self._store.delete_object(item_id)
        elif self.exists(self.state['uri_base'] + item_id):
            return self._store.delete_object(self.state['uri_base'] + item_id)
        else:
            raise ObjectNotFoundException

    def list_items(self):
        return self._store.list_ids()

