#!/usr/bin/env python

from __future__ import with_statement

from persiststate import PersistentState

from records import HarvestedRecord, RDFRecord

from pairtree import PairtreeStorageClient
from pairtree import id_encode, id_decode
from pairtree import FileNotFoundException, ObjectNotFoundException

from datetime import datetime

from os import path, mkdir, rename, listdir

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

class SiloNotFound(Exception):
    pass

class Silo(object):
    """Item persistence layer - uses pairtree as a basis for storage."""
    def __init__(self, storage_dir, uri_base=None, **kw):
        self.state = PersistentState()
        self.state['storage_dir'] = storage_dir
        if not uri_base:
            if self.state.has_key('uri_base') and self.state['uri_base']:
                uri_base = self.state['uri_base']
            else:
                self.state['uri_base'] = uri_base = "info:"
        else:
            self.state['uri_base'] = uri_base
        self.state.update(kw)
        self._init_storage()
        
    def _init_storage(self):
        try:
            if "hashing_type" in self.state.keys():
                self._store = PairtreeStorageClient(self.state['uri_base'], self.state['storage_dir'], shorty_length=2, hashing_type=self.state['hashing_type'])
            else:
                self._store = PairtreeStorageClient(self.state['uri_base'], self.state['storage_dir'], shorty_length=2)
            if not self.state.set_filepath(self.state['storage_dir']):
                raise Exception
            else:
                self.state.revert()
        except OSError:
            logger.error("Cannot make storage directory")
            raise Exception("Cannot make storage directory")
        except Exception, e:
            logger.error("Cannot setup the state persistence file at %s/%s" % (self.state['storage_dir'], "__manifest.json"))
            raise Exception("Cannot setup the state persistence file at %s/%s" % (self.state['storage_dir'], "__manifest.json"))

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


class RDFSilo(Silo):
    def get_item(self, item_id, date=None, force=False):
        if self.exists(item_id):
            p_obj = self._store.get_object(item_id)
            return RDFRecord(p_obj, date)
        elif self.exists(self.state['uri_base'] + item_id) and not force:
            p_obj = self._store.get_object(self.state['uri_base'] + item_id)
            return RDFRecord(p_obj, date)
        else:
            p_obj = self._store.get_object(item_id)
            return RDFRecord(p_obj, date)

    def del_item(self, item_id):
        if self.exists(item_id):
            return self._store.delete_object(item_id)
        elif self.exists(self.state['uri_base'] + item_id):
            return self._store.delete_object(self.state['uri_base'] + item_id)
        else:
            raise ObjectNotFoundException

class Granary(object):
    def __init__(self, dir_of_silos="data"):
        self.root_dir = dir_of_silos
        self._init_granary()
    
    def _init_granary(self):
        if not path.exists(self.root_dir):
            mkdir(self.root_dir)
        self.state = PersistentState()
        self.state.set_filepath(self.root_dir)
        self.silos = [x for x in listdir(self.root_dir) if self.issilo(x)]
        
    def issilo(self, silo_name):
        return path.isdir(path.join(self.root_dir, silo_name)) and path.exists(path.join(self.root_dir, silo_name, 'pairtree_root'))
    
    def describe_silo(self, silo_name, **kw):
        if kw:
            if self.issilo(silo_name):
                if not self.state.has_key(silo_name):
                    self.state[silo_name] = {}
                self.state[silo_name].update(kw)
            else:
                raise SiloNotFound
        else:
            if self.state.has_key(silo_name):
                return self.state[silo_name]
            else:
                return {}
    
    def sync(self):
        self.state.sync()
    
    def get_silo(self, silo_name, uri_base=None, **kw):
        return Silo(path.join(self.root_dir, silo_name), uri_base=uri_base, **kw)
        
    def get_rdf_silo(self, silo_name, uri_base=None, **kw):
        return RDFSilo(path.join(self.root_dir, silo_name), uri_base=uri_base, **kw)
