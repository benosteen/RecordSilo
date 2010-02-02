from os import path, mkdir

import simplejson

import logging

logger = logging.getLogger("PersistentState")
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)

logger.addHandler(ch)

PERSISTENCE_FILENAME="persisted_state.json"

class PersistentState(dict):
    """Base class for the serialisation of the state of the harvest. Stores itself as JSON at the filepath given in the init phase."""
    def __init__(self, filepath=None, filename=PERSISTENCE_FILENAME, create = True):
        self.state = {}
        self.filepath = None
        if filepath:
            self.set_filepath(filepath, filename, create)
        self.revert()
    
    def set_filepath(self, filepath, filename=PERSISTENCE_FILENAME, create = True):
        if path.isdir(filepath):
            logger.debug("Filepath exists - setting persistence file to %s" % path.join(filepath, filename))
            self.filepath = path.join(filepath, filename)
            if create and not path.isfile(self.filepath):
                self.sync()
            return True
        else:
            logger.info("Filepath does not exist - persistence file would not be able to be created")
            return False
    
    def revert(self):
        """Revert the state to the version stored on disc."""
        if self.filepath:
            if path.isfile(self.filepath):
                with open(self.filepath, "r") as serialised_file:
                    try:
                        self.state = simplejson.load(serialised_file)
                    except ValueError:
                        logger.info("No JSON information could be read from the persistence file - could be empty: %s" % self.filepath)
                        self.state = {}
            else:
                logger.debug("The persistence file has not yet been created or does not exist, so the state cannot be read from it yet.")
        else:
            logger.debug("Filepath to the persistence file is not set. State cannot be read.")
            return False
    
    def sync(self):
        """Synchronise and update the stored state to the in-memory state."""
        if self.filepath:
            with open(self.filepath, "w") as serialised_file:
                simplejson.dump(self.state, serialised_file)
        else:
            logger.info("Filepath to the persistence file is not set. State cannot be synced to disc.")

    # Dictionary methods
    def keys(self): return self.state.keys()
    def has_key(self, key): return self.state.has_key(key)
    def items(self): return self.state.items()
    def values(self): return self.state.values()
    def clear(self): self.state.clear()
    def update(self, kw):
        for key in kw:
            self.state[key] = kw[key]
    def __setitem__(self, key, item): self.state[key] = item
    def __getitem__(self, key):
        try:
            return self.state[key]
        except KeyError:
            raise KeyError(key)
    def __repr__(self): return repr(self.state)
    def __cmp__(self, dict):
        if isinstance(dict, PersistentState):
            return cmp(self.state, dict.state)
        else:
            return cmp(self.state, dict)
    def __len__(self): return len(self.state)
    def __delitem__(self, key): del self.state[key]

