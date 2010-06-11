
from __future__ import with_statement

from os import path, mkdir

from rdfobject.constructs import Manifest

import logging

logger = logging.getLogger("RDFManifest")
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)

logger.addHandler(ch)

class RDFManifest(Manifest):
    def __init__(self, filepath, format="xml", uri=None):
        super(RDFManifest, self).__init__(uri)
        self.filepath = filepath
        self.format = format
        if path.isfile(self.filepath):
            logger.debug(self.filepath + " exists - loading rdf")
            self.revert()

    def revert(self):
        with open(self.filepath, "r") as mfile:
            m_str = mfile.read().decode("utf-8")
            try:
                self.from_string(m_str, self.format)
            except Exception, e:
                # likely an empty or unparsable file
                logger.debug("RDFManifest was unable to read or parse file: %s in format: %s" % (self.filepath, self.format))
                logger.debug("This can happen if no triples have been added for this object.")
                logger.debug("Error: %s" % e)
                

    def sync(self):
        with open(self.filepath, "w") as mfile:
            m_str = self.to_string(self.format).encode("utf-8")
            mfile.write(m_str)

