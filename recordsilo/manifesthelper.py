#import rdflib
from rdflib import ConjunctiveGraph
from rdflib import Namespace, URIRef, Literal, BNode

from xml.sax.xmlreader import InputSource
from StringIO import StringIO

from urihelper import URIHelper, NAMESPACES

class TextInputSource(InputSource, object):
    def __init__(self, text, system_id=None):
        super(TextInputSource, self).__init__(system_id)
        self.url = system_id
        file = StringIO(text)
        self.setByteStream(file)

    def __repr__(self):
        return self.url

class ManifestHelper(object):
    def __init__(self, uri=None):
        self.uri = None
        if uri:
            self.uri = uri
        self.reset()
    
    def reset(self):
        self.g = None
        if self.uri:
            self.g = ConjunctiveGraph(identifier=self.uri)
        else:
            self.g = ConjunctiveGraph()
        self.namespaces = {}
        self.urihelper = URIHelper(self.namespaces)
        #add defaults
        for prefix, ns in NAMESPACES.iteritems():
            self.add_namespace(prefix, ns)
    
    def from_string(self, text, format="xml", encoding="utf-8"):
        self.reset()
        t = TextInputSource(text, system_id=self.uri)
        t.setEncoding(encoding)
        self.g.parse(t, format)
        return
    
    def triple_exists(self, s, p, o):
        if not type(self.g).__name__ in ['ConjunctiveGraph', 'Graph']:
            return False        
        if s == '*':
            s = None
        if p == '*':
            p = None
        if o == '*':
            o = None

        if not isinstance(s, URIRef) and not isinstance(s, BNode) and not s == None:
            s = self.urihelper.get_uriref(s)
        
        if not isinstance(p, URIRef) and not p == None:
            p = self.urihelper.parse_uri(p)

        if not isinstance(o, URIRef) and not isinstance(o, Literal) and not isinstance(o, BNode) and not o == None:
            if not isinstance(o, basestring):
                o = unicode(o)
            o = self.urihelper.parse_uri(o, return_Literal_not_Exception=True)
             
        count = 0
        for ans_s, ans_p, ans_o in self.g.triples((s, p, o)):
            count += 1
        if count > 0:
            return True
        else:
            return False 
    
    def list_objects(self, s, p):
        objects = []
        if not type(self.g).__name__ in ['ConjunctiveGraph', 'Graph']:
            return objects
        if s == '*':
            s = None
        if p == '*':
            p = None

        if not isinstance(s, URIRef) and not isinstance(s, BNode) and not s == None:
            s = self.urihelper.get_uriref(s)
        
        if not isinstance(p, URIRef) and not p == None:
            p = self.urihelper.parse_uri(p)

        for o in self.g.objects(s, p):
            objects.append(o)
        return objects
    
    def add_triple(self, s, p, o):
        if not isinstance(s, URIRef) and not isinstance(s, BNode):
            s = self.urihelper.get_uriref(s)
        
        if not isinstance(p, URIRef):
            p = self.urihelper.parse_uri(p)

        if not isinstance(o, URIRef) and not isinstance(o, Literal) and not isinstance(o, BNode):
            if not isinstance(o, basestring):
                o = unicode(o)
            o = self.urihelper.parse_uri(o, return_Literal_not_Exception=True)

        self.g.add((s, p, o))
        self.g.commit()
        return
    
    def add_namespace(self, prefix, uri):
        if not isinstance (prefix, basestring):
            raise TypeError('Add namespace: prefix is not of type string or unicode') 

        if not isinstance(uri, (URIRef, Namespace)):
            if not isinstance(uri, basestring):
                raise TypeError('Add namespace: namespace is not of type string or unicode') 

        if not isinstance(prefix, unicode):
            prefix = unicode(prefix)

        if isinstance(uri, basestring) and not isinstance(uri, unicode):
            uri = unicode(uri)

        self.namespaces[prefix] = self.urihelper.get_namespace(uri)
        if prefix not in self.urihelper.namespaces:
            self.urihelper.namespaces[prefix] = self.urihelper.get_namespace(uri)
        self.g.bind(prefix, self.namespaces[prefix])
        return
    
    def del_namespace(self, prefix, ns):
        if prefix in self.namespaces:
            del self.namespaces[prefix]
        return
    
    def del_triple(self, s, p, o=None):
        if not type(self.g).__name__ in ['ConjunctiveGraph', 'Graph']:
            return
        if s == '*':
            s = None
        if p == '*':
            p = None
        if o == '*':
            o = None

        if not isinstance(s, URIRef) and not isinstance(s, BNode) and not s == None:
            s = self.urihelper.get_uriref(s)
        
        if not isinstance(p, URIRef) and not p == None:
            p = self.urihelper.parse_uri(p)

        if not isinstance(o, URIRef) and not isinstance(o, Literal) and not isinstance(o, BNode) and not o == None:
            if not isinstance(o, basestring):
                o = unicode(o)
            o = self.urihelper.parse_uri(o, return_Literal_not_Exception=True)
        self.g.remove((s, p, o))
        return
    
    def get_graph(self):
        return self.g
    
    def to_string(self, format="xml"):
        if type(self.g).__name__ in ['ConjunctiveGraph', 'Graph'] and len(self.g)>0:
            self.g.commit()
            ans_str = self.g.serialize(format=format, encoding="utf-8")+"\n"
            return ans_str
        else:
            return u'<?xml version="1.0" encoding="UTF-8"?>\n'

