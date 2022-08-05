from rdflib.store import Store
from rdflib import Literal, RDF
from neo4j import GraphDatabase
from neo4j import WRITE_ACCESS
import logging

__all__ = ["CypherNeo4jStore"]

class CypherNeo4jStore(Store):

    context_aware = False
    formula_aware = True
    transaction_aware = True
    graph_aware = True

    def __init__(self, config=None, identifier=None):
        self.config = config
        self.inbatch = False
        self.queryBuffer = {}
        self.paramBuffer = {}
        self.bufferMaxSize = 10000
        self.bufferActualSize = 0
        super(CypherNeo4jStore, self).__init__(config)
        self.__namespace = {}
        self.__prefix = {}
        self.__open = False


    def open(self, config, create=False):
        self.driver = GraphDatabase.driver(config['uri'], auth=(config['auth']['user'], config['auth']['pwd']))
        self.session = self.driver.session(database=config['database'], default_access_mode=WRITE_ACCESS)
        # tst connectivity to the backend
        result = self.session.run("return 1 as uno")
        storeReady = next((True for x in result if x["uno"] > 0), False)
        self.__open = True #storeReady

    def is_open(self):
        return self.__open

    def close(self, commit_pending_transaction=False):
        self.driver.close()
        self.__open = False

    def destroy(self, configruation):
        print("destroying the store.")

    def add(self, triple, context, quoted=False):
        assert self.__open, "The Store must be open."
        assert context != self, "Can not add triple directly to store"
        (subject, predicate, object) = triple

        self.bufferActualSize += 1

        if isinstance(object, Literal):
            # ignoring datatypes and lang tags for now
            lang = object.language or None
            datatype = object.datatype or None

            # if new predicate add new query
            if (shorten(predicate) not in self.paramBuffer.keys()):
                self.paramBuffer[shorten(predicate)] = [{"uri": subject, "val": object}]
                self.queryBuffer[shorten(predicate)] = "unwind $params as pair " \
                                                       "merge (x:Resource {{ uri:pair.uri }}) " \
                                                       "set x.`{propname}` = pair.val".format(
                    propname=shorten(predicate))
            else:
                self.paramBuffer[shorten(predicate)].append({"uri": subject, "val": object})

        elif (predicate == RDF.type):
            # add a prefix to indicate if the is used as a type
            if (shorten(object) not in self.paramBuffer.keys()):
                self.paramBuffer[shorten(object)] = [subject]
                self.queryBuffer[shorten(object)] = "unwind $params as uri " \
                                                       "merge (r:Resource {{ uri: uri }}) set r:`{type}`".format(
                    type=shorten(object))
            else:
                self.paramBuffer[shorten(object)].append(subject)

        else:
            #add a prefix to indicate if the pred is being used as a prop or as a rel
            if (shorten(predicate) not in self.paramBuffer.keys()):
                self.paramBuffer[shorten(predicate)] = [{"uri": subject, "val": object}]
                self.queryBuffer[shorten(predicate)] = "unwind $params as pair " \
                                                       "merge (from:Resource {{ uri:pair.uri }}) " \
                                                       "merge (to:Resource {{ uri:pair.val }}) " \
                                                       "merge (from)-[:`{propname}`]->(to) ".format(
                    propname=shorten(predicate))
            else:
                self.paramBuffer[shorten(predicate)].append({"uri": subject, "val": object})

        if self.inbatch:
            if self.bufferActualSize>= self.bufferMaxSize:
               self.__flushBuffer()
        else:
            self.__flushBuffer()

    def remove(self, triple, context, txn=None):
        return "this is a streamer no state, no triple removal"


    def __len__(self, context=None):
        # no triple state, jsut a streamer
        return 0

    def __flushBuffer(self):
        assert self.__open, "The Store must be open."

        for key in self.queryBuffer.keys():
            self.session.run(self.queryBuffer[key], params = self.paramBuffer[key])

        self.bufferActualSize = 0

    def startBatchedWrite(self, bufferSize = 10000):
        assert self.__open, "The Store must be open."
        self.inbatch = True
        self.bufferMaxSize = bufferSize
        logging.info("starting import. Batch size {bufferSize}".format(bufferSize=bufferSize))

    def endBatchedWrite(self):
        if self.inbatch:
            assert self.__open, "The Store must be open."
            if self.bufferActualSize >0:
                self.__flushBuffer()
            self.inbatch = False
            logging.info("batch import done")

def getLocalPart(uri):
    pos = -1
    pos = uri.rfind('#')
    if pos < 0:
        pos = uri.rfind('/')
    if pos < 0:
        pos = uri.rindex(':')
    return uri[pos + 1:]

def getNamespacePart(uri):
    pos = -1
    pos = uri.rfind('#')
    if pos < 0:
        pos = uri.rfind('/')
    if pos < 0:
        pos = uri.rindex(':')
    return uri[0:pos + 1]

def shorten(uri):
    return getLocalPart(str(uri))
