from rdflib.store import Store
from rdflib import Literal, RDF, URIRef
from neo4j import GraphDatabase
from neo4j import WRITE_ACCESS
import logging
from decimal import Decimal

__all__ = ["CypherNeo4jStore"]

class CypherNeo4jStore(Store):

    context_aware = True
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

        self.session = self.driver.session(database=config.get('database','neo4j'), default_access_mode=WRITE_ACCESS)

        # test connectivity to backend and check that constraint on :Resource(uri) is present
        constraint_check = """
        show constraints yield * 
        where type = "UNIQUENESS" 
            and entityType = "NODE" 
            and labelsOrTypes = ["Resource"] 
            and properties = ["uri"] 
        return count(*) = 1 as constraint_found
        """
        result = self.session.run(constraint_check)
        constraint_found = next((True for x in result if x["constraint_found"]), False)
        print("Uniqueness constraint on :Resource(uri) {yes_or_no}found. {suffix}"
              .format(yes_or_no = "" if constraint_found else "not ",
                      suffix = "" if constraint_found else "Run the following command on the Neo4j DB: "
                            "CREATE CONSTRAINT n10s_unique_uri FOR (r:Resource) REQUIRE r.uri IS UNIQUE"))
        self.__open = True

    def is_open(self):
        return self.__open

    def close(self, commit_pending_transaction=False):
        self.session.close()
        self.driver.close()
        self.__open = False

    def destroy(self, configruation):
        print("destroying the store.")

    def add(self, triple, context=None, quoted=False):
        assert self.__open, "The Store must be open."
        assert context != self, "Can not add triple directly to store"
        (subject, predicate, object) = triple

        self.bufferActualSize += 1

        if isinstance(object, Literal):
            # 'special' datatypes are converted to strings and lang tags are lost (for now)
            # also multivalued props are overwritten
            lang = object.language or None

            #python driver does not support decimal params
            value = float(object.toPython()) if type(object.toPython()) == Decimal else object.toPython()

            prop_key = "prop_" + shorten(predicate)
            if (prop_key not in self.paramBuffer.keys()):
                self.paramBuffer[prop_key] = [{"uri": subject, "val": value }]
                self.queryBuffer[prop_key] = "unwind $params as pair " \
                                                       "merge (x:Resource {{ uri:pair.uri }}) " \
                                                       "set x.`{propname}` = pair.val".format(
                    propname=shorten(predicate))
            else:
                self.paramBuffer[prop_key].append({"uri": subject, "val": value})

        elif (predicate == RDF.type):

            type_key = "type_" + shorten(object)
            if (type_key not in self.paramBuffer.keys()):
                self.paramBuffer[type_key] = [subject]
                self.queryBuffer[type_key] = "unwind $params as uri " \
                                                       "merge (r:Resource {{ uri: uri }}) set r:`{type}`".format(
                    type=shorten(object))
            else:
                self.paramBuffer[type_key].append(subject)

        else:
            rel_key = "rel_" + shorten(predicate)
            if (rel_key not in self.paramBuffer.keys()):
                self.paramBuffer[rel_key] = [{"uri": subject, "val": object}]
                self.queryBuffer[rel_key] = "unwind $params as pair " \
                                                       "merge (from:Resource {{ uri:pair.uri }}) " \
                                                       "merge (to:Resource {{ uri:pair.val }}) " \
                                                       "merge (from)-[:`{propname}`]->(to) ".format(
                    propname=shorten(predicate))
            else:
                self.paramBuffer[rel_key].append({"uri": subject, "val": object})

        if self.inbatch:
            if self.bufferActualSize>= self.bufferMaxSize:
               self.__flushBuffer()
        else:
            self.__flushBuffer()

    def remove(self, triple, context=None, txn=None):
        return "this is a streamer no state, no triple removal"


    def __len__(self, context=None):
        # no triple state, jsut a streamer
        return 0

    def __flushBuffer(self):
        assert self.__open, "The Store must be open."

        for key in self.queryBuffer.keys():
            try:
                self.session.run(self.queryBuffer[key], params = self.paramBuffer[key])
            except TypeError:
                print("query:",self.queryBuffer[key],"params:",self.paramBuffer[key])

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
