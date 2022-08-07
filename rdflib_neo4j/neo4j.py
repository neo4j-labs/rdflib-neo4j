from rdflib.store import Store
from rdflib import URIRef, Literal, BNode
from neo4j import GraphDatabase
from neo4j import WRITE_ACCESS

__all__ = ["N10sNeo4jStore"]

class N10sNeo4jStore(Store):

    context_aware = False
    formula_aware = True
    transaction_aware = True
    graph_aware = True
    __TRIPLE_COUNT_QUERY = "call { match (n:Resource) return sum(size(keys(n)) - 1) + sum(size(labels(n)) - 1) as ct " \
                         "union match (:Resource)-[r]->() return count(r) as ct } return sum(ct) as tripleCount"

    def __init__(self, config=None, identifier=None):
        self.config = config
        self.inbatch = False
        self.tripleBuffer = []
        self.bufferMaxSize = 10000
        super(N10sNeo4jStore, self).__init__(config)
        self.__namespace = {}
        self.__prefix = {}
        self.__open = False


    def open(self, config, create=False):
        self.driver = GraphDatabase.driver(config['uri'], auth=(config['auth']['user'], config['auth']['pwd']))
        self.session = self.driver.session(database=config.get('database','neo4j'), default_access_mode=WRITE_ACCESS)
        result = self.session.run("call n10s.graphconfig.show() yield param, value return count(*) as params")
        storeReady = next((True for x in result if x["params"] > 0), False)
        print('store ready:' + str(storeReady))
        self.__open = storeReady

        #if read access only... no need to check the GraphConfig is present

    def is_open(self):
        return self.__open

    def close(self, commit_pending_transaction=False):
        self.driver.close()
        self.__open = False

    def destroy(self, configruation):
        print("destroying the store")

    def __serialise(self, triple):
        (subject, predicate, object) = triple
        subject = "bnode://" + subject if isinstance(subject, BNode) else subject
        object = "bnode://" + object if isinstance(object, BNode) else object
        if isinstance(object, Literal):
            lang = object.language or None
            datatype = object.datatype or None
            if (lang):
                serialisedTriple = "<{s}> <{p}> \"{o}\"@{l} .".format(s=subject, p=predicate, o=object, l=lang)
            else:
                serialisedTriple = "<{s}> <{p}> \"{o}\"{dtprefix}{dt}{dtsuffix} ."\
                    .format(s=subject, p=predicate, o=object, dtprefix="^^<" * bool(datatype),dt=datatype if datatype else "",dtsuffix=">" * bool(datatype))

        else:
            serialisedTriple = "<{s}> <{p}> <{o}> .".format(s=subject, p=predicate, o=object)

        return serialisedTriple

    def add(self, triple, context, quoted=False):
        assert self.__open, "The Store must be open."
        assert context != self, "Can not add triple directly to store"
        Store.add(self, triple, context, quoted)
        if self.inbatch:
            self.tripleBuffer.append(self.__serialise(triple))
            if len(self.tripleBuffer)>= self.bufferMaxSize:
                self.__flushBuffer()
        else:
            result = self.session.run("CALL n10s.rdf.import.inline($rdf,'N-Triples')", rdf=self.__serialise(triple)).single()
            if(result["terminationStatus"]) == "KO":
                raise Exception("Could not persist triple in Neo4j: ", result["extraInfo"])

        #self.refreshNamespaces()

    def remove(self, triple, context, txn=None):
        assert self.__open, "The Store must be open."
        Store.remove(self, triple, context)

        for result in self.triples(triple):
            (spo,ctx) = result
            result= self.session.run("CALL n10s.rdf.delete.inline($rdf,'N-Triples')", rdf=self.__serialise(spo)).single()
            if (result["terminationStatus"]) == "KO":
                raise Exception("Could not delete triple from Neo4j: ", result["extraInfo"])

    def refreshNamespaces(self):
        nsresults = self.session.run("call n10s.nsprefixes.list()")
        for x in nsresults:
            self.__namespace[x["prefix"]] = x["namespace"]
            self.__prefix[x["namespace"]] = x["prefix"]


    def triples(self, triple_pattern, context=None):
        assert self.__open, "The Store must be open."
        (subject, predicate, object) = triple_pattern
        if isinstance(object, Literal):
            lang = object.language or None
            datatype = object.datatype or None
            result = self.session.run("call n10s.rdf.export.spo($spat, $ppat, $opat, $lit, $dt, $lan) "
                             "yield subject, predicate, object, isLiteral, literalType",
                              spat = subject, ppat = predicate,  opat = object, lit = True, dt = datatype, lan = lang)
        else:
            result = self.session.run("call n10s.rdf.export.spo($spat, $ppat, $opat) "
                                      "yield subject, predicate, object, isLiteral, literalType",
                                      spat=subject, ppat=predicate, opat=object)

        for record in result:
            yield (URIRef(record["subject"]),URIRef(record["predicate"]),
                   Literal(record["object"], datatype=record["literalType"]) if record["isLiteral"] else URIRef(record["object"])), context #lang=,

    def query(self, query, initNs, initBindings, queryGraph, **kwargs):
        assert self.__open, "The Store must be open."
        result = self.session.run("call n10s.rdf.export.cypher($cypher, $params)"
                                  "yield subject, predicate, object, isLiteral, literalType",
                                  cypher=query, params=initBindings)
        for record in result:
            yield (URIRef(record["subject"]),URIRef(record["predicate"]),
                   Literal(record["object"], datatype=record["literalType"]) if record["isLiteral"] else URIRef(record["object"]))

    def add_graph(self, graph):
        self.session.run("CALL n10s.rdf.import.inline($therdf,'Turtle')",
                         therdf=graph.serialize(format="turtle").decode("utf-8"))

    def __len__(self, context=None):
        #this is fine for RDF imported data, but this should also work with LPG (look at GraphConfig)
        result = self.session.run(N10sNeo4jStore.__TRIPLE_COUNT_QUERY)
        return next((x["tripleCount"] for x in result), 0)

    def bind(self, prefix, namespace):
        assert self.__open, "The Store must be open."
        if prefix != '':
            nsresults  = self.session.run("call n10s.nsprefixes.add($pref,$ns)", pref = prefix, ns = namespace)
        else:
            nsresults = []

        for x in nsresults:
            self.__namespace[x["prefix"]] = x["namespace"]
            self.__prefix[x["namespace"]] = x["prefix"]


    def namespace(self, prefix):
        return self.__namespace.get(prefix, None)

    def prefix(self, namespace):
        return self.__prefix.get(namespace, None)

    def namespaces(self):
        for prefix, namespace in self.__namespace.items():
            yield prefix, namespace

    def __flushBuffer(self):
        assert self.__open, "The Store must be open."
        print("Flushing {bufferSize} buffered Triples to DB".format(bufferSize=len(self.tripleBuffer)))
        self.session.run("CALL n10s.rdf.import.inline($rdf,'N-Triples')", rdf='\n'.join(self.tripleBuffer))
        self.tripleBuffer = []

    def startBatchedWrite(self, bufferSize = 10000):
        assert self.__open, "The Store must be open."
        self.inbatch = True
        self.bufferMaxSize = bufferSize
        print("start batch process. Triples will be buffered and flushed in batches of {bufferSize}".format(bufferSize=bufferSize))

    def endBatch(self):
        if self.inbatch:
            assert self.__open, "The Store must be open."
            if len(self.tripleBuffer)>0:
                self.__flushBuffer()
            self.inbatch = False
            self.bufferMaxSize = 10000


