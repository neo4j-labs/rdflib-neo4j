from typing import Dict

from rdflib import Literal, RDF
from rdflib.store import Store
from rdflib.term import BNode
from neo4j import GraphDatabase, Driver
from neo4j import WRITE_ACCESS
import logging

from rdflib_neo4j.Neo4jTriple import Neo4jTriple
from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig
from rdflib_neo4j.config.const import NEO4J_DRIVER_USER_AGENT_NAME
from rdflib_neo4j.config.utils import check_auth_data
from rdflib_neo4j.query_composers.NodeQueryComposer import NodeQueryComposer
from rdflib_neo4j.query_composers.RelationshipQueryComposer import RelationshipQueryComposer
from rdflib_neo4j.utils import handle_neo4j_driver_exception


class Neo4jStore(Store):

    context_aware = True

    def __init__(self, config: Neo4jStoreConfig, neo4j_driver: Driver | None = None):
        self.__open = False
        self.driver = neo4j_driver
        self.session = None
        self.config = config

        # Check that either driver or credentials are provided
        if not neo4j_driver:
            check_auth_data(config.auth_data)
        elif config.auth_data:
            raise Exception("Either initialize the store with credentials or driver. You cannot do both.")

        # Set named_graphs before super().__init__() because rdflib's Store.__init__
        # calls self.open() which in turn calls __constraint_check which reads this attr.
        self.named_graphs = config.named_graphs

        super(Neo4jStore, self).__init__(config.get_config_dict())

        self.batching = config.batching
        self.buffer_max_size = config.batch_size

        self.total_triples = 0
        self.node_buffer_size = 0
        self.rel_buffer_size = 0
        self.node_buffer: Dict[str, NodeQueryComposer] = {}
        self.rel_buffer: Dict[str, RelationshipQueryComposer] = {}
        self.current_subject: Neo4jTriple = None
        self.mappings = config.custom_mappings
        self.handle_vocab_uri_strategy = config.handle_vocab_uri_strategy
        self.handle_multival_strategy = config.handle_multival_strategy
        self.multival_props_predicates = config.multival_props_names

    def open(self, configuration, create=True):
        """
        Opens a connection to the Neo4j database.

        Args:
            configuration: The configuration for the Neo4j database. (Not used, just kept for the method declaration in the Store class)
            create (bool): Flag indicating whether to create the uniqueness constraint if not found.

        """
        self.__create_session()
        self.__constraint_check(create)
        self.__set_open(True)

    def close(self, commit_pending_transaction=True):
        """
        Closes the store.

        Args:
            commit_pending_transaction (bool): Flag indicating whether to commit any pending transaction before closing.
        """
        if commit_pending_transaction:
            self.commit(commit_nodes=True)
            self.commit(commit_rels=True)
        self.session.close()
        self.__set_open(False)
        print(f"IMPORTED {self.total_triples} TRIPLES")
        self.total_triples=0

    def is_open(self):
        """
        Checks if the store is open.

        Returns:
            bool: True if the store is open, False otherwise.
        """
        return self.__open

    def add(self, triple, context=None, quoted=False):
        """
        Adds a triple to the Neo4j store.

        Args:
            triple: The triple to add.
            context: The context of the triple (default: None).  When
                ``named_graphs=True`` in the store config, ``context`` is
                expected to be an rdflib ``Graph`` whose ``.identifier``
                attribute is a ``URIRef`` or ``BNode`` naming the graph.
            quoted (bool): Flag indicating whether the triple is quoted (default: False).
        """
        assert self.is_open(), "The Store must be open."
        assert context != self, "Can not add triple directly to store"

        # Resolve named-graph URI when the feature is enabled
        graph_uri = None
        if self.named_graphs and context is not None:
            identifier = getattr(context, "identifier", None)
            if identifier is not None:
                graph_uri = str(identifier)

        # Unpacking the triple
        (subject, predicate, object) = triple

        self.__check_current_subject(subject=subject, graph_uri=graph_uri)
        self.current_subject.parse_triple(triple=triple, mappings=self.mappings)
        self.total_triples += 1

        # If batching, we push whenever the buffers are filled with enough data
        try:
            if self.batching:
                if self.node_buffer_size >= self.buffer_max_size:
                    self.commit(commit_nodes=True)
                if self.rel_buffer_size >= self.buffer_max_size:
                    self.commit(commit_rels=True)
            else:
                self.commit()
        except Exception as e:
            print(f"Flushing all query params due to error: {e}")
            self.__close_on_error()
            raise e

    def commit(self, commit_nodes=False, commit_rels=False):
        """
        Commits the changes to the Neo4j database.

        Args:
            commit_nodes (bool): Flag indicating whether to commit the nodes in the buffer.
            commit_rels (bool): Flag indicating whether to commit the relationships in the buffer.
        """
        # To prevent edge cases for the last declaration in the file.
        if self.current_subject:
            self.__store_current_subject()
            self.current_subject = None
        self.__flushBuffer(commit_nodes, commit_rels)

    def remove(self, triple, context=None, txn=None):
        raise NotImplementedError("This is a streamer so it doesn't preserve the state, there is no removal feature.")

    def __close_on_error(self):
        """
        Empties the query buffers in case of an error.

        This method empties the query parameters in the node and relationship buffers.
        """
        for node_buffer in self.node_buffer.values():
            node_buffer.empty_query_params()
        for rel_buffer in self.rel_buffer.values():
            rel_buffer.empty_query_params()

    def __set_open(self, val: bool):
        """
        Sets the 'open' status of the store.

        Args:
            val (bool): The value to set for the 'open' status.
        """
        self.__open = val
        print(f"The store is now: {'Open' if self.__open else 'Closed'}")

    def __get_driver(self) -> Driver:
        if not self.driver:
            auth_data = self.config.auth_data
            self.driver = GraphDatabase.driver(
                auth_data['uri'],
                auth=(auth_data['user'], auth_data['pwd']),
                database=auth_data.get('database', 'neo4j'),
                user_agent=NEO4J_DRIVER_USER_AGENT_NAME
            )
        return self.driver

    def __create_session(self):
        """
        Creates the Neo4j session and driver.

        This function initializes the driver and session based on the provided configuration.

        """
        self.session = self.__get_driver().session(
            default_access_mode=WRITE_ACCESS
        )

    def __constraint_check(self, create):
        """
        Checks (and optionally creates) the required schema object on :Resource.

        * **Default mode** (``named_graphs=False``): requires a uniqueness constraint
          on ``:Resource(uri)`` — the same one used by n10s triple import.

        * **Named-graph mode** (``named_graphs=True``): requires only a plain index on
          ``:Resource(uri)`` because the same URI can exist in multiple graphs as
          separate nodes.  A uniqueness constraint would reject duplicate URIs across
          graphs, so it must not be present.

        Args:
            create (bool): Flag indicating whether to create the missing schema object.
        """
        if self.named_graphs:
            self.__index_check(create)
        else:
            self.__uniqueness_constraint_check(create)

    def __uniqueness_constraint_check(self, create):
        """Check / create the uniqueness constraint on :Resource(uri)."""
        constraint_check = """
           SHOW CONSTRAINTS YIELD *
           WHERE type = "UNIQUENESS"
               AND entityType = "NODE"
               AND labelsOrTypes = ["Resource"]
               AND properties = ["uri"]
           RETURN COUNT(*) = 1 AS constraint_found
           """
        result = self.session.run(constraint_check)
        constraint_found = next((True for x in result if x["constraint_found"]), False)

        if not constraint_found and create:
            try:
                create_constraint = """
                   CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS FOR (r:Resource) REQUIRE r.uri IS UNIQUE
                   """
                self.session.run(create_constraint)
                print("Uniqueness constraint on :Resource(uri) is created.")
            except Exception as e:
                print("Error: Unable to create the uniqueness constraint. Make sure you have the necessary privileges.")
                print("Exception: ", e)
        else:
            print(f"""Uniqueness constraint on :Resource(uri) {"" if constraint_found else "not "}found. \
{"" if constraint_found else "Run: CREATE CONSTRAINT n10s_unique_uri FOR (r:Resource) REQUIRE r.uri IS UNIQUE, or pass create=True."}\
""")

    def __index_check(self, create):
        """Check / create a plain index on :Resource(uri) (required for named-graph mode)."""
        index_check = """
           SHOW INDEXES YIELD *
           WHERE type = "RANGE"
               AND entityType = "NODE"
               AND labelsOrTypes = ["Resource"]
               AND properties = ["uri"]
           RETURN COUNT(*) >= 1 AS index_found
           """
        result = self.session.run(index_check)
        index_found = next((True for x in result if x["index_found"]), False)

        if not index_found and create:
            try:
                self.session.run(
                    "CREATE INDEX n10s_resource_uri IF NOT EXISTS FOR (r:Resource) ON (r.uri)"
                )
                print("Index on :Resource(uri) is created (named-graph mode).")
            except Exception as e:
                print("Error: Unable to create the index on :Resource(uri).")
                print("Exception: ", e)
        else:
            print(
                f"Index on :Resource(uri) {'found' if index_found else 'not found'} "
                f"(named-graph mode)."
                + ("" if index_found else " Run: CREATE INDEX n10s_resource_uri FOR (r:Resource) ON (r.uri), or pass create=True.")
            )

    def __store_current_subject_props(self):
        """
        Stores the properties of the current subject in the respective node buffer.

        This function adds the properties of the current subject to the node buffer for later insertion into the Neo4j database.
        """
        label_key = self.current_subject.extract_label_key()
        # When named_graphs is active, each (labels, graphUri) combination needs its
        # own NodeQueryComposer so that the MERGE key is consistent within a batch.
        if self.named_graphs and self.current_subject.graph_uri is not None:
            label_key = f"{label_key}|{self.current_subject.graph_uri}"
        if label_key not in self.node_buffer:
            self.node_buffer[label_key] = NodeQueryComposer(
                labels=self.current_subject.labels,
                handle_multival_strategy=self.handle_multival_strategy,
                multival_props_predicates=self.multival_props_predicates,
                graph_uri_aware=self.named_graphs,
            )

        self.node_buffer[label_key].add_props(self.current_subject.extract_props_names())
        self.node_buffer[label_key].add_props(self.current_subject.extract_props_names(multi=True), multi=True)
        query_params = self.current_subject.extract_params()
        self.node_buffer[label_key].add_query_param(query_params)
        self.node_buffer_size += 1

    def __store_current_subject_rels(self):
        """
        Stores the relationships of the current subject in the respective relationship buffer.

        This function adds the relationships of the current subject to the relationship buffer for later insertion into the Neo4j database.
        """
        rel_types_and_relationships = self.current_subject.extract_rels()
        if self.current_subject.extract_rels():
            graph_uri = self.current_subject.graph_uri if self.named_graphs else None
            for rel_type in rel_types_and_relationships:
                if rel_type not in self.rel_buffer:
                    self.rel_buffer[rel_type] = RelationshipQueryComposer(
                        rel_type,
                        graph_uri_aware=self.named_graphs,
                    )
                for to_node in rel_types_and_relationships[rel_type]:
                    self.rel_buffer[rel_type].add_query_param(
                        from_node=self.current_subject.uri,
                        to_node=to_node,
                        graph_uri=graph_uri,
                    )
                    self.rel_buffer_size += 1

    def __store_current_subject(self):
        """
        Stores the current subject in the respective buffers.

        This function stores the current subject's properties and relationships in the respective buffers.
        """
        self.__store_current_subject_props()
        self.__store_current_subject_rels()

    def __create_current_subject(self, subject, graph_uri=None):
        return Neo4jTriple(
            uri=subject,
            prefixes={value: key for key, value in self.config.get_prefixes().items()},
            # Reversing the Prefix dictionary
            handle_vocab_uri_strategy=self.handle_vocab_uri_strategy,
            handle_multival_strategy=self.handle_multival_strategy,
            multival_props_names=self.multival_props_predicates,
            keep_lang_tag=self.config.keep_lang_tag,
            keep_custom_data_types=self.config.keep_custom_data_types,
            language_filter=self.config.language_filter,
            graph_uri=graph_uri,
        )

    def __check_current_subject(self, subject, graph_uri=None):
        """
        Checks the current subject and stores the previous subject if it has changed.

        This function checks if the provided subject is the same as the current subject.
        If the current subject is different, it stores the properties and relationships of the previous subject.

        Args:
            subject: The subject to check.
            graph_uri: Optional named-graph URI string for the current triple.
        """
        if self.current_subject is None:
            self.current_subject = self.__create_current_subject(subject, graph_uri)
        else:
            # A new subject or a new graph context forces a flush of the previous subject
            if self.current_subject.uri != subject or self.current_subject.graph_uri != graph_uri:
                self.__store_current_subject()
                self.current_subject = self.__create_current_subject(subject, graph_uri)

    def triples(self, triple, context=None):
        """
        Yield triples matching the pattern. Any component may be None (wildcard).

        Args:
            triple: A (subject, predicate, object) tuple; any element may be None.
            context: Optional graph context.  When ``named_graphs=True`` in the store
                config, passing a graph object here restricts results to triples that
                were stored under the named graph identified by
                ``context.identifier``.

        Yields:
            ((subject, predicate, object), self) tuples, matching the rdflib
            Store.triples() contract.
        """
        assert self.is_open(), "The Store must be open."
        from rdflib_neo4j.query_composers.ExportQueryComposer import ExportQueryComposer
        from rdflib_neo4j.expander import expand_uri, neo4j_value_to_literal

        (s, p, o) = triple

        # Resolve named-graph filter when the feature is enabled
        graph_uri = None
        if self.named_graphs and context is not None:
            identifier = getattr(context, "identifier", None)
            if identifier is not None:
                graph_uri = str(identifier)

        prefix_map = {name: str(ns) for name, ns in self.config.get_prefixes().items()}

        # Yield property and label triples from a node record
        def _node_triples(record):
            subject_uri = record["uri"]
            subject = expand_uri(subject_uri, prefix_map)
            # Subject filter
            if s is not None and subject != s:
                return
            props = record["props"]
            labels = record["extra_labels"]

            # Property triples (skip internal Neo4j bookkeeping keys)
            for prop_key, prop_val in props.items():
                if prop_key in ("uri", "graphUri"):
                    continue
                predicate = expand_uri(prop_key, prefix_map)
                if p is not None and predicate != p:
                    continue
                if isinstance(prop_val, list):
                    for v in prop_val:
                        lit = neo4j_value_to_literal(v, prop_key)
                        if o is None or o == lit:
                            yield (subject, predicate, lit), self
                else:
                    lit = neo4j_value_to_literal(prop_val, prop_key)
                    if o is None or o == lit:
                        yield (subject, predicate, lit), self

            # Label → rdf:type triples
            if p is None or p == RDF.type:
                for label in labels:
                    class_uri = expand_uri(label, prefix_map)
                    if o is None or o == class_uri:
                        yield (subject, RDF.type, class_uri), self

        # Yield a relationship triple from a relationship record
        def _rel_triples(record):
            from_uri = expand_uri(record["from_uri"], prefix_map)
            to_uri = expand_uri(record["to_uri"], prefix_map)
            rel_type = expand_uri(record["rel_type"], prefix_map)
            if s is not None and from_uri != s:
                return
            if p is not None and rel_type != p:
                return
            if o is not None and to_uri != o:
                return
            yield (from_uri, rel_type, to_uri), self

        if s is not None:
            # Subject known — query specific node
            s_uri = f"bnode://{s}" if isinstance(s, BNode) else str(s)
            run_kwargs = {"uri": s_uri}
            if graph_uri is not None:
                run_kwargs["graphUri"] = graph_uri
            node_result = self.session.run(
                ExportQueryComposer.node_by_uri_query(graph_uri=graph_uri),
                **run_kwargs,
            )
            for record in node_result:
                yield from _node_triples(record)
            # Only fetch relationships when predicate is not rdf:type and object
            # is not a Literal (relationships link Resource nodes only)
            if p != RDF.type and not isinstance(o, Literal):
                rel_kwargs = {"uri": s_uri}
                if graph_uri is not None:
                    rel_kwargs["graphUri"] = graph_uri
                rel_result = self.session.run(
                    ExportQueryComposer.relationships_from_uri_query(graph_uri=graph_uri),
                    **rel_kwargs,
                )
                for record in rel_result:
                    yield from _rel_triples(record)
        else:
            # Subject wildcard — scan all nodes
            node_kwargs = {}
            if graph_uri is not None:
                node_kwargs["graphUri"] = graph_uri
            node_result = self.session.run(
                ExportQueryComposer.all_nodes_query(graph_uri=graph_uri),
                **node_kwargs,
            )
            for record in node_result:
                yield from _node_triples(record)
            # Fetch relationships unless we know only Literal objects are wanted
            if not isinstance(o, Literal):
                rel_kwargs = {}
                if graph_uri is not None:
                    rel_kwargs["graphUri"] = graph_uri
                rel_result = self.session.run(
                    ExportQueryComposer.all_relationships_query(graph_uri=graph_uri),
                    **rel_kwargs,
                )
                for record in rel_result:
                    yield from _rel_triples(record)

    def __len__(self, context=None):
        """
        Return an approximate triple count.

        Sums: (number of non-uri properties per node) + (number of non-Resource
        labels per node) + (total relationship count).  This matches the rdflib
        Store contract better than a raw node count, though it will be exact only
        when no multi-value properties are stored as arrays.
        """
        if not self.is_open():
            return 0
        from rdflib_neo4j.query_composers.ExportQueryComposer import ExportQueryComposer
        result = self.session.run(ExportQueryComposer.count_query())
        record = result.single()
        return int(record["cnt"]) if record else 0

    def __iter__(self):
        """Iterate over all triples in the store."""
        return (triple for triple, _ in self.triples((None, None, None)))

    def __flushBuffer(self, only_nodes, only_rels):
        """
        Flushes the buffer by committing the changes to the Neo4j database.

        Args:
            only_nodes (bool): Flag indicating whether to flush only nodes.
            only_rels (bool): Flag indicating whether to flush only relationships.
        """
        assert self.is_open(), "The Store must be open."
        if not only_rels:
            self.__flushNodeBuffer()
        if not only_nodes:
            self.__flushRelBuffer()

    def __flushNodeBuffer(self):
        """
        Flushes the node buffer by committing the changes to the Neo4j database.
        """
        for key in self.node_buffer:
            cur = self.node_buffer[key]
            if not cur.is_redundant():
                query = cur.write_query()
                params = cur.query_params
                self.__query_database(query=query, params=params)
                cur.empty_query_params()
        self.node_buffer_size = 0

    def __flushRelBuffer(self):
        """
        Flushes the relationship buffer by committing the changes to the Neo4j database.
        """
        for key in self.rel_buffer:
            cur = self.rel_buffer[key]
            if not cur.is_redundant():
                query = cur.write_query()
                params = cur.query_params
                self.__query_database(query=query, params=params)
                cur.empty_query_params()
        self.rel_buffer_size = 0

    def __query_database(self, query, params):
        """
        Executes a Cypher query on the Neo4j database.

        Args:
            query (str): The Cypher query to execute.
            params: The parameters to pass to the query.
        """
        try:
            self.session.run(query, params=params)
        except Exception as e:
            e = handle_neo4j_driver_exception(e)
            logging.error(e)
            raise e
