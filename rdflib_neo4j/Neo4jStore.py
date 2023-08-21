from typing import Dict

from rdflib.store import Store
from neo4j import GraphDatabase
from neo4j import WRITE_ACCESS
import logging

from rdflib_neo4j.Neo4jTriple import Neo4jTriple
from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig
from rdflib_neo4j.config.const import NEO4J_DRIVER_USER_AGENT_NAME
from rdflib_neo4j.query_composers.NodeQueryComposer import NodeQueryComposer
from rdflib_neo4j.query_composers.RelationshipQueryComposer import RelationshipQueryComposer

__all__ = ["Neo4jStore"]


class Neo4jStore(Store):

    def __init__(self, config: Neo4jStoreConfig):
        self.__open = False
        self.driver = None
        self.session = None
        self.config = config
        super(Neo4jStore, self).__init__(config.get_config_dict())

        self.batching = config.batching
        self.buffer_max_size = config.batch_size

        self.total_triples = 0
        self.node_buffer_size = 0
        self.rel_buffer_size = 0
        self.node_buffer: Dict[str, NodeQueryComposer] = {}
        self.rel_buffer: Dict[str, RelationshipQueryComposer] = {}
        self.current_subject: Neo4jTriple | None = None
        self.mappings = config.custom_mappings
        self.statistics = {"node_count": 0, "rel_count": 0}
        self.handle_vocab_uri_strategy = config.handle_vocab_uri_strategy
        self.handle_multival_strategy = config.handle_multival_strategy
        self.multival_props_predicates = config.multival_props_names

    def create_session(self):
        """
        Creates the Neo4j session and driver.

        This function initializes the driver and session based on the provided configuration.

        """
        auth_data = self.config.auth_data
        self.driver = GraphDatabase.driver(
            auth_data['uri'],
            auth=(auth_data['user'], auth_data['pwd']),
            user_agent=NEO4J_DRIVER_USER_AGENT_NAME
        )
        self.session = self.driver.session(
            database=auth_data.get('database', 'neo4j'),
            default_access_mode=WRITE_ACCESS
        )

    def constraint_check(self, create):
        """
        Checks the existence of a uniqueness constraint on the `Resource` node with the `uri` property.

        Args:
            create (bool): Flag indicating whether to create the constraint if not found.

        """
        # Test connectivity to backend and check that constraint on :Resource(uri) is present
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
                # Create the uniqueness constraint
                create_constraint = """
                CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS FOR (r:Resource) REQUIRE r.uri IS UNIQUE
                """
                self.session.run(create_constraint)
                print("Uniqueness constraint on :Resource(uri) is created.")
            except Exception as e:
                print("Error: Unable to create the uniqueness constraint. Make sure you have the necessary privileges.")
                print("Exception: ", e)
        else:
            print(f"""Uniqueness constraint on :Resource(uri) {"" if constraint_found else "not "}found. 
            {"" if constraint_found else "Run the following command on the Neo4j DB to create the constraint: "
                                         "CREATE CONSTRAINT n10s_unique_uri FOR (r:Resource) REQUIRE r.uri IS UNIQUE. Or provide create=True to create it."} 
             """)

    def open(self, db_config, create=True):
        """
        Opens a connection to the Neo4j database.

        Args:
            db_config: The configuration for the Neo4j database. (Not used, just kept for the method declaration in the Store class)
            create (bool): Flag indicating whether to create the uniqueness constraint if not found.

        """
        self.create_session()
        self.constraint_check(create)
        self.set_open(True)

    def set_open(self, val: bool):
        """
        Sets the 'open' status of the store.

        Args:
            val (bool): The value to set for the 'open' status.
        """
        self.__open = val

    def is_open(self):
        """
        Checks if the store is open.

        Returns:
            bool: True if the store is open, False otherwise.
        """
        return self.__open

    def close(self, commit_pending_transaction=True):
        """
        Closes the store.

        Args:
            commit_pending_transaction (bool): Flag indicating whether to commit any pending transaction before closing.
        """
        if commit_pending_transaction:
            if self.node_buffer_size > 0:
                self.commit(only_nodes=True)
            if self.rel_buffer_size > 0:
                self.commit(only_rels=True)
        self.session.close()
        self.driver.close()
        self.set_open(False)
        print(f"IMPORTED {self.total_triples} TRIPLES")
        print(f"TOTAL FLUSHED: NODES: {self.statistics['node_count']} RELATIONSHIPS: {self.statistics['rel_count']}")

    def store_current_subject_props(self):
        """
        Stores the properties of the current subject in the respective node buffer.

        This function adds the properties of the current subject to the node buffer for later insertion into the Neo4j database.
        """
        label_key = self.current_subject.extract_label_key()
        if label_key not in self.node_buffer:
            self.node_buffer[label_key] = NodeQueryComposer(labels=self.current_subject.labels,
                                                            handle_multival_strategy=self.handle_multival_strategy,
                                                            multival_props_predicates=self.multival_props_predicates)

        self.node_buffer[label_key].add_props(self.current_subject.extract_props_names())
        self.node_buffer[label_key].add_props(self.current_subject.extract_props_names(multi=True), multi=True)
        query_params = self.current_subject.extract_params()
        self.node_buffer[label_key].add_query_param(query_params)
        self.node_buffer_size += 1

    def store_current_subject_rels(self):
        """
        Stores the relationships of the current subject in the respective relationship buffer.

        This function adds the relationships of the current subject to the relationship buffer for later insertion into the Neo4j database.
        """
        rel_types_and_relationships = self.current_subject.extract_rels()
        if self.current_subject.extract_rels():
            for rel_type in rel_types_and_relationships:
                if rel_type not in self.rel_buffer:
                    self.rel_buffer[rel_type] = RelationshipQueryComposer(rel_type)
                for to_node in rel_types_and_relationships[rel_type]:
                    self.rel_buffer[rel_type].add_query_param(from_node=self.current_subject.uri, to_node=to_node)
                    self.rel_buffer_size += 1

    def store_current_subject(self):
        """
        Stores the current subject in the respective buffers.

        This function stores the current subject's properties and relationships in the respective buffers.
        """
        self.store_current_subject_props()
        self.store_current_subject_rels()

    def create_current_subject(self, subject):
        return Neo4jTriple(uri=subject,
                           prefixes={value: key for key, value in self.config.get_prefixes().items()},
                           # Reversing the Prefix dictionary
                           handle_vocab_uri_strategy=self.handle_vocab_uri_strategy,
                           handle_multival_strategy=self.handle_multival_strategy,
                           multival_props_names=self.multival_props_predicates)

    def check_current_subject(self, subject):
        """
        Checks the current subject and stores the previous subject if it has changed.

        This function checks if the provided subject is the same as the current subject.
        If the current subject is different, it stores the properties and relationships of the previous subject.

        Args:
            subject: The subject to check.
        """
        if self.current_subject is None:
            self.current_subject = self.create_current_subject(subject)
        else:
            if self.current_subject.uri != subject:
                self.store_current_subject()
                self.current_subject = self.create_current_subject(subject)

    def add(self, triple, context=None, quoted=False):
        """
        Adds a triple to the Neo4j store.

        Args:
            triple: The triple to add.
            context: The context of the triple (default: None).
            quoted (bool): Flag indicating whether the triple is quoted (default: False).
        """
        assert self.is_open(), "The Store must be open."
        assert context != self, "Can not add triple directly to store"

        # Unpacking the triple
        (subject, predicate, object) = triple

        self.check_current_subject(subject=subject)
        self.current_subject.parse_triple(triple=triple, mappings=self.mappings)
        self.total_triples += 1

        # If batching, we push whenever the buffers are filled with enough data
        if self.batching:
            if self.node_buffer_size >= self.buffer_max_size:
                self.commit(only_nodes=True)
            if self.rel_buffer_size >= self.buffer_max_size:
                self.commit(only_rels=True)
        else:
            self.commit()

    def remove(self, triple, context=None, txn=None):
        return "this is a streamer no state, no triple removal"

    def __len__(self, context=None):
        # no triple state, just a streamer
        return 0

    def commit(self, only_nodes=False, only_rels=False):
        """
        Commits the changes to the Neo4j database.

        Args:
            only_nodes (bool): Flag indicating whether to commit only nodes.
            only_rels (bool): Flag indicating whether to commit only relationships.
        """
        # To prevent edge cases for the last declaration in the file.
        if self.current_subject:
            self.store_current_subject()
            self.current_subject = None
        self.__flushBuffer(only_nodes, only_rels)

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
                self.query_database(query=query, params=params)
                self.statistics["node_count"] += len(cur.query_params)
                cur.empty_query_params()
        self.node_buffer_size = 0

    def __flushRelBuffer(self):
        """
        Flushes the relationship buffer by committing the changes to the Neo4j database.
        """
        for key in self.rel_buffer:
            cur = self.rel_buffer[key]
            if not cur.is_redundant():
                self.query_database(cur.write_query(), cur.query_params)
                self.statistics["rel_count"] += len(cur.query_params)
                cur.empty_query_params()
        self.rel_buffer_size = 0

    def query_database(self, query, params):
        """
        Executes a Cypher query on the Neo4j database.

        Args:
            query (str): The Cypher query to execute.
            params: The parameters to pass to the query.
        """
        try:
            self.session.run(query, params=params)
        except Exception as e:
            print(e)
            logging.error(e)
            raise e
