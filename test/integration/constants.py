import os
from dotenv import load_dotenv

load_dotenv()

N10S_PROC_DB = "neo4j"
RDFLIB_DB = "rdflib"
LOCAL = (os.getenv("RUN_TEST_LOCALLY", "False").lower() == "true")

N10S_CONSTRAINT_QUERY = "CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS FOR (r:Resource) REQUIRE r.uri IS UNIQUE"
GET_DATA_QUERY = "MATCH (n:Resource) RETURN n.uri as uri, labels(n) as labels, properties(n) as props ORDER BY uri"
GET_NODES_PROPS_QUERY = "MATCH (n:Resource) RETURN n.uri as uri, labels(n) as labels, properties(n) as props ORDER BY uri"
GET_RELS_QUERY = "MATCH (n:Resource)-[r]->(n2:Resource) RETURN  n.uri as nuri, n2.uri as n2uri,type(r) as type ORDER by nuri,n2uri"
