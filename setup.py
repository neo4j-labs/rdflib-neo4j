#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Setup script for rdflib-neo4j
"""
import os
import sys

if os.path.exists('MANIFEST'):
    os.remove('MANIFEST')

from setuptools import setup

if sys.argv[-1] == 'setup.py':
    print("To install, run 'python setup.py install'")
    print()

if sys.version_info[:2] < (3, 6):
    print("NetworkX Neo4j requires Python 3.6 or later (%d.%d detected)." %
          sys.version_info[:2])
    sys.exit(-1)

if __name__ == "__main__":
    setup(
        name="rdflib-neo4j",
        version="0.0.1b1",
        author="Jesús Barrasa",
        author_email="jbarrasa@outlook.com",
        description="RDFLib Store backed by neo4j + n10s",
        keywords="neo4j, rdflib, neosemantics, n10s",
        long_description="RDFLib Store backed by neo4j + n10s",
        license="Apache 2",
        platforms="All",
        url="https://github.com/neo4j-labs/rdflib-neo4j",
        install_requires=[
            'rdflib >= 5.0.0','neo4j >= 4.1.0',
        ],
        packages=['rdflib_neo4j'],
        entry_points={
            'rdf.plugins.store': [
                'Neo4j = rdflib_neo4j.neo4j:N10sNeo4jStore',
            ],
        }, 
        zip_safe=False
    )

