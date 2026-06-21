"""Root conftest.py: ensure the local rdflib_neo4j package is on sys.path.

This is needed because the editable install may point to a different worktree.
"""
import sys
import os

# Insert the worktree root at the front of sys.path so that
# `import rdflib_neo4j` resolves to THIS worktree's package.
sys.path.insert(0, os.path.dirname(__file__))
