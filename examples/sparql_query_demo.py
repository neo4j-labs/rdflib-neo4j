"""SPARQL → Cypher demo app for rdflib-neo4j.

Run with:
    streamlit run examples/sparql_query_demo.py

Requires (in addition to rdflib-neo4j itself):
    pip install streamlit streamlit-ace "neo4j-viz[neo4j]"
"""

import textwrap

import streamlit as st
from streamlit_ace import st_ace

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="rdflib-neo4j · SPARQL Explorer",
    page_icon="🔗",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Global CSS ────────────────────────────────────────────────────────────────

st.markdown(
    """
<style>
/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: #0d1117 !important;
    border-right: 1px solid #30363d;
    min-width: 230px !important; max-width: 260px !important;
}
/* Force all sidebar text to be readable */
section[data-testid="stSidebar"] *:not(button):not(input):not(select) {
    color: #e6edf3 !important;
}
section[data-testid="stSidebar"] input,
section[data-testid="stSidebar"] select {
    background: #161b22 !important;
    color: #e6edf3 !important;
    border-color: #30363d !important;
}
section[data-testid="stSidebar"] label { font-size: 0.8rem !important; }

/* ── Hero banner in sidebar ── */
.sidebar-hero {
    background: linear-gradient(135deg, #003f8a, #0070f3 55%, #00c6a0);
    border-radius: 8px; padding: 12px 14px; margin-bottom: 12px;
}
.sidebar-hero-title {
    font-size: 1.15rem; font-weight: 800; color: #fff !important; line-height: 1.2;
}
.sidebar-hero-sub { font-size: 0.72rem; color: rgba(255,255,255,0.8) !important; margin-top: 2px; }

/* ── Section labels ── */
.section-label {
    font-size: 0.68rem; font-weight: 700; letter-spacing: 0.1em;
    text-transform: uppercase; color: #8b949e; margin-bottom: 4px;
}

/* ── Cypher code block ── */
.stCode pre {
    border-left: 3px solid #0070f3 !important;
    font-size: 0.82rem !important;
}

/* ── Parameter pills ── */
.param-pill {
    display: inline-block;
    background: #21262d; border: 1px solid #30363d; border-radius: 10px;
    padding: 1px 9px; font-size: 0.76rem; color: #c9d1d9;
    margin: 2px 2px; font-family: monospace;
}

/* ── Run button ── */
div[data-testid="stButton"] > button[kind="primary"] {
    background: linear-gradient(90deg, #0070f3, #00c6a0) !important;
    border: none !important; color: #fff !important;
    font-weight: 700 !important; border-radius: 6px !important;
    padding: 6px 22px !important;
}
div[data-testid="stButton"] > button[kind="primary"]:hover {
    box-shadow: 0 0 14px rgba(0,112,243,0.45) !important;
    filter: brightness(1.08);
}

/* ── Compact metrics ── */
div[data-testid="stMetric"] { padding: 4px 0 !important; }
div[data-testid="stMetricValue"] { font-size: 1.4rem !important; }
div[data-testid="stMetricLabel"] { font-size: 0.72rem !important; }

/* ── No extra padding on columns ── */
div[data-testid="column"] { padding-top: 0 !important; }

/* ── Expander tighter ── */
details summary { padding: 6px 0 !important; }
div[data-testid="stExpander"] { margin-top: 6px !important; }

/* ── Remove Streamlit default top padding ── */
.block-container { padding-top: 1rem !important; }

/* ── Fix selectbox dropdown z-index so it isn't clipped ── */
[data-baseweb="popover"] { z-index: 9999 !important; }

/* ── Fix vocab strategy selectbox: white text on dark background ── */
section[data-testid="stSidebar"] [data-baseweb="select"] > div,
section[data-testid="stSidebar"] [data-baseweb="select"] span,
section[data-testid="stSidebar"] [data-baseweb="select"] [class*="placeholder"] {
    background: #161b22 !important;
    color: #e6edf3 !important;
    border-color: #30363d !important;
}
section[data-testid="stSidebar"] [data-baseweb="select"] svg { fill: #e6edf3 !important; }
</style>
""",
    unsafe_allow_html=True,
)

# ── Example queries (defined before sidebar so selectbox can reference them) ──

EXAMPLES = {
    "All people & names": textwrap.dedent("""\
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT ?person ?name WHERE {
          ?person a foaf:Person ;
                  foaf:name ?name .
        }
        ORDER BY ?name"""),
    "Knows relationships": textwrap.dedent("""\
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT ?a ?rel ?b ?aName ?bName WHERE {
          ?a a foaf:Person .
          ?b a foaf:Person .
          ?a ?rel ?b .
          ?a foaf:name ?aName .
          ?b foaf:name ?bName .
        }"""),
    "OPTIONAL age": textwrap.dedent("""\
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT ?name ?age WHERE {
          ?p a foaf:Person ; foaf:name ?name .
          OPTIONAL { ?p foaf:age ?age }
        }
        ORDER BY ?name"""),
    "FILTER age ≥ 28": textwrap.dedent("""\
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT ?name ?age WHERE {
          ?p a foaf:Person ;
             foaf:name ?name ;
             foaf:age  ?age .
          FILTER(?age >= 28)
        }"""),
    "UNION two patterns": textwrap.dedent("""\
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT ?x WHERE {
          { ?x foaf:name "Alice" }
          UNION
          { ?x foaf:name "Bob" }
        }"""),
    "All relationships (?s ?p ?o)": textwrap.dedent("""\
        SELECT ?s ?p ?o WHERE {
          ?s ?p ?o .
        }
        LIMIT 20"""),
}

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown(
        """
<div class="sidebar-hero">
  <div class="sidebar-hero-title">🔗 SPARQL Explorer</div>
  <div class="sidebar-hero-sub">rdflib-neo4j · SPARQL→Cypher transpiler</div>
</div>
""",
        unsafe_allow_html=True,
    )

    st.markdown("**Neo4j connection**")
    neo4j_uri = st.text_input("URI", value="bolt://localhost:7687", label_visibility="collapsed",
                               placeholder="bolt://localhost:7687")
    st.caption("URI")
    neo4j_user = st.text_input("User", value="neo4j", label_visibility="collapsed")
    st.caption("User")
    neo4j_pwd = st.text_input("Password", type="password", value="password",
                               label_visibility="collapsed")
    st.caption("Password")
    neo4j_db = st.text_input("Database", value="neo4j", label_visibility="collapsed")
    st.caption("Database")

    st.divider()
    st.markdown("**Vocabulary strategy**")
    vocab_strategy = st.selectbox(
        "vocab",
        ["IGNORE — local name only", "MAP — prefix:local", "KEEP — full URI"],
        index=0,
        label_visibility="collapsed",
    )
    st.caption("How predicate URIs map to property/relationship names.")


# ── Vocab strategy mapping ────────────────────────────────────────────────────

from rdflib_neo4j.config.const import HANDLE_VOCAB_URI_STRATEGY  # noqa: E402

handle_vocab = {
    "IGNORE — local name only": HANDLE_VOCAB_URI_STRATEGY.IGNORE,
    "MAP — prefix:local": HANDLE_VOCAB_URI_STRATEGY.MAP,
    "KEEP — full URI": HANDLE_VOCAB_URI_STRATEGY.KEEP,
}[vocab_strategy]

# ── Example picker ───────────────────────────────────────────────────────────
# Extra top space ensures the dropdown opens downward rather than being clipped

st.markdown('<div style="height:3rem"></div>', unsafe_allow_html=True)
st.markdown('<div class="section-label">Example query</div>', unsafe_allow_html=True)
pick_col, param_col = st.columns([4, 4])
with pick_col:
    chosen = st.selectbox(
        "example", ["(custom)"] + list(EXAMPLES.keys()), label_visibility="collapsed"
    )

# Populate editor when example changes — bump version to force ace remount, auto-run
if st.session_state.get("_last_chosen") != chosen:
    st.session_state["_last_chosen"] = chosen
    if chosen != "(custom)":
        st.session_state["sparql_query"] = EXAMPLES[chosen]
        st.session_state["_editor_v"] = st.session_state.get("_editor_v", 0) + 1
        st.session_state["_auto_run"] = True

# ── Editor + Cypher side-by-side ──────────────────────────────────────────────

edit_col, cypher_col = st.columns(2)

with edit_col:
    st.markdown('<div class="section-label">SPARQL query</div>', unsafe_allow_html=True)
    _ekey = f"sparql_ace_{st.session_state.get('_editor_v', 0)}"
    sparql_input = st_ace(
        value=st.session_state.get("sparql_query", ""),
        language="sparql",
        theme="github",
        height=185,
        font_size=13,
        wrap=False,
        auto_update=True,
        show_gutter=False,
        show_print_margin=False,
        key=_ekey,
    )
    # auto_update=True returns None on the very first render; guard against that
    if sparql_input is not None:
        st.session_state["sparql_query"] = sparql_input
    sparql_input = sparql_input or st.session_state.get("sparql_query", "")
    run_btn = st.button("▶  Run", type="primary")

# Consume the auto-run flag set when a new example is selected
if st.session_state.pop("_auto_run", False):
    run_btn = True

# ── Live transpile ────────────────────────────────────────────────────────────

cypher_str = params = None
transpile_err = None

if sparql_input and sparql_input.strip():
    from rdflib_neo4j import Neo4jStoreConfig  # noqa: E402
    from rdflib_neo4j.sparql.transpiler import (  # noqa: E402
        TranslationError, UnsupportedAlgebraNode, translate,
    )
    try:
        cypher_str, params = translate(
            sparql_input,
            Neo4jStoreConfig(
                auth_data={"uri": neo4j_uri, "user": neo4j_user,
                            "pwd": neo4j_pwd, "database": neo4j_db},
                custom_prefixes={
                    "foaf": "http://xmlns.com/foaf/0.1/",
                    "owl":  "http://www.w3.org/2002/07/owl#",
                    "rdf":  "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                    "schema": "https://schema.org/",
                    "ex":   "http://example.org/",
                },
                handle_vocab_uri_strategy=handle_vocab,
            ),
            cypher_version_prefix=True,
        )
    except (TranslationError, UnsupportedAlgebraNode) as e:
        transpile_err = str(e)
    except Exception as e:
        transpile_err = f"Parse error: {e}"

with cypher_col:
    st.markdown('<div class="section-label">Generated Cypher</div>', unsafe_allow_html=True)
    if cypher_str:
        st.code(cypher_str, language="cypher")
    elif transpile_err:
        st.error(transpile_err, icon="⚠️")
    else:
        st.code("// type a SPARQL query to see Cypher here", language="cypher")

# ── Parameters (compact, top-right) ──────────────────────────────────────────

with param_col:
    st.markdown('<div class="section-label">Parameters</div>', unsafe_allow_html=True)
    if params:
        pills = "".join(
            f'<span class="param-pill">${k}: {v!r}</span>' for k, v in params.items()
        )
        st.markdown(f'<div style="padding:3px 0;line-height:2">{pills}</div>',
                    unsafe_allow_html=True)
    else:
        st.markdown(
            '<span style="color:#8b949e;font-size:0.8rem">none</span>',
            unsafe_allow_html=True,
        )

# ── Execute on Run ────────────────────────────────────────────────────────────

if run_btn and cypher_str:
    try:
        from neo4j import GraphDatabase, RoutingControl  # noqa: E402

        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_pwd))

        exec_cypher = cypher_str
        if exec_cypher.startswith("CYPHER"):
            exec_cypher = "\n".join(exec_cypher.splitlines()[1:]).strip()

        records, summary, keys = driver.execute_query(
            exec_cypher, database_=neo4j_db, routing_=RoutingControl.READ, **params,
        )

        if not records:
            st.info("Query returned no results.")
        else:
            # ── Table + stats ────────────────────────────────────────────────
            rows = []
            for rec in records:
                row = {}
                for k in keys:
                    val = rec[k]
                    try:
                        from neo4j.graph import Node as _N, Relationship as _R  # noqa: E402
                        if isinstance(val, _N):
                            val = val.get("uri", repr(val))
                        elif isinstance(val, _R):
                            val = val.type
                    except ImportError:
                        pass
                    row[k] = val
                rows.append(row)

            tbl_col, stat_col = st.columns([5, 1])
            with tbl_col:
                st.markdown('<div class="section-label">Results</div>', unsafe_allow_html=True)
                st.dataframe(rows, use_container_width=True, height=160)
            with stat_col:
                st.markdown('<div class="section-label">Stats</div>', unsafe_allow_html=True)
                st.metric("Rows", len(rows))
                st.metric("ms", summary.result_available_after)

            # ── Graph view ───────────────────────────────────────────────────
            try:
                from neo4j_viz.neo4j import from_neo4j  # noqa: E402

                vg = from_neo4j(
                    driver.execute_query(
                        exec_cypher, database_=neo4j_db,
                        routing_=RoutingControl.READ, **params,
                    )
                )
                if vg.nodes:
                    st.markdown(
                        '<div class="section-label">Graph view</div>',
                        unsafe_allow_html=True,
                    )
                    st.iframe(
                        vg.render(height="340px", width="100%", theme="dark").data,
                        height=358,
                    )
            except Exception as viz_err:
                st.caption(f"Graph view unavailable: {viz_err}")

        driver.close()

    except Exception as e:
        st.error(f"Neo4j error: {e}")

elif run_btn:
    st.warning("Enter a SPARQL query first.", icon="✏️")

# ── DB Schema ─────────────────────────────────────────────────────────────────

with st.expander("🗂  Database schema & ontology", expanded=False):
    if st.button("Load schema", key="schema_btn"):
        try:
            from neo4j import GraphDatabase, RoutingControl  # noqa: E402
            from neo4j_viz.neo4j import from_neo4j  # noqa: E402

            driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_pwd))

            lbls = driver.execute_query(
                "CALL db.labels() YIELD label RETURN collect(label) AS l",
                database_=neo4j_db,
            ).records[0]["l"]
            rels = driver.execute_query(
                "CALL db.relationshipTypes() YIELD relationshipType "
                "RETURN collect(relationshipType) AS r",
                database_=neo4j_db,
            ).records[0]["r"]

            mc, rc = st.columns(2)
            with mc:
                st.markdown("**Labels:** " + " ".join(f"`{x}`" for x in sorted(lbls)))
            with rc:
                st.markdown("**Rel types:** " + " ".join(f"`{x}`" for x in sorted(rels)))

            try:
                vg_s = from_neo4j(
                    driver.execute_query("CALL db.schema.visualization()", database_=neo4j_db)
                )
                if vg_s.nodes:
                    st.iframe(
                        vg_s.render(height="380px", width="100%", theme="dark").data,
                        height=398,
                    )
                else:
                    raise ValueError("empty")
            except Exception:
                # Fallback: sample actual patterns
                sample = driver.execute_query(
                    "MATCH (n)-[r]->(m) "
                    "WITH labels(n)[0] AS s, type(r) AS t, labels(m)[0] AS o "
                    "WHERE s IS NOT NULL AND o IS NOT NULL "
                    "RETURN DISTINCT s, t, o LIMIT 200",
                    database_=neo4j_db,
                )
                if sample.records:
                    from neo4j_viz import VisualizationGraph  # noqa: E402
                    from neo4j_viz.node import Node as VizNode  # noqa: E402
                    from neo4j_viz.relationship import Relationship as VizRel  # noqa: E402

                    nids: dict[str, str] = {}
                    vnodes, vrels = [], []
                    for rec in sample.records:
                        for lbl in (rec["s"], rec["o"]):
                            if lbl not in nids:
                                nids[lbl] = str(len(nids))
                                vnodes.append(VizNode(id=nids[lbl], caption=lbl))
                        vrels.append(VizRel(
                            id=f"{nids[rec['s']]}-{rec['t']}-{nids[rec['o']]}",
                            from_=nids[rec["s"]], to=nids[rec["o"]], caption=rec["t"],
                        ))
                    st.iframe(
                        VisualizationGraph(nodes=vnodes, relationships=vrels)
                        .render(height="380px", width="100%", theme="dark").data,
                        height=398,
                    )

            driver.close()
        except Exception as e:
            st.error(f"Could not connect: {e}")
