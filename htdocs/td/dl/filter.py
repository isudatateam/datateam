"""This is our fancy pants filter function.

We end up return a JSON document that lists out what is possible

{'treatments': ['ROT1', 'ROT2'...],
 'years': [2011, 2012, 2013...],
 'agronomic': ['AGR1', 'AGR2']
}
"""

import json

import pandas as pd
from pyiem.database import get_sqlalchemy_conn
from pyiem.webutil import ensure_list, iemapp
from sqlalchemy import text

# NOTE: filter.py is upstream for this table, copy to dl.py
AGG = {
    "_T1": ["ROT4", "ROT5", "ROT54"],
    "_T2": ["ROT8", "ROT7", "ROT6"],
    "_T3": ["ROT16", "ROT15", "ROT17"],
    "_T4": ["ROT37", "ROT36", "ROT55", "ROT59"],
    "_T5": ["ROT61", "ROT56", "ROT1"],
    "_T6": ["ROT57", "ROT58", "ROT38"],
    "_T7": ["ROT40", "ROT50"],
    "_S1": [
        "SOIL41",
        "SOIL34",
        "SOIL29",
        "SOIL30",
        "SOIL31",
        "SOIL2",
        "SOIL35",
        "SOIL32",
        "SOIL42",
        "SOIL33",
        "SOIL39",
    ],
    "_S19": [
        "SOIL19.8",
        "SOIL19.11",
        "SOIL19.12",
        "SOIL19.1",
        "SOIL19.10",
        "SOIL19.2",
        "SOIL19.5",
        "SOIL19.7",
        "SOIL19.6",
        "SOIL19.13",
    ],
}


def redup(arr):
    """Replace any codes that are collapsed by the above"""
    additional = []
    for key in AGG:
        vals = AGG[key]
        for val in vals:
            if val in arr and key not in additional:
                additional.append(key)
    return arr + additional


def agg(arr):
    """Make listish and apply dedup logic"""
    if len(arr) == 0:
        arr.append("ZZZ")
    additional = []
    for val in arr:
        if val in AGG:
            additional += AGG[val]
    if len(additional) > 0:
        arr += additional
    return arr


def do_filter(pgconn, environ):
    """Do the filtering fun."""
    res = {"treatments": [], "agronomic": [], "soil": [], "water": []}
    sites = agg(ensure_list(environ, "sites[]"))

    # build a list of treatments based on the sites selected
    df = pd.read_sql(
        text(
            "select distinct drainage_water_management, irrigation "
            "from meta_treatment_identifier where "
            "siteid = ANY(:sites)"
        ),
        pgconn,
        params={"sites": sites},
        index_col=None,
    )
    res["treatments"] = df["drainage_water_management"].unique().tolist()
    res["treatments"].extend(df["irrigation"].unique().tolist())

    # Agronomic Filtering
    df = pd.read_sql(
        text("select * from agronomic_data where siteid = ANY(:sites)"),
        pgconn,
        params={"sites": sites},
        index_col=None,
    )
    for col, val in df.max(numeric_only=True).items():
        if not pd.isnull(val):
            res["agronomic"].append(col)

    # Soil Filtering
    df = pd.read_sql(
        text("select * from soil_properties_data where siteid = ANY(:sites)"),
        pgconn,
        params={"sites": sites},
        index_col=None,
    )
    for col in df.columns:
        if all(pd.isnull(df[col])):
            continue
        res["soil"].append(col)

    # Water Table Filtering
    df = pd.read_sql(
        text(
            "select max(water_table_depth) as water_table_depth "
            "from water_table_data where siteid = ANY(:sites) "
            "GROUP by siteid"
        ),
        pgconn,
        params={"sites": sites},
        index_col=None,
    )
    for col, val in df.max(numeric_only=True).items():
        if not pd.isnull(val):
            res["water"].append(col)

    # Water Stage Filtering
    df = pd.read_sql(
        text(
            "select max(stage) as water_stage "
            "from water_stage_data where siteid = ANY(:sites) "
            "GROUP by siteid"
        ),
        pgconn,
        params={"sites": sites},
        index_col=None,
    )
    for col, val in df.max(numeric_only=True).items():
        if not pd.isnull(val):
            res["water"].append(col)

    # Water Filtering
    df = pd.read_sql(
        text(
            "select max(soil_moisture) as soil_moisture, "
            "max(soil_temperature) as soil_temperature, "
            "max(soil_ec) as soil_ec from soil_moisture_data where "
            "siteid = ANY(:sites) GROUP by siteid"
        ),
        pgconn,
        params={"sites": sites},
        index_col=None,
    )
    for col, val in df.max(numeric_only=True).items():
        if not pd.isnull(val):
            res["water"].append(col)
    df = pd.read_sql(
        text(
            "select max(tile_flow) as tile_flow, "
            "max(discharge) as discharge, "
            "max(nitrate_n_load) as nitrate_n_load, "
            "max(nitrate_n_removed) as nitrate_n_removed, "
            "max(tile_flow_filled) as tile_flow_filled, "
            "max(nitrate_n_load_filled) as nitrate_n_load_filled "
            "from tile_flow_and_N_loads_data where siteid = ANY(:sites) "
            "GROUP by siteid"
        ),
        pgconn,
        params={"sites": sites},
        index_col=None,
    )
    for col, val in df.max(numeric_only=True).items():
        if not pd.isnull(val):
            res["water"].append(col)
    df = pd.read_sql(
        text("select * from water_quality_data where siteid = ANY(:sites)"),
        pgconn,
        params={"sites": sites},
        index_col=None,
    )
    # tricky non-numeric stuff here, sigh
    for col in df.columns:
        if all(pd.isnull(df[col])):
            continue
        res["water"].append(col)

    return res


@iemapp()
def application(environ, start_response):
    """Do Stuff"""
    start_response("200 OK", [("Content-type", "application/json")])
    with get_sqlalchemy_conn("td") as pgconn:
        res = do_filter(pgconn, environ)
    return json.dumps(res).encode("utf-8")
