"""This is our fancy pants filter function.

We end up return a JSON document that lists out what is possible

{'treatments': ['ROT1', 'ROT2'...],
 'years': [2011, 2012, 2013...],
 'agronomic': ['AGR1', 'AGR2']
}
"""
import json
import sys

from paste.request import parse_formvars
from pandas.io.sql import read_sql
import pandas as pd
from pyiem.util import get_dbconn

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
    sys.stderr.write("dedup added %s to %s\n" % (str(additional), str(arr)))
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
    sys.stderr.write("agg added %s to %s\n" % (str(additional), str(arr)))
    return arr


def do_filter(form):
    """Do the filtering fun."""
    pgconn = get_dbconn("td")
    res = {"treatments": [], "agronomic": [], "soil": [], "water": []}
    sites = agg(form.getall("sites[]"))
    # treatments = agg(form.getall("treatments[]"))
    # agronomic = agg(form.getall("agronomic[]"))
    # soil = agg(form.getall("soil[]"))
    # ghg = agg(form.getall("ghg[]"))
    # water = agg(form.get("water[]"))
    # ipm = agg(form.get("ipm[]"))
    # year = agg(form.get("year[]"))

    # build a list of treatments based on the sites selected
    df = read_sql(
        "select distinct dwm from meta_treatment_identifier where "
        "siteid in %s and dwm is not null",
        pgconn,
        params=(tuple(sites),),
        index_col=None,
    )
    res["treatments"] = df["dwm"].tolist()

    # Agronomic Filtering
    df = read_sql(
        "select * from agronomic_data where siteid in %s",
        pgconn,
        params=(tuple(sites),),
        index_col=None,
    )
    arr = []
    for col, val in df.max().iteritems():
        if not pd.isnull(val):
            arr.append(col)
    res["agronomic"] = arr

    # Soil Filtering
    df = read_sql(
        "select * from soil_properties_data where siteid in %s",
        pgconn,
        params=(tuple(sites),),
        index_col=None,
    )
    arr = []
    for col, val in df.max().iteritems():
        if not pd.isnull(val):
            arr.append(col)
    res["soil"] = arr

    # Water Filtering
    df = read_sql(
        "select max(soil_moisture) as soil_moisture, "
        "max(soil_temperature) as soil_temperature, "
        "max(soil_ec) as soil_ec from soil_moisture_data where siteid in %s "
        "GROUP by siteid",
        pgconn,
        params=(tuple(sites),),
        index_col=None,
    )
    for col, val in df.max().iteritems():
        if not pd.isnull(val):
            res["water"].append(col)
    df = read_sql(
        "select max(tile_flow) as tile_flow, "
        "max(discharge) as discharge, "
        "max(nitrate_n_load) as nitrate_n_load, "
        "max(nitrate_n_removed) as nitrate_n_removed, "
        "max(tile_flow_filled) as tile_flow_filled, "
        "max(nitrate_n_load_filled) as nitrate_n_load_filled "
        "from tile_flow_and_N_loads_data where siteid in %s "
        "GROUP by siteid",
        pgconn,
        params=(tuple(sites),),
        index_col=None,
    )
    for col, val in df.max().iteritems():
        if not pd.isnull(val):
            res["water"].append(col)
    df = read_sql(
        "select * from water_quality_data where siteid in %s",
        pgconn,
        params=(tuple(sites),),
        index_col=None,
    )
    for col, val in df.max().iteritems():
        if not pd.isnull(val):
            res["water"].append(col)

    return res


def application(environ, start_response):
    """Do Stuff"""
    start_response("200 OK", [("Content-type", "application/json")])
    form = parse_formvars(environ)
    res = do_filter(form)
    return [json.dumps(res).encode("utf-8")]
