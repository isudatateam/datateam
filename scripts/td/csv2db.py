"""Verbatim copy of a csv file to the database."""
import sys
import os
from io import StringIO

from pyiem.util import logger, get_dbconn
import pandas as pd
import numpy as np

LOG = logger()
COLTYPES = {
    "object": "text",
    "int64": "int",
    "float64": "real",
    "datetime64[ns]": "timestamptz",
}
NOT_REALCOLS = {
    "siteid",
    "plotid",
    "location",
    "height",
    "date",
    "sample_type",
    "crop",
    "trt",
    "trt_value",
    "year",
    "irrigated_plotids",
    "irrigation_structure",
    "irrigation_method",
    "year_calendar",
    "date_irrigation_start",
    "date_irrigation_end",
    "comments",
    "controlled_plotids",
    "control_structure",
    "outlet_depth",
    "notes",
    "operation",
    "cashcrop",
    "plant_hybrid",
    "plant_maturity",
    "plant_maturity_GDD_source",
    "plant_rate",
    "plant_rate_units",
    "operation_type",
    "manure_source",
    "manure_method",
    "fertilizer_form",
    "fertilizer_crop",
    "fertilizer_application_type",
    "fertilizer_formulation",
    "stabilizer",
    "stabilizer_used",
    "stabilizer_name",
    "depth",
    "fertilizer_rate",
    "nitrogen_elem",
    "phosphorus_elem",
    "potassium_elem",
    "sulfur_elem",
    "zinc_elem",
    "magnesium_elem",
    "calcium_elem",
    "iron_elem",
    "station",
    "et_method",
    "subsample",
    "soil_texture",
    "percent_sand",
    "bulk_density",
    "hydraulic_conductivity",
    "NO3_concentration",
    "notill",
    "data_category",
    "variable_name",
    "method_description",
    "dwm_treatment",
    "dwmid",
    "irrigation_type",
    "irrid",
    "tile_spacing",
    "tile_material",
    "location_mngt",
    "location_agr",
    "location_sp",
    "location_sm",
    "location_wt",
    "location_st",
    "location_wq",
    "location_tf",
    "location_nl",
    "dwm",
    "dwr",
    "reading_type",
    "nitrate_N_concentration",
    "ammonia_N_concentration",
    "ortho_P_filtered_concentration",
    "ortho_P_unfiltered_concentration",
    "total_P_filtered_concentration",
}


def create_table(cursor, table_name, df):
    """Create the table."""
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    sio = StringIO()
    sio.write(f"CREATE TABLE {table_name} (")
    tokens = []
    for col in df.columns:
        coltyp = COLTYPES[str(df[col].dtype)]
        if col.startswith("date"):
            coltyp = "date"
        tokens.append("%s %s" % (col, coltyp))
    sio.write(", ".join(tokens))
    sio.write(")")
    cursor.execute(sio.getvalue())
    cursor.execute(f"GRANT SELECT on {table_name} to nobody,apache")


def copy_rows(cursor, table_name, df):
    """Copy data."""
    ps = ", ".join(["%s"] * len(df.columns))
    for row in df.itertuples(index=False):
        cursor.execute(f"INSERT into {table_name} values({ps})", row)


def create_indicies(cursor, table_name, df):
    """Create indexes."""
    if "siteid" in df.columns and "plotid" in df.columns:
        cursor.execute(f"CREATE INDEX on {table_name}(siteid, plotid)")
    if "date" in df.columns:
        cursor.execute(f"CREATE INDEX on {table_name}(date)")


def main(argv):
    """Go Main Go."""
    csvfn = argv[1]
    table_name = os.path.basename(csvfn)[:-4]
    LOG.info("dumping %s into table: %s", csvfn, table_name)
    df = pd.read_csv(csvfn, low_memory=False)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    failed = False
    for col in df.columns:
        if col in NOT_REALCOLS:
            continue
        try:
            df[col] = pd.to_numeric(df[col])
        except Exception:
            LOG.info("failed to convert col: %s to numeric", col)
            failed = True
    if failed:
        sys.exit()
    pgconn = get_dbconn("td")
    cursor = pgconn.cursor()
    create_table(cursor, table_name, df)
    # Do after schema creation as types will change with the below?
    df = df.replace({np.nan: None})
    copy_rows(cursor, table_name, df)
    create_indicies(cursor, table_name, df)
    cursor.close()
    pgconn.commit()


if __name__ == "__main__":
    main(sys.argv)
