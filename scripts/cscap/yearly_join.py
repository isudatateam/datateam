from pandas.io.sql import read_sql
import psycopg2
import pandas as pd

pgconn = psycopg2.connect(database='sustainablecorn', host='iemdb',
                          user='nobody')
df = read_sql("""
  SELECT year, site, plotid, depth, avg(value::float) as value from soil_data
  where varname = 'SOIL13'
  and value not in ('.', 'did not collect', 'n/a') and
  substr(value, 1, 1) != '<'
  GROUP by year, site, plotid, depth
  """, pgconn, index_col=None)

df = pd.pivot_table(df, values='value', index=['site', 'plotid', 'depth'],
                    columns=['year'])
df.to_csv('test.csv')
