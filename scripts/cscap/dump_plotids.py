from pandas.io.sql import read_sql
import psycopg2
pgconn = psycopg2.connect(database='sustainablecorn', host='iemdb',
                          user='nobody')

df = read_sql("""SELECT * from plotids""", pgconn)

df.to_csv('test.csv')