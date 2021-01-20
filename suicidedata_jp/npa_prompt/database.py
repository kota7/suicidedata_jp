# coding: utf-8

from logging import getLogger
import os
import itertools
from glob import glob
import sqlite3
import pandas as pd
from tqdm import tqdm
logger = getLogger(__name__)

def _create_derived_tables_AB5to8(dbfile):
  drop_table_template = "DROP TABLE IF EXISTS {table}_{tabulation}"
  craete_query_template = """
    CREATE TABLE {table}_{tabulation} AS
    SELECT {common_cols}, category AS "{tabulation}", n_suicide
    FROM {table} WHERE tabulation = '{tabulation}'
  """
  def _get_common_cols(table):
    out = ["time", "geocode", "geoname", "geoname2", 
           "timedef", "locdef", "sex"]
    return ", ".join(out)

  tables = ["".join(a) for a in itertools.product("AB", "5678")]
  tabulations = ["age", "housemate", "occupation", "place", "means",
                 "hour", "dayofweek", "reason", "pastattempt"]
  for table, tabulation in tqdm(itertools.product(tables, tabulations),
                                total=len(tables) * len(tabulations)):
    common_cols = _get_common_cols(table)
    q1 = drop_table_template.format(table=table, tabulation=tabulation)
    q2 = craete_query_template.format(table=table, tabulation=tabulation, common_cols=common_cols)
    with sqlite3.connect(dbfile) as conn:
      c = conn.cursor()
      logger.info("Running query: %s", q1)
      c.execute(q1)
      logger.info("Running query:\n%s", q2)
      c.execute(q2)

def create_derived_tables(dbfile):
  _create_derived_tables_AB5to8(dbfile)

def insert_csvs_to_sqlite(dbfile, csvdir):
  os.makedirs(os.path.dirname(dbfile), exist_ok=True)
  if os.path.isfile(dbfile):
    os.remove(dbfile)
    logger.debug("Existing '%s' has been deleted", dbfile)
  dirs = glob(os.path.join(csvdir, "*"))
  dirs = [d for d in dirs if os.path.isdir(d)]
  dirs.sort()
  logger.info("Directories detected (each directory becomes a table):\n %s", "\n ".join(dirs))
  for d in dirs:
    tablename = os.path.basename(d)
    csvs = glob(os.path.join(d, "**", "*.csv"), recursive=True)  # include sub-folder as well.
    csvs.sort()
    logger.info("Start creating table '%s' (%d CSV files)", tablename, len(csvs))
    for c in tqdm(csvs):
      x = pd.read_csv(c)
      with sqlite3.connect(dbfile) as conn:
        x.to_sql(tablename, conn, if_exists="append", index=False)
      logger.info("Inserted CSV file '%s' -> table '%s'", c, tablename)
    logger.info("Finish creating table '%s'", tablename)
  
def create_sqlite_database(dbfile, csvdir):
  logger.info("Start inserting csv files to '%s'", dbfile)
  insert_csvs_to_sqlite(dbfile, csvdir)
  logger.info("End inserting csv files to '%s'", dbfile)
  logger.info("Start creating derived tables in '%s'", dbfile)
  create_derived_tables(dbfile)
  logger.info("End creating derived tables in '%s'", dbfile)
