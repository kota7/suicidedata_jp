# coding: utf-8

from logging import getLogger
from glob import glob
import os
import pandas as pd
import sqlite3
from tqdm import tqdm
logger = getLogger(__name__)


def insert_csvs_to_sqlite(dbfile, csvdir, tablename="prompt"):
  os.makedirs(os.path.dirname(dbfile), exist_ok=True)
  if os.path.isfile(dbfile):
    q = 'DROP TABLE IF EXISTS "{}"'.format(tablename)
    with sqlite3.connect(dbfile) as conn:
      c = conn.cursor()
      logger.info("Running query: %s", q)
      c.execute(q)
    logger.debug("Existing table '%s' in '%s' has been deleted", tablename, dbfile)
  csvfiles = glob(os.path.join(csvdir, "**", "*.csv"), recursive=True)
  logger.info("Start creating table '%s' of '%s'", tablename, dbfile)
  for c in tqdm(csvfiles):
    x = pd.read_csv(c)
    with sqlite3.connect(dbfile) as conn:
      x.to_sql(tablename, conn, if_exists="append", index=False)
    logger.info("Inserted CSV file '%s' -> table '%s' of '%s'", c, tablename, dbfile)
  logger.info("Finish creating table '%s' of '%s'", tablename, dbfile)

def create_sqlite_database(dbfile, csvdir, tablename="prompt"):
  logger.info("Start creating table '%s' in '%s'", tablename, dbfile)
  insert_csvs_to_sqlite(dbfile, csvdir, tablename=tablename)
  logger.info("End creating table '%s' in '%s'", tablename, dbfile)
  