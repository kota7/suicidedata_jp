# coding: utf-8

from logging import getLogger
import os
import sqlite3
from shutil import copyfileobj
from urllib.request import urlopen
from tqdm import tqdm
import pandas as pd
logger = getLogger(__name__)

def urlretrieve(url, savepath):
  os.makedirs(os.path.dirname(savepath), exist_ok=True)
  obj = urlopen(url)
  with open(savepath, "wb") as f:
    copyfileobj(obj, f)

def sqlite_to_csvs(dbfile, outdir, skipped=[], compress=True):
  os.makedirs(outdir, exist_ok=True)
  q = "SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%';"
  with sqlite3.connect(dbfile) as conn:
    c = conn.cursor()
    c.execute(q)
    tables = [row[0] for row in c]
  logger.info("%d tables in '%s': %s", len(tables), dbfile, tables)
  skipped = set([s.lower() for s in skipped])
  tables = [t for t in tables if t.lower() not in skipped]
  outpath = []
  for t in tqdm(tables):
    logger.info("Exporting '%s'", t)
    with sqlite3.connect(dbfile) as conn:
      q = 'SELECT * FROM "{}"'.format(t)
      logger.info("Running query: %s", q)
      x = pd.read_sql(q, conn)
      ext = ".csv.gz" if compress else ".csv"
      savepath = os.path.join(outdir, "{}{}".format(t, ext))
      x.to_csv(savepath, index=False)
      logger.info("Table '%s' -> File '%s'", t, savepath)
      outpath.append(savepath)
  return outpath