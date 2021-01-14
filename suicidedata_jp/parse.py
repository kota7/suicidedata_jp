# coding: utf-8

import os
import re
from zipfile import ZipFile
from shutil import rmtree, copyfileobj
from xlrd import open_workbook

def unzip_xls_files(rawdir="./raw", outdir="./tmp"):
  if os.path.isdir(outdir):
    rmtree(outdir)
  os.makedirs(outdir, exist_ok=True)
  xlsfiles = []
  with ZipFile("raw/content_12200000_000523586.zip") as z:
    for member in z.infolist():
      if not os.path.splitext(member.filename)[1].lower() in (".xls", ".xlsx"):
        # all files so far are .xls, but allow .xlsx just in case
        continue
      filename = member.filename.encode("cp437").decode("cp932")
      filename = os.path.join(outdir, filename)
      with z.open(member.filename) as src, open(filename, "wb") as dst:
        copyfileobj(src, dst)
      xlsfiles.append(filename)
  return xlsfiles

def get_table_type(sheet):
  for cell in sheet[0]:
    v = cell.value.strip()
    r = re.match(r"([A-C]\d)表", v)
    if r is not None:
      return r.group(1)
  raise ValueError("Table type not found in {}".format(sheet[0]))

