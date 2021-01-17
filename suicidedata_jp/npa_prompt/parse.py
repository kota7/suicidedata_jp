# coding: utf-8

from logging import getLogger
import os
import re
import tempfile
import itertools
from zipfile import ZipFile
from shutil import rmtree, copyfileobj
import pandas as pd
from tqdm import tqdm
from xlrd import open_workbook
logger = getLogger(__name__)

def get_sheet_type(sheet):
  for cell in sheet[0]:
    v = cell.value.strip()
    r = re.match(r"([A-C]\d)表", v)
    if r is not None:
      return r.group(1)
  raise ValueError("Table type not found in {}".format(sheet[0]))

# todo: parser for [AB][1-4]

def _parse_AB5to8_sheet(sheet):
  # parser for [AB][5-8]
  type_ = get_sheet_type(sheet)
  assert re.match("[AB][5-8]$", type_) is not None, "Given: '{}'".format(type_)

  timedef = "dead" if type_[0] == "A" else "found"
  locdef = "residence" if int(type_[1]) in (5, 7) else "found"
  geolevel = "prefecture" if int(type_[1]) in (5, 6) else "municipality"
  
  def _find_table_edge():
    # table's top-left edge position, including header row
    for i, j in itertools.product(range(5), range(5)):
      if sheet[i][j].value.find("コード") >= 0:
        return i, j
    return None
  edge = _find_table_edge()
  assert edge is not None, "table edge not found"
  row0, col0 = edge

  def _find_month():
    for i, j in itertools.product(range(row0), range(7)):
      cell = sheet[i][j]
      r = re.match("([^\d]+)(元|\d+)年(\d{1,2})月", cell.value)
      if r is not None:
        gou = r.group(1)
        year = r.group(2)
        year = 1 if year == "元" else int(year)
        if gou == "平成":
          year += 1988
        elif gou == "令和":
          year += 2018
        else:
          raise ValueError("Unknown wareki '{}'".format(gou))
        month = int(r.group(3))
        month = "%04d-%02d" % (year, month)
        return month
    return None
  month = _find_month()
  assert month is not None, "month not found"

  def _find_sex():
    for i in range(row0):
      cell = sheet[i][col0]
      r = re.match("(総数|男|女)", cell.value)
      if r is not None:
        sex = ("total" if r.group(1) == "総数" else
               "male" if r.group(1) == "男" else
               "female" if r.group(1) == "女" else
               None)
        return sex
    return None
  sex = _find_sex()
  assert sex is not None, "sex not found"

  def _find_datarows():
    for i in range(row0 + 4, row0 + 9):
      if sheet[i][col0].value != "":
        startrow = i
        for k in reversed(range(startrow, sheet.nrows)):
          if sheet[k][col0].value != "":
            return range(startrow, k + 1)
    return None
  rows = _find_datarows()
  rows = list(rows)
  assert rows is not None, "start row not found"

  def _find_col_locations():
    col_widths = [
       ("common", 2 if geolevel=="prefecture" else 3)
      ,("aggregate", 3) # unnecessary aggregate level information
      ,("age", 9)
      ,("housemate", 3)
      ,("occupation", 10)
      ,("place", 7)
      ,("means", 7)
      ,("hour", 13)
      ,("dayofweek", 8)
      ,("reason", 8)
      ,("pastattempt", 3)
    ]
    col_locs = {}
    col = col0
    for name, size in col_widths:
      col_locs[name] = range(col, col + size)
      col += size
    return col_locs
  col_locs = _find_col_locations()

  common_cols = col_locs.pop("common")
  col_locs.pop("aggregate")  # we don't use these columns

  if geolevel == "municipality":  # add extra column for wards ("ku")
    comman_cols_name = ["geocode", "geoname", "geoname2"]
  else:
    comman_cols_name = ["geocode", "geoname"]
  # column headers
  categories = [sheet[row0+3][j].value if sheet[6][j].value != "" else \
                sheet[row0+2][j].value if sheet[5][j].value != "" else \
                sheet[row0+1][j].value if sheet[4][j].value != "" else \
                None \
                for j in range(sheet.ncols)]
  #print(categories)
  ignored = set(["無職", "無職者"])  # shall be ignored, these are subtotals

  # obtain common data
  common = [[sheet[i][j].value for j in common_cols] for i in rows]
  common = pd.DataFrame(common, columns=comman_cols_name)
  common.geocode = common.geocode.astype(int)
  # for municipality data, fill empty geoname
  common = common.reset_index(drop=True)  # clear index just in case
  if geolevel == "municipality":
    # for municipality, geoname: ku-aggregated, geoname2: ku-disaggregated
    # # fill empty geonames vertically (add aggregate geolevel name)
    # v = None
    # for i in common.index:
    #   if common.loc[i,"geoname"].strip() != "":
    #     v = common.loc[i,"geoname"]
    #   else:
    #     common.loc[i,"geoname"] = v
    # fill empty geonames2 horizontally (propagate aggregate geolevel name to lower)
    flg1 = (common.geoname.astype(str).str.contains("（計）"))  # ku-aggregate cols
    flg2 = (common.geoname.astype(str).str.strip() == "")    # ku-disaggregate cols
    common.loc[flg2, "geoname"] = ""
    common.loc[~flg1 & ~flg2, "geoname2"] = common.geoname[~flg1 & ~flg2]
    common.geoname = common.geoname.str.replace("（計）", "")
    # for i in common.index:
    #   if common.loc[i,"geoname2"].strip() == "":
    #     common.loc[i,"geoname2"] = common.loc[i,"geoname"]
  else:
    # for pref, geoname: nation total, geoname2: prefecture disaggregated
    common["geoname2"] = common["geoname"]
    common.loc[common.geocode != 0, "geoname"] = ""
    common.loc[common.geocode == 0, "geoname2"] = ""
  
  common["time"] = month
  common["timedef"] = timedef
  common["locdef"] = locdef
  common["geolevel"] = geolevel
  common["tablecode"] = type_.upper()
  common["sex"] = sex
  # get data across tabulation
  out = None
  for g, cols in col_locs.items():
    for j in cols:
      category = categories[j]
      if category in ignored:
        continue
      tmp = common.copy()
      tmp["tabulation"] = g
      tmp["category"] = category
      tmp["n_suicide"] = [sheet[i][j].value for i in rows]
      out = tmp if out is None else pd.concat((out, tmp), ignore_index=True)
  
  ## remove rows not needed (due to duplicates)
  # if geolevel=="prefecture":
  #   out["subtotal"] = (out.geocode == 0)  # national total
  #   pass
  # elif geolevel=="municipality":
  #   out["subtotal"] = (out.geoname2.str.contains("（計）")) # ku-total rows
  #   # remove "（計）" from text
  #   out.geoname = out.geoname.str.replace("（計）", "")
  #   out.geoname2 = out.geoname2.str.replace("（計）", "")
  # cleaning
  out.geoname = out.geoname.astype(str).str.strip()
  out.geoname2 = out.geoname2.astype(str).str.strip()
  # empty n_suicide to None, then cast to integer
  out.loc[out.n_suicide.astype(str).str.strip().isin(("", "***")), "n_suicide"] = None
  out.n_suicide = out.n_suicide.astype(float)
  return out

def parse_sheet(sheet):
  type_ = get_sheet_type(sheet)
  if re.match("[AB][5-8]$", type_) is not None:
    return _parse_AB5to8_sheet(sheet)
  else:
    None  # no parser defined yet

def parse_book(book, outdir):
  if type(book) == str:
    book = open_workbook(book)
  out = {}
  for sheet in book.sheets():
    try:
      x = parse_sheet(sheet)
    except Exception as e:
      logger.error("Error while parsing '%s', sheet '%s': %s", book, sheet, e)
      raise e
    if x is None:
      continue # no parser for this sheet
    type_ = get_sheet_type(sheet)
    out[type_] = x if type_ not in out else pd.concat((out[type_], x), ignore_index=False)
  
  outfiles = []
  for type_, df in out.items():
    time = df.time.unique()  # assumes that time variable is unique within a book
    assert len(time) == 1, "time is not unique within a book: {}".format(time)
    time = time.item()
    csvpath = os.path.join(outdir, type_, "{}.csv".format(time))
    os.makedirs(os.path.dirname(csvpath), exist_ok=True)
    df.to_csv(csvpath, index=False)
    outfiles.append(csvpath)
  return outfiles

def extract_xls_files(zippath, outdir):
  logger.info("Extracting xls files in '%s' into '%s", zippath, outdir)  
  assert os.path.isdir(outdir), "'{}' is not a directory".format(outdir)
  assert os.path.isfile(zippath), "'{}' is not a file".format(zippath)
  xlsfiles = []
  with ZipFile(zippath) as z:
    for member in z.infolist():
      if not os.path.splitext(member.filename)[1].lower() == ".xls":
        logger.debug("'%s' is skipped (not a .xls file)", member.filename)
        # all files so far are .xls, may need to allow .xlsx in the future
        continue
      filename = member.filename.encode("cp437").decode("cp932")
      filename = os.path.join(outdir, filename)
      with z.open(member.filename) as src, open(filename, "wb") as dst:
        copyfileobj(src, dst)
      xlsfiles.append(filename)
      logger.debug("'%s' is extracted", filename)
  logger.info("Extracted: %d files\n %s", len(xlsfiles), "\n ".join(xlsfiles))
  return xlsfiles

def parse_zipfile(zippath, outdir):
  # parse geodada in a zipfile to csvfile files
  with tempfile.TemporaryDirectory() as tmpdir:
    logger.debug("Temporary directory to extract xls files: '%s'", tmpdir)
    xlsfiles = extract_xls_files(zippath, outdir=tmpdir)
    outfiles = []
    for f in xlsfiles:
      logger.debug("Parsing '%s'", f)
      tmp = parse_book(f, outdir)
      outfiles += tmp  
  return outfiles

def parse_zipfiles(zippaths, outdir):
  outfiles = []
  for zippath in tqdm(zippaths):
    logger.info("Parsing '%s'", zippath)
    tmp = parse_zipfile(zippath, outdir)
    logger.info("Created: %d files\n %s", len(tmp), "\n ".join(tmp))
    outfiles += tmp
  return outfiles