# coding: utf-8

from logging import getLogger
import itertools
import os
import numpy as np
import pandas as pd
import re
import jaconv
from tqdm import tqdm
logger = getLogger(__name__)


def _read_file(srcpath):
  # read either xls, xlsx or csv file
  # return numpy array
  with open(srcpath, "rb") as f:
    try:
      x = pd.read_excel(f, engine="xlrd", header=None)
      logger.debug("'%s' is read as .xls file", srcpath)
      return x
    except Exception as e:
      logger.info("'%s' could not be read as .xls file: '%s'", srcpath, e)
  
  with open(srcpath, "rb") as f:
    try:
      x = pd.read_excel(f, engine="openpyxl", header=None)
      logger.debug("'%s' is read as .xlsx file", srcpath)
      return x
    except Exception as e:
      logger.info("'%s' could not be read as .xlsx file: '%s'", srcpath, e)

  with open(srcpath, "rb") as f:
    try:
      x = pd.read_csv(f, encoding="cp932", header=None)
      logger.debug("'%s' is read as .csv file", srcpath)
      return x
    except Exception as e:
      logger.info("'%s' could not be read as .csv file: '%s'", srcpath, e)
  raise ValueError("Failed to read '%s' (not .xls, .xlsx nor .csv?)", srcpath)

def parse_to_df(srcpath):
  x = _read_file(srcpath)
  x = x.fillna("").values.astype(str)
  def _find_year_month():
    for i, j  in itertools.product(range(5), range(5)):
      cell = x[i,j].strip()
      logger.debug("Cell at (%d,%d) = %s", i, j, cell)
      r = re.search(r"([^\d]+)(\d+|元)年(\d+)月", cell)
      if r:
        gou, year, month = r.groups()
        month = int(month)
        year = 1 if year=="元" else int(year)
        if gou == "昭和":
          year += 1925
        elif gou == "平成":
          year += 1988
        elif gou == "令和":
          year += 2018
        else:
          logger.error("Unknown wareki '%s'", gou)
          raise ValueError("Unknown wareki '{}'".format(gou))
        return year, month
    raise ValueError("Year, month not found in '{}'".format(srcpath))
  year, month = _find_year_month()
  time = "%04d-%02d" % (year, month)

  def _find_age_row():
    for i in range(10):
      count = sum(1 for v in x[i] if v.find("歳") > 0)
      if count > 3:
        return i
    logger.error("Age row could not be detected in '%s'", srcpath)
    raise ValueError("Age row could not be detected in '{}'".format(srcpath))
  agerow = _find_age_row()
  logger.debug("Age row: %d (%s)", agerow, x[agerow])
  def _find_data_rows():
    def _condition(v):
      v = re.sub(r"\s", "", v)
      if re.match(r"\d+-\d+", v) is not None:
        return True
      if v == "不詳":
        return True
      if v.find("以上") > 0:
        return True
      if v != "":
        logger.debug("Age '%s' is omitted", v)
      return False
    out = [j for j, v in enumerate(x[agerow]) if _condition(v)]
    out = np.array(out)
    return out
  datarows = _find_data_rows()
  ages = x[agerow, datarows].tolist()
  def _normalize_age(a):
    a = re.sub(r"\s+", "", a)
    a = a.replace("歳", "").replace("以上", "+")
    a = jaconv.z2h(a, digit=True)
    r = re.match(r"^(\d+)\-(\d+)$", a)
    if not r:
      return a
    else:
      return "%02d-%02d" % (int(r.group(1)), int(r.group(2)))
  ages = [_normalize_age(a) for a in ages]
  logger.debug("Data rows: %s", datarows)
  logger.debug("Age values: %s", ages)

  def _find_sex_col():
    for j in range(10):
      col = x[0:30, j]
      count = sum(1 for v in col if v.find("男") >= 0)
      if count > 5:
        logger.debug("Sex col detected at %d: %s", j, col)
        return j
    logger.error("Sex column could not be detected in; '%s'", srcpath)
    raise ValueError("Sex column could not be detected in; '{}'", srcpath)
  sexcol = _find_sex_col()
  logger.debug("Sex col: %d (%s)", sexcol, x[0:10, sexcol])

  def _find_cause_col():
    for j in range(10):
      col = x[0:30, j]
      count = sum(1 for v in col if v.find("症") >= 0)
      if count > 1:
        logger.debug("Cause col detected at %d: %s", j, col)
        return j
    logger.error("Cause column could not be detected in; '%s'", srcpath)
    raise ValueError("Cause column could not be detected in; '{}'", srcpath)
  causecol = _find_cause_col()
  logger.debug("Cause col: %d (%s)", causecol, x[0:30, causecol])

  def _find_geo_col():
    for j in range(10):
      col = [re.sub(r"\s", "", v) for v in x[:, j]]
      for v in x[:, j]:
        v = re.sub(r"\s", "", v)
        if v.find("北海道") >= 0:
          return j
    logger.error("Geography column could not be detected in; '%s'", srcpath)
    raise ValueError("Geography column could not be detected in; '{}'", srcpath)
  geocol = _find_geo_col()
  logger.debug("Geography col: %d (%s)", geocol, x[0:20, geocol])

  def _normalize_geoname(geoname):
    geoname = re.sub(r"\s+", "", geoname)
    if geoname[-1] in "県道府市部": # '部' for '東京都区部'
      return geoname
    elif geoname in ("外国", "不詳"):
      return geoname
    elif geoname in ("京都", "大阪"):
      return geoname + "府"
    elif geoname in ("東京", "東京都"):
      return "東京都"
    elif geoname == "北海":  # just in case
      return "北海道"
    else:
      return geoname + "県"

  def _normalize_cause(cause):
    return re.sub(r"\d+", "", cause)  

  def _split_geocode(geo):
    r = re.match(r"(\d*)(.*)", geo.strip())
    if r is None:
      return "", geo.strip()  # code not found
    else:
      code = jaconv.z2h(r.group(1), digit=True)
      geoname = r.group(2)
      return code, geoname

  out = []
  header = ["time", "geocode", "geoname", "sex", "cause"] + ages
  geo, cause = None, None
  for i in range(agerow+1, len(x)):
    sex = x[i, sexcol].strip()
    # update geo
    tmp = re.sub(r"\s", "", x[i, geocol])
    if tmp != "" and sex == "":
      geo = tmp
      geocode, geoname = _split_geocode(geo)
      geoname = _normalize_geoname(geoname)
      logger.debug("Row %d: Geo updated to '%s' (%s, %s)", i, geo, geocode, geoname)
    # update cause
    tmp = re.sub(r"\s", "", x[i, causecol])
    if tmp != "" and sex == "計":
      cause = tmp
      cause = _normalize_cause(cause)
      logger.debug("Row %d: Cause updated to '%s'", i, cause)
    # skip criteria
    if sex not in ("男", "女"):
      continue
    if geo is None or geo.find("全国") >= 0:
      continue
    if cause is None or cause.find("自殺") < 0:
      continue
    sex_eng = "male" if sex == "男" else "female"
    row = [time, geocode, geoname, sex_eng, cause] + x[i, datarows].tolist()
    out.append(row)
  out = pd.DataFrame(out, columns=header)
  out = out.melt(id_vars=["time", "geocode", "geoname", "sex", "cause"],
                 value_vars=ages, var_name="age", value_name="n_death")
  # cleaning
  def _clean_numbers(n):
    n = n.str.strip()
    n[n == "-"] = "0"
    n[n.isin(("・", "…", ""))] = None
    ns = n.unique()
    not_numbers = [a for a in ns if a is not None and re.match(r"^\d+$", a) is None]
    if len(not_numbers) > 0:
      pos = np.where(n.isin(not_numbers))[0]
      logger.error("Irregular number expressions: %s at %s", not_numbers, pos)      
      raise ValueError("Irregular number expressions: {} at {}".format(not_numbers, pos))
    n = n.astype(float)  # safe, all numbers are either number or None
    return n
  out.n_death = _clean_numbers(out.n_death)

  return out
  # savepath = os.path.join(outdir, "{}.csv".format(time))
  # out.to_csv(savepath, index=False)

def parse_files(srcfiles, outdir):
  os.makedirs(outdir, exist_ok=True)
  for srcpath in tqdm(srcfiles):
    try:
      x = parse_to_df(srcpath)
    except Exception as e:
      logger.error("Error occurred while parsing '%s': '%s'", srcpath, e)
      raise e
    savepath, _ = os.path.splitext(os.path.basename(srcpath))
    savepath = os.path.join(outdir, savepath + ".csv")
    x.to_csv(savepath, index=False)
    logger.info("Parsed '%s'\n-> '%s' (shape: %s)", srcpath, savepath, x.shape)