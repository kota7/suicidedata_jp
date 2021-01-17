# coding: utf-8

from logging import getLogger
import os
import re
from urllib.request import urlopen
from urllib.parse import urljoin
from shutil import copyfileobj
from bs4 import BeautifulSoup as bs
from tqdm import tqdm

from ..utils import urlretrieve
logger = getLogger(__name__)

ROOTURL = "https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/0000140901.html"

def _find_year_urls():
  x = urlopen(ROOTURL).read()
  soup = bs(x, "html.parser")
  urls = []
  for l in soup.find_all("a"):
    if re.match(r"地域における自殺の基礎資料[\(（].*年[\)）]", l.text.strip()) is None:
      continue
    url = l.get("href")
    if url is None:
      continue
    url = urljoin(ROOTURL, url)
    urls.append(url)
  urls = list(set(urls))
  return urls

def _find_monthzip_urls(year_url):
  x = urlopen(year_url).read()
  soup = bs(x, "html.parser")
  urls = {}
  for l in soup.find_all("a"):
    r = re.match(r"([^\d]+)(\d+|元)年(\d+)月.*[\(（]暫定値[\)）]", l.text.strip())
    if r is None:
      continue
    gou = r.group(1)
    year = r.group(2)
    year = 1 if year=="元" else int(year)
    if gou == "平成":
      year += 1988
    elif gou == "令和":
      year += 2018
    else:
      logger.error("Wareki '%s' is not supported", gou)
      raise ValueError("Wareki '{}' is not supported".format(gou))
    month = int(r.group(3))
    url = l.get("href")
    if url is None:
      continue
    if url.lower()[-4:] != ".zip":
      continue
    url = urljoin(year_url, url)
    urls[(year, month)] = url
  logger.debug("Zip links found in '%s': %s", year_url, urls)
  return urls

def get_zipfile_urls(month_from=(1900, 1), month_to=(9999, 12)):
  # returns dict (year, month) -> url
  year_urls = _find_year_urls()
  logger.debug("Year urls detected: %s", year_urls)
  month_urls = {}
  for year_url in year_urls:
    tmp = _find_monthzip_urls(year_url)
    for key, url in tmp.items():
      if key < month_from or key > month_to:
        logger.debug("%s: '%s' is out of target period", key, url)
        continue  # out of target period
      month_urls[key] = url
  return month_urls

def _filename(year, month):
  return "%04d-%02d.zip" % (year, month)

def download_zipfiles(savedir, month_from=(1900, 1), month_to=(9999, 12), replace=False):
  urls = get_zipfile_urls(month_from=month_from, month_to=month_to)
  downloaded = []
  for (year, month), url in tqdm(urls.items()):
    savepath = os.path.join(savedir, _filename(year, month))
    if (not replace) and os.path.isfile(savepath):
      logger.debug("'%s' already exists, skipped", savepath)
      continue
    urlretrieve(url, savepath)
    downloaded.append((url, savepath))
    logger.info("Downloaded '%s' -> '%s'", url, savepath)
  return downloaded