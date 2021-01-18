# coding: utf-8

from logging import getLogger
import os
import re
from urllib.request import urlopen
from urllib.parse import urljoin
from bs4 import BeautifulSoup as bs
from tqdm import tqdm

from ..utils import urlretrieve
logger = getLogger(__name__)

ROOTURL = "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00450011&tstat=000001028897&cycle=1&tclass1=000001053058&tclass2=000001053060&result_back=1&tclass3val=0"
KEYWORDS = ("県", "死因", "性", "年齢")

def _find_month_urls():
  x = urlopen(ROOTURL).read()
  soup = bs(x, "html.parser")
  links = soup.find_all("a")
  links = [l for l in links if re.match(r"\d+月$", l.text.strip())]
  links = [l.get("href") for l in links]
  links = [l for l in links if l is not None]
  links = [urljoin(ROOTURL, l) for l in links]
  links = list(set(links))
  logger.info("%d month urls detected", len(links))
  return links

def _find_file_link(url):
  # find target file links
  x = urlopen(url).read()
  soup = bs(x, "html.parser")
  links = soup.find_all("a")
  def _filter(link):
    text = link.text.strip()
    for key in KEYWORDS:
      if text.find(key) < 0:
        return False
    return True
  links = [l for l in links if _filter(l)]
  if len(links) == 0:
    logger.warning("Target link not found in '%s'", url)
    return None
  elif len(links) > 1:
    logger.warning("Multiple target links found in '%s'", url)
    return None
  link = links[0]
  logger.debug("Unique target link found '%s'\n-> '%s'", url, link.text.strip())
  
  def _get_info(link):
    # find (year, month, file url) for the link
    # year-month info is in the next cell, 
    # file link url is within a same line (link is to the description page)
    def _get_year_month(link):
      div = link.find_next("div")
      if div.text.find("年月"):
        r = re.search(r"(\d{4})年(\d{1,2})月", re.sub(r"\s", "", div.text))
        if r is not None:
          year, month = int(r.group(1)), int(r.group(2))
          return year, month
      logger.info("Year-month not found for '%s'", link.text)
      #raise ValueError("Year-month not found for '{}'".format(link.text))
      return None, None

    def _get_file_link(link):
      url = link
      while url is not None:
        url = url.find_next("a")
        if not url.has_attr("href"):
          continue
        url = url.get("href")
        logger.debug("Next url: '%s'", url)
        if url.find("stat-search/file-download") >= 0:
          return url
        elif url.find("stat-search/files"):
          break
      logger.info("File link not found for in '%s'", link.text)
      #raise ValueError("File link not found in '{}'".format(link.text))
      return None
    year, month = _get_year_month(link)
    url = _get_file_link(link)
    if year is None or month is None or url is None:
      return None
    return year, month, url
  info = _get_info(link)
  if info is None:
    return None
  year, month, fileurl = info
  return year, month, urljoin(url, fileurl)

def get_file_urls(month_from=(1000, 1), month_to=(9999, 12)):
  out = {}
  month_urls = _find_month_urls()
  for u in tqdm(month_urls):
    tmp = _find_file_link(u)
    if tmp is None:
      logger.warning("Target file link not found in '%s'", u)
      continue
    year, month, url = tmp
    if (year, month) >= month_from and (year, month) <= month_to:
      out[(year, month)] = url
  logger.info("%d file link detected: \n %s", len(out),
              "\n ".join("({}): '{}'".format(key, url) for key, url in out.items()))
  return out

def _filename(year, month, extension):
  return "%04d-%02d%s" % (year, month, extension)

# def _file_extension(url):
#   if url.find("fileKind=1") >= 0:
#     return ".csv"
#   elif url.find("fileKind=4") >= 0:
#     return ".xlsx"
#   elif url.find("fileKind=0") >= 0:
#     return ".xls"
#   else:
#     logger.error("File type could not be inferred: '%s'", url)
#     raise ValueError("File type could not be inferred: '{}'".format(url))

def download_spreadsheets(savedir, month_from=(1000, 1), month_to=(9999, 12), replace=False):
  urls = get_file_urls(month_from=month_from, month_to=month_to)
  downloaded = []
  for (year, month), url in tqdm(urls.items()):
    #extension = _file_extension(url)
    extension = ".xls"  # extension inferrence from url is not complete, simply save all as .xls
    savepath = os.path.join(savedir, _filename(year, month, extension))
    if (not replace) and os.path.isfile(savepath):
      logger.debug("'%s' already exists, skipped", savepath)
      continue
    urlretrieve(url, savepath)
    downloaded.append((url, savepath))
    logger.info("Downloaded '%s' -> '%s'", url, savepath)
  return downloaded