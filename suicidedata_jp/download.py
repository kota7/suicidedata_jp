# coding: utf-8

import os
import re
from urllib.request import urlopen
from urllib.parse import urljoin
from shutil import copyfileobj
from bs4 import BeautifulSoup as bs
from tqdm import tqdm

ROOTURL = "https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/0000140901.html"
DONELIST = "_donelist.txt"


def urlretrieve(url, savepath):
  obj = urlopen(url)
  with open(savepath, "wb") as f:
    copyfileobj(obj, f)

def find_year_urls():
  x = urlopen(ROOTURL).read()
  soup = bs(x, "html.parser")
  urls = []
  for l in soup.find_all("a"):
    if l.text.find("地域における自殺の基礎資料") < 0:
      continue
    url = l.get("href")
    if url is None:
      continue
    url = urljoin(ROOTURL, url)
    urls.append(url)
  urls = list(set(urls))
  return urls

def find_month_urls(year_url):
  x = urlopen(year_url).read()
  soup = bs(x, "html.parser")
  urls = []
  for l in soup.find_all("a"):
    if re.search(r"(\d|元)年\d+月", l.text) is None:
      continue
    url = l.get("href")
    if url is None:
      continue
    url = urljoin(year_url, url)
    urls.append(url)
  urls = list(set(urls))
  return urls 

def get_filename(url):
  # todo: handle very unlikely edgecase where two different urls gets a same filename
  filename = url.replace("https://www.mhlw.go.jp/", "")
  filename = filename.replace("/", "_")
  return filename

def get_finished_urls(donelist_file):
  if not os.path.isfile(donelist_file):
    return set()
  with open(donelist_file) as f:
    donelist = set(line.strip() for line in f)
  return donelist

def download_files(urls, savedir="./raw", replace=False):
  os.makedirs(savedir, exist_ok=True)  
  donelist_file = os.path.join(savedir, DONELIST)  # record urls already downloaded
  finished_urls = get_finished_urls(donelist_file)  # this is a set
  for url in tqdm(urls):
    if not replace and url in finished_urls:
      continue
    filename = get_filename(url)
    urlretrieve(url, os.path.join(savedir, filename))
    finished_urls.add(url)
  with open(donelist_file, "w") as f:
    f.write("\n".join(finished_urls))  # save updated done list
    
def download_suicide_data(savedir="./raw", replace=False):
  year_urls = find_year_urls()
  month_urls = []
  for url in year_urls:
    month_urls += find_month_urls(url)
  download_files(month_urls, replace=False)
