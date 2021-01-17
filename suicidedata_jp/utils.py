# coding: utf-8

import os
from shutil import copyfileobj
from urllib.request import urlopen

def urlretrieve(url, savepath):
  os.makedirs(os.path.dirname(savepath), exist_ok=True)
  obj = urlopen(url)
  with open(savepath, "wb") as f:
    copyfileobj(obj, f)
