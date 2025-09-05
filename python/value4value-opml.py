#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import re
from datetime import datetime

import requests
import json

import time
import hashlib


from xml.etree.ElementTree import Element, SubElement, Comment

from lxml import etree


class APIAuthorization:

  api_key = ''
  api_secret = ''
  auth_date = 0

  headers = {
    'User-Agent': 'Python-PodcastIndexLib/0.1 (@cisene)',
    'X-Auth-Date': '',
    'X-Auth-Key': '',
    'Authorization': '',
  }

  def __init__(self, api_key = '', api_secret = ''):
    pass

  def _setAuthDateNow(self):
    self.auth_date = int(time.time())

  def _epochNow(self):
    return int(time.time())

  def generateHeaders(self):
    self._setAuthDateNow()
    hash_input = "".join(
      [
        str(self.api_key),
        str(self.api_secret),
        str(self.auth_date)
      ]
    ).encode('utf-8')

    sha_1 = hashlib.sha1()
    sha_1.update(hash_input)
    hash_output = sha_1.hexdigest()

    self.headers['X-Auth-Date'] = str(self.auth_date)
    self.headers['X-Auth-Key'] = str(self.api_key)
    self.headers['Authorization'] = str(hash_output)
    return self.headers

  def refreshHeaders(self, headers):
    epoch_now = self._epochNow()

    epoch_then = int(headers['X-Auth-Date'])
    epoch_delta = (epoch_now - epoch_then)

    if epoch_delta > 120:
      return self.generateHeaders()

    return headers




USER_AGENT = 'Mozilla/5.0 (PodcastIndex.Org - OPML/@cisene@podcastindex.social)'

PODCASTING20_STATE = './pc20-index.json'

def writeOPML(filepath, contents):
  s = "\n".join(contents) + "\n"
  with open(filepath, "w") as f:
    f.write(contents)


def writeFile(filepath, contents):
  f = open(filepath,"w+")
  f.write(contents)
  f.close()
  return


def readFile(filepath):
  contents = None
  if os.path.isfile(filepath):
    try:
      fp = open(filepath)
      contents = fp.read()
    finally:
      fp.close()

  return contents


def urlEncode(data):
  data = re.sub(r"\x26", "&amp;", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x3c", "%3C", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x3e", "%3E", str(data), flags=re.IGNORECASE)
  
  return data

def htmlEncode(data):
  data = re.sub(r"\x26", "&amp;", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x22", "&quot;", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x3c", "&lt;", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x3e", "&gt;", str(data), flags=re.IGNORECASE)

  data = re.sub(r"(\r\n|\r|\n)", "<br>", str(data), flags=re.IGNORECASE)

  return data

def fullTrim(data):
  data = re.sub(r"(\r\n|\r|\n|\t)", " ", str(data), flags=re.IGNORECASE)
  data = re.sub(r"^\s{1,}", "", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\s{1,}$", "", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\s{2,}", " ", str(data), flags=re.IGNORECASE)

  return data


def formatDateString(data):
  dt = datetime.fromisoformat(data)
  datestr = dt.strftime("%a, %d %b %Y %H:%M:%S")
  return datestr

def dateNow():
  my_date = datetime.now()
  data = str(my_date.strftime('%Y-%m-%d %H:%M:%S'))
  return data


def fixLink(data):
  data = fullTrim(data)

  if len(data) == 0:
    return ""

  if not re.search(r"^([a-z]{2,8})\x3a\x2f\x2f", str(data), flags=re.IGNORECASE):
    data = "http://" + data

  if re.search(r"^http(s)?\x3a\x2f\x2f([a-z0-9\x2e]{1,})\x2e([a-z]{2,14})$", str(data), flags=re.IGNORECASE):
    data = data + "/"

  return data


def snipLanguage(data):
  if re.search(r"^([a-z]{2})$", str(data), flags=re.IGNORECASE):
    return data.lower()

  if re.search(r"^([a-z]{2})\x2d(.*)$", str(data), flags=re.IGNORECASE):
    return data[:2].lower()

  return "en"


def LoadContents(filepath):
  contents = None
  if os.path.isfile(filepath):
    try:
      fp = open(filepath)
      contents = fp.read()
    finally:
      fp.close()

  return contents


def GetURL(url, headers):
  result = ''
  response = ''
  response = None
    
  result_struct = {
    'text'                    : '',
    'text-length'             : 0,
    'encoding'                : '',
    'error'                   : '',
    'error-description'       : '',
    'headers'                 : {},
    'redirected'              : False,
    'redirected-permanent'    : False,
    'redirected-to'           : '',
    'status'                  : 0,
  }
  
  request_headers = {
    'User-Agent': None,
    'X-Auth-Date': None,
    'X-Auth-Key': None,
    'Authorization': None,
  }

  for header in headers:
    request_headers[header] = headers[header]
  
  r = requests
  try:
    response = r.get(
      url,
      params          = None,
      stream          = False,
      headers         = request_headers,
      timeout         = (5,5),
      verify          = True,
      allow_redirects = False
    )
    result_struct['redirected-to']        = response.url
    result_struct['encoding']             = response.apparent_encoding
    result_struct['redirected']           = response.is_redirect
    result_struct['redirected-permanent'] = response.is_permanent_redirect
    result_struct['status']               = response.status_code
    result_struct['text']                 = response.text
    result_struct['text-length']          = len(response.text)
    result_struct['error']                = ''
    result_struct['error-description']    = ''
    result_struct['headers']              = response.headers

  except requests.RequestException as e:
    #print "\tRequestException: " + str(e)
    result_struct['error'] = "RequestException"
    result_struct['error-description'] = str(e)
    pass

  except requests.ConnectionError as e:
    #print "\tConnectionError: " + str(e)
    result_struct['error'] = "ConnectionException"
    result_struct['error-description'] = str(e)
    pass

  except requests.HTTPError as e:
    #print "\tHTTPError: " + str(e)
    result_struct['error'] = "HTTPError"
    result_struct['error-description'] = str(e)
    pass

  except requests.URLRequired as e:
    #print "\tURLRequired: " + str(e)
    result_struct['error'] = "URLRequired"
    result_struct['error-description'] = str(e)
    pass

  except requests.TooManyRedirects as e:
    result_struct['error'] = "TooManyRedirects"
    result_struct['error-description'] = str(e)
    pass

  except requests.ConnectTimeout as e:
    result_struct['error'] = "ConnectTimeout"
    result_struct['error-description'] = str(e)
    pass

  except requests.ReadTimeout as e:
    result_struct['error'] = "ReadTimeout"
    result_struct['error-description'] = str(e)
    pass

  except requests.Timeout as e:
    result_struct['error'] = "Timeout"
    result_struct['error-description'] = str(e)
    pass

  except:
    result_struct['error'] = "UndefinedException"
    result_struct['error-description'] = str(response.status_code)
    pass
    
  return result_struct



def fetchIndex(headers):

  dictIndex = {}

  url_max = 1000
  url_start_at = 1
  url_start_at_old = 1
  last_round = False

  while (1):

    url = f"https://api.podcastindex.org/api/1.0/podcasts/bytag?podcast-value&max={url_max}&start_at={url_start_at}"

    response = GetURL(url, headers)
    print(f"{url} -> {response['status']} ..")

    if response['status'] == 200:
      contents = response['text']

      objects = None
      objects = json.loads(contents)
      if objects != None:

        if "nextStartAt" in objects:
          if objects['nextStartAt'] != None:
            url_start_at_old = url_start_at
            url_start_at = objects['nextStartAt']
        else:
          last_round = True

        feeds = None
        if "feeds" in objects:
          if objects['feeds'] != None:
            feeds = objects['feeds']

        if feeds != None:
          for obj in feeds:

            opml_item = {
              'feedGuid': obj['podcastGuid'],
              'language': obj['language'],
              'title': obj['title'],
              'text': obj['title'],
              'xmlurl': obj['url'],
              'htmlUrl': obj['link'],
              'description': obj['description'],
              'image': obj['image'],
              'type': 'link',
              'version': 'RSS',
            }

            if "categories" in obj:
              if obj['categories'] != None:
                for cat in obj['categories']:
                  category_name = obj['categories'][cat]

                  if category_name not in dictIndex:
                    dictIndex[category_name] = []

                  if opml_item not in dictIndex[category_name]:
                    dictIndex[category_name].append(opml_item)

      else:
        print(f"Response was empty, bailing out")
        break

    else:
      print(f"Unexpected response, bailing out")
      break


    if last_round == True:
      break

  state_contents = json.dumps(dictIndex)
  writeFile(PODCASTING20_STATE, state_contents)
  return dictIndex

def renderCategoriesToOPML(idx):
  if idx != None:
    for cat in idx.keys():
      category_filename = cat.lower()
      category_filename = re.sub(r"\x20", "-", str(category_filename), flags=re.IGNORECASE)

      filepath = f"./podcastindex-org-Value4Value-category-{category_filename}.opml"
      opml_url = re.sub(r"^\x2e\x2f", "", str(filepath), flags=re.IGNORECASE)
      print(f"Working on {filepath}")

      feeds = idx[cat]

      #stack = []
      #stack.append(f"<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
      #stack.append(f"<!-- Source: https://b19.se/data/opml/podcastindex/{opml_url} -->")
      #stack.append(f"<opml version=\"2.0\" xmlns:podcast=\"https://github.com/Podcastindex-org/podcast-namespace/blob/main/docs/1.0.md\">")
      #stack.append(f"  <head>")
      #stack.append(f"    <title>Podcasting 2.0 - Value4Value Category '{cat}'</title>")
      #stack.append(f"    <dateCreated>{formatDateString(dateNow())} +0100</dateCreated>")
      #stack.append(f"    <dateModified>{formatDateString(dateNow())} +0100</dateModified>")
      #stack.append(f"    <ownerName>PodcastIndex.org</ownerName>")
      #stack.append(f"  </head>")
      #stack.append(f"  <body>")
      #stack.append(f"    <outline text=\"{cat}\">")

      # Open OPML
      opml = etree.Element("opml", version = "2.0")

      # Open Head
      head = etree.SubElement(opml, "head")

      # Handle Title
      title_text = f"Podcasting 2.0 - Value4Value Category '{cat}'"
      title = etree.Element("title")
      title.text = str(title_text)
      head.append(title)

      # Handle dateCreated
      datecreated_text = f"{formatDateString(dateNow())} +0100"
      dateCreated = etree.Element("dateCreated")
      dateCreated.text = str(datecreated_text)
      head.append(dateCreated)

      # Handle dateCreated
      datemodified_text = f"{formatDateString(dateNow())} +0100"
      dateModified = etree.Element("dateModified")
      dateModifies.text = str(datemodified_text)
      head.append(dateModified)

      # Handle ownerName
      ownerName_text = f"{formatDateString(dateNow())} +0100"
      ownerName = etree.Element("ownerName")
      ownerName.text = "PodcastIndex.org"
      head.append(ownerName)

      # Close Head
      opml.append(head)

      # Drop source comment
      comment_text = f" Source: https://b19.se/data/opml/podcastindex/{opml_url} "
      comment = etree.Comment(comment_text)
      opml.append(comment)

      # Open body
      body = etree.Element("body")

      outline_cat = etree.Element("outline")
      outline_cat.set("text", str(cat))

      for feed in feeds:
        item_stack = []

        outline_item = etree.Element("outline")

        if "type" in feed:
          if feed['type'] != None:
            outline_item.set("type", "link")

        if "version" in feed:
          if feed['version'] != None:
            outline_item.set("version", "RSS")

        if "language" in feed:
          if feed['language'] != None:
            item_language = snipLanguage(feed['language'])
            outline_item.set("language", str(item_language))

        if "xmlurl" in feed:
          if feed['xmlurl'] != None:
            item_xmlUrl = urlEncode(feed['xmlurl'])
            outline_item.set("xmlUrl", str(item_xmlUrl))

        if "htmlUrl" in feed:
          if feed['htmlUrl'] != None:
            item_htmlUrl = urlEncode(feed['htmlUrl'])
            outline_item.set("htmlUrl", str(item_htmlUrl))

        if "image" in feed:
          if feed['image'] != None:
            item_imgUrl = urlEncode(feed['image'])
            outline_item.set("image", str(item_imgUrl))

        if "title" in feed:
          if feed['title'] != None:
            item_title = htmlEncode(fullTrim(feed['title']))
            outline_item.set("title", str(item_title))

        if "text" in feed:
          if feed['text'] != None:
            item_text = htmlEncode(fullTrim(feed['text']))
            outline_item.set("text", str(item_text))

        outline_cat.append(outline_item)

      # Add outlines to body
      body.append(outline_cat)

      # Close body
      opml.append(body)

      opml_contents = etree.tostring(opml, pretty_print=True, xml_declaration=True, encoding='UTF-8').decode()
      writeOPML(filepath, opml_contents)

    print("Done!")

  return

def main():

  api_key = os.environ["API_KEY"]
  api_secret = os.environ["API_SECRET"]

  auth = APIAuthorization()
  auth.api_key = api_key
  auth.api_secret = api_secret
  auth.headers['User-Agent'] = USER_AGENT

  headers = auth.generateHeaders()


  #if os.path.isfile(PODCASTING20_STATE):
  #  contents = LoadContents(PODCASTING20_STATE)
  #  pc20index = json.loads(contents)
  #else:
  pc20index = fetchIndex(headers)

  renderCategoriesToOPML(pc20index)



if __name__ == '__main__':
  main()
