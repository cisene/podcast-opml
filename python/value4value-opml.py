#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import re
from datetime import datetime

import requests
import json

import time

from xml.etree.ElementTree import Element, SubElement, Comment

#API_ENDPOINT = 'https://podcastindex.org/api/podcasts/bytag?podcast-value'
#CONTENT_FILE = './value4value-pretty.json'

PODCASTING20_STATE = './pc20-index.json'

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


def GetURL(url):
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
    'User-Agent': 'Mozilla/5.0 (PodcastIndex.Org - OPML/@cisene@podcastindex.social) Gecko/20100101 Firefox/93.0',
  }
  
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
    result_struct['redirected-to']    = response.url
    result_struct['encoding']             = response.apparent_encoding
    result_struct['redirected']           = response.is_redirect
    result_struct['redirected-permanent'] = response.is_permanent_redirect
    result_struct['status']               = response.status_code
    result_struct['text']                 = response.text
    result_struct['text-length']          = len(response.text)
    result_struct['error']                = ''
    result_struct['error-description']    = ''
    result_struct['headers']              = response.headers
    
    result_struct['text'] = flattenResponse(result_struct['text'])

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
    #print("\tUndefined exception")
    result_struct['error'] = "UndefinedException"
    result_struct['error-description'] = str(response.status_code)
    pass
    
  return result_struct



def fetchIndex():

  dictIndex = {}

  url_max = 1000
  url_start_at = 1
  url_start_at_old = 0
  last_round = False

  while (1):
    url = f"https://podcastindex.org/api/podcasts/bytag?podcast-value&max={url_max}&start_at={url_start_at}"

    if url_start_at == url_start_at_old:
      print(f"Offset did not move {url_start_at} == {url_start_at_old} .. bailing out.")
      break

    response = GetURL(url)

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
              'xmlurl': obj['url'],
              'htmlUrl': obj['link'],
              'description': obj['description'],
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

    if last_round == True:
      break

  state_contents = json.dumps(dictIndex)
  writeFile('./pc20-index.json', state_contents)
  return dictIndex

def renderCategoriesToOPML(idx):
  if idx != None:
    for cat in idx.keys():
      category_filename = cat.lower()
      category_filename = re.sub(r"\x20", "-", str(category_filename), flags=re.IGNORECASE)

      filepath = f"./podcastindex-org-podcasting-2.0-category-{category_filename}.opml"
      opml_url = re.sub(r"^\x2e\x2f", "", str(filepath), flags=re.IGNORECASE)
      print(f"Working on {filepath}")

      feeds = idx[cat]

      stack = []
      stack.append(f"<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
      stack.append(f"<!-- Source: https://b19.se/data/{opml_url} -->")
      stack.append(f"<opml version=\"2.0\" xmlns:podcast=\"https://github.com/Podcastindex-org/podcast-namespace/blob/main/docs/1.0.md\">")
      stack.append(f"  <head>")
      stack.append(f"    <title>Podcasting 2.0 - Category '{cat}'</title>")
      stack.append(f"    <dateCreated>{formatDateString(dateNow())} +0100</dateCreated>")
      stack.append(f"    <dateModified>{formatDateString(dateNow())} +0100</dateModified>")
      stack.append(f"    <ownerName>PodcastIndex.org</ownerName>")
      stack.append(f"  </head>")
      stack.append(f"  <body>")
      stack.append(f"    <outline text=\"{cat}\">")

      for feed in feeds:
        item_stack = []

        if "type" in feed:
          if feed['type'] != None:
            item_stack.append(f"type=\"{feed['type']}\"")

        if "version" in feed:
          if feed['version'] != None:
            item_stack.append(f"version=\"{feed['version']}\"")

        if "language" in feed:
          if feed['language'] != None:
            item_language = snipLanguage(feed['language'])
            item_stack.append(f"language=\"{item_language}\"")

        if "feedGuid" in feed:
          if feed['feedGuid'] != None:
            item_stack.append(f"podcast:feedGuid=\"{feed['feedGuid']}\"")

        if "xmlurl" in feed:
          if feed['xmlurl'] != None:
            item_xmlUrl = urlEncode(feed['xmlurl'])
            item_stack.append(f"xmlUrl=\"{item_xmlUrl}\"")

        if "htmlUrl" in feed:
          if feed['htmlUrl'] != None:
            item_htmlUrl = urlEncode(feed['htmlUrl'])
            item_stack.append(f"htmlUrl=\"{item_htmlUrl}\"")

        if "title" in feed:
          if feed['title'] != None:
            item_title = htmlEncode(fullTrim(feed['title']))
            item_stack.append(f"title=\"{item_title}\"")

        if "description" in feed:
          if feed['description'] != None:
            item_description = htmlEncode(fullTrim(feed['description']))
            item_stack.append(f"description=\"{item_description}\"")

        if len(item_stack) > 0:
          item_contents = " ".join(item_stack)

          stack.append(f"      <outline {item_contents}/>")

      stack.append(f"    </outline>")
      stack.append(f"  </body>")
      stack.append(f"</opml>")


      opml_contents = "\n".join(stack)
      #print(opml_contents)

      writeFile(filepath, opml_contents)

    print("Done!")

  return

def main():


  if os.path.isfile(PODCASTING20_STATE):
    contents = LoadContents(PODCASTING20_STATE)
    pc20index = json.loads(contents)
  else:
    pc20index = fetchIndex()

  renderCategoriesToOPML(pc20index)



if __name__ == '__main__':
  main()
