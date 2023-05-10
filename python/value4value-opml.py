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

API_ENDPOINT = 'https://podcastindex.org/api/podcasts/bytag?podcast-value'
CONTENT_FILE = './value4value-pretty.json'

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
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:93.0) Gecko/20100101 Firefox/93.0',
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


def htmlEncode(data):
  data = re.sub(r"\x26", "&amp;", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x22", "&quot;", str(data), flags=re.IGNORECASE)
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


def ProcessItems(opml_data):
  config = {
    'head': {
      'title': 'PodcastIndex.org - Value4Value Enabled podcasts in categories',
      'dateCreated': '2021-11-13 20:25:03',
      'dateModified': dateNow(),
      'ownerName': 'PodcastIndex.org',
      'ownerEmail': 'info@podcastindex.org',
    }
  }

  opml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
  opml += "<!-- Source: https://b19.se/data/podcastindex-org-value4value-enabled.opml -->\n"
  opml += "<opml version=\"2.0\">\n"

  head_title = config['head']['title']
  head_dateCreated = formatDateString(config['head']['dateCreated'])
  head_dateModified = formatDateString(config['head']['dateModified'])
  ownerName = config['head']['ownerName']
  ownerEmail = config['head']['ownerEmail']

  opml += "  <head>\n"
  opml += "    <title>{0}</title>\n".format(str(head_title))
  opml += "    <dateCreated>{0} +0100</dateCreated>\n".format(str(head_dateCreated))
  opml += "    <dateModified>{0} +0100</dateModified>\n".format(str(head_dateModified))

  opml += "    <ownerName>{0}</ownerName>\n".format(str(ownerName))
  opml += "    <ownerEmail>{0}</ownerEmail>\n".format(str(ownerEmail))

  opml += "  </head>\n"

  opml += "  <body>\n"


  for category in opml_data:

    issue_title = category

    opml += "    <outline text=\"{0}\">\n".format(
        str(issue_title),
      )

    for podcast_obj in opml_data[category].items():
      #print(podcast_obj, podcast_obj[0], podcast_obj[1])
      podcast_id = podcast_obj[0]
      podcast = podcast_obj[1]
      #print(podcast)
      podcast_type = "link"
      podcast_version = "RSS"

      if 'language' not in podcast:
        podcast_language = "en"
      else:
        podcast_language = podcast['language']
        podcast_language = snipLanguage(podcast_language)

      if 'title' not in podcast:
        podcast_title = ""
      else:
        podcast_title = podcast['title']

      if 'link' not in podcast:
        podcast_htmlUrl = ""
      else:
        podcast_htmlUrl = podcast['link']

      if 'feed' not in podcast:
        podcast_xmlUrl = ""
      else:
        podcast_xmlUrl = podcast['feed']


      if len(podcast_htmlUrl) == 0:
        podcast_htmlUrl = "https://podcastindex.org/podcast/" + str(podcast_id)


      opml += "      <opml type=\"{0}\" version=\"{1}\" language=\"{2}\" title=\"{3}\" text=\"{3}\" htmlUrl=\"{4}\" xmlUrl=\"{5}\" />\n".format(
        str(podcast_type),
        str(podcast_version),
        str(podcast_language),
        htmlEncode(str(podcast_title)),
        htmlEncode(str(podcast_htmlUrl)),
        htmlEncode(str(podcast_xmlUrl))
      )


    opml += "    </outline>\n"

  opml += "  </body>\n"

  opml += "</opml>\n"

  #print(opml)

  filepath = "./podcastindex-org-value4value-enabled.opml"
  writeFile(filepath, opml)
  return

def writeFile(filepath, contents):
  f = open(filepath,"w+")
  f.write(contents)
  f.close()
  return


def fetchV4VList():
  opml_struct = {}
  response = GetURL(API_ENDPOINT)

  objects = None
  if response['status'] == 200:
    contents = response['text']
    objects = json.loads(contents)
  
  if objects != None:

    if "feeds" in objects:
      o_feeds = objects['feeds']
      for o_feed_item in o_feeds:
        item_id = None
        item_title = None
        item_link = None
        item_feed = None
        item_lang = None

        if "id" in o_feed_item:
          item_id = int(o_feed_item['id'])

        if "title" in o_feed_item:
          item_title = fullTrim(o_feed_item['title'])

        if "link" in o_feed_item:
          item_link = fullTrim(o_feed_item['link'])
          item_link = fixLink(item_link)

        if "url" in o_feed_item:
          item_feed = fullTrim(o_feed_item['url'])

        if "language" in o_feed_item:
          item_lang = fullTrim(o_feed_item['language'])

        if "categories" in o_feed_item:
          categories = o_feed_item['categories']
          if categories != None:
            if len(categories) > 0:
              for cat_key, cat_val in categories.items():
                category_caption = cat_val

                if category_caption not in opml_struct:
                  opml_struct[category_caption] = {}

                if item_id not in opml_struct[category_caption]:
                  opml_struct[category_caption][item_id] = { "title": item_title, "link": item_link, "feed": item_feed, "language": item_lang }


          else:
            category_caption = "Podcast"
            if category_caption not in opml_struct:
              opml_struct[category_caption] = {}

            if item_id not in opml_struct[category_caption]:
              opml_struct[category_caption][item_id] = { "title": item_title, "link": item_link, "feed": item_feed }

    else:
      print("No feed list in object")

  return opml_struct


def main():

  opml_data = fetchV4VList()
  ProcessItems(opml_data)


if __name__ == '__main__':
  main()
