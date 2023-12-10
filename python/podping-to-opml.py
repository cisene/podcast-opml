#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import re
from datetime import datetime

import time
import hashlib

import argparse

import redis
import json
import requests

# MySQL related libraries
import mysql.connector



APP_NAME = "Podping-to-OPML"
APP_VERSION = "0.0.1"
APP_AUTHOR = "@cisene@podcastindex.social"

global conn

global cur_channel_read
global cur_channel_write

global auth

class APIAuthorization:

  api_key = ''
  api_secret = ''
  auth_date = 0

  headers = {
    'User-Agent': f"{APP_NAME}/{APP_VERSION} ({APP_AUTHOR})",
    'X-Auth-Date': '',
    'X-Auth-Key': '',
    'Authorization': '',
  }

  def __init__(self, api_key = '', api_secret = ''):
    self.api_key = api_key
    self.api_secret = api_secret

  def _setAuthDateNow(self):
    self.auth_date = int(time.time())

  def _epochNow(self):
    return int(time.time())

  def generateHeaders(self):
    if int(time.time()) == self.auth_date:
      return self.headers

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

def writeFile(filepath, contents):
  f = open(filepath,"w+")
  f.write(contents)
  f.close()
  return


def urlEncode(data):

  data = re.sub(r"\x25", "%25", str(data), flags=re.IGNORECASE)
  
  data = re.sub(r"\x20", "%20", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x21", "%21", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x22", "%22", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x23", "%23", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x24", "%24", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x26", "%26", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x27", "%27", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x28", "%28", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x29", "%29", str(data), flags=re.IGNORECASE)

  data = re.sub(r"\x2b", "%2B", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x2c", "%2C", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x2d", "%2D", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x2e", "%2E", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x2f", "%2F", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x3a", "%3A", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x3d", "%3D", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x3f", "%3F", str(data), flags=re.IGNORECASE)

  data = re.sub(r"\x5f", "%5F", str(data), flags=re.IGNORECASE)

  return data

def htmlEncode(data):
  data = re.sub(r"\x3c(\x2f)?.+?(\x2f)?\x3e", "", str(data), flags=re.IGNORECASE)

  data = re.sub(r"\x26", "&amp;", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x22", "&quot;", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x27", "&apos;", str(data), flags=re.IGNORECASE)

  data = re.sub(r"\x3c", "&lt;", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x3e", "&gt;", str(data), flags=re.IGNORECASE)

  data = re.sub(r"(\r\n|\r|\n)", " ", str(data), flags=re.IGNORECASE)

  data = re.sub(r"\t", " ", str(data), flags=re.IGNORECASE)

  data = re.sub(r"^\s{1,}", "", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\s{1,}$", "", str(data), flags=re.IGNORECASE)
  data = re.sub(r"\s{2,}", " ", str(data), flags=re.IGNORECASE)

  return data


def remove_html_tags(text):
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

def forceISO639(data):
  data = re.sub(r"^([a-z]{2})(.*)$", "\\1", str(data), flags=re.IGNORECASE)
  return data


def doHTTPGET(url, headers):
  result = ''
  response = None
    
  result_struct = {
    'encoding'                : '',
    'redirected'              : False,
    'redirected-permanent'    : False,
    'redirected-to'           : '',
    'status'                  : 0,
    'text'                    : '',
    'text-length'             : 0,
    'error'                   : '',
    'error-description'       : '',
    'headers'                 : {},
    'request_headers'         : {},
  }
  
  proxies = {}

  request_headers = {
    'User-Agent': str(headers['User-Agent']),
    'X-Auth-Date': str(headers['X-Auth-Date']),
    'X-Auth-Key': str(headers['X-Auth-Key']),
    'Authorization': str(headers['Authorization']),
  }

  r = requests
  try:
    response = r.get(
      url,
      params          = None,
      stream          = False,
      headers         = request_headers,
      timeout         = (15,15),
      verify          = True,
      allow_redirects = True,
      proxies         = proxies
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
    result_struct['request_headers']      = request_headers

  except requests.RequestException as e:
    result_struct['error'] = "RequestException"
    result_struct['error-description'] = str(e)
    pass

  except requests.ConnectionError as e:
    result_struct['error'] = "ConnectionException"
    result_struct['error-description'] = str(e)
    pass

  except requests.HTTPError as e:
    result_struct['error'] = "HTTPError"
    result_struct['error-description'] = str(e)
    pass

  except requests.URLRequired as e:
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
    pass
    
  return result_struct

def formatDateString(data):
  dt = datetime.fromisoformat(data)
  datestr = dt.strftime("%a, %d %b %Y %H:%M:%S")
  return datestr


def GetKeyToday():
  today_date = datetime.utcnow().strftime('%Y%m%d')
  result = f"podmix_hive_{today_date}"
  result = f"podmix_hive_{today_date}_test"
  return result

def GetRedisKeys():
  result = []
  key_today = GetKeyToday()
  scan_keys = redisClient.keys()
  for key_name in scan_keys:
    kn = key_name.decode('UTF-8')
    if re.search(r"podmix\x5fhive\x5f(\d{8})\x5ftest$", str(kn), flags=re.IGNORECASE):
      if kn != key_today:
        if kn not in result:
          result.append(kn)

  result.sort()
  return result

def GetRedisKeyDate(redis_key):
  result = None
  parts = re.split(r"\x5f", str(redis_key), flags=re.IGNORECASE)
  if len(parts) > 1:
    for elem in parts:
      if re.search(r"^(\d{8})$", str(elem), flags=re.IGNORECASE):
        result = re.sub(r"^(\d{4})(\d{2})(\d{2})$", "\\1-\\2-\\3", str(elem), flags=re.IGNORECASE)
        break

  return result


def expandObjectsToOPML(objects):
  stack = []
  stack.append(f"<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
  stack.append(f"<opml version=\"2.0\" xmlns:podcast=\"https://github.com/Podcastindex-org/podcast-namespace/blob/main/docs/1.0.md\">")
  stack.append(f"  <head>")
  stack.append(f"    <title>{htmlEncode(objects['head']['title'])}</title>")
  stack.append(f"    <dateCreated>{objects['head']['dateCreated']}</dateCreated>")
  stack.append(f"    <dateModified>{objects['head']['dateModified']}</dateModified>")
  stack.append(f"    <ownerName>{htmlEncode(objects['head']['ownerName'])}</ownerName>")
  stack.append(f"  </head>")
  stack.append(f"  <body>")
  #stack.append(f"    <outline>")

  for ol in objects['body']:
    inner_stack = []
    inner_stack.append("<outline")
    for k in ol.keys():
      inner_stack.append(f"{k}=\"{htmlEncode(ol[k])}\"")

    inner_stack.append("/>")
    ol_line = " ".join(inner_stack)
    #stack.append(f"      {ol_line}")
    stack.append(f"    {ol_line}")

  #stack.append(f"    </outline>")
  stack.append(f"  </body>")
  stack.append(f"</opml>")

  return "\n".join(stack)


def processQueue():

  redis_keys = GetRedisKeys()
  for redis_key in redis_keys:
    redis_key_date = GetRedisKeyDate(redis_key)
    print(redis_key, redis_key_date)

    opml_filename = f"podping-{redis_key_date}.opml"

    collection = []
    
    line_count = 0
    while (int(redisClient.llen(redis_key)) > 0):
      obj = redisClient.rpop(redis_key)
      if obj != None:
        item = json.loads(str(obj.decode('UTF-8')))

        if "url" in item:
          if item['url'] != None:
            if re.search(r"^http(s)?\x3a\x2f\x2f", str(item['url']), flags=re.IGNORECASE):
              if item['url'] not in collection:
                collection.append(item['url'])
                #print(item['url'])
                line_count += 1

                if (line_count != 0 and (line_count % 1000) == 0):
                  print(f"\t{line_count}")

    print(f"\t{line_count} read ")

    headers = auth.generateHeaders()

    objects = {
      'head': {
        'title': f"Podcasting 2.0 - Podpings of {redis_key_date}",
        'dateCreated': f"{formatDateString(redis_key_date)} +0000",
        'dateModified': f"{formatDateString(redis_key_date)} +0000",
        'ownerName': f"PodcastIndex.org",
      },
      'body': []
    }

    r_count = 0
    r_max = len(collection)
    for obj in collection:
      query = f"SELECT c.channel_id, c.channel_title, c.channel_link, c.channel_language, c.channel_description, g.channel_guid FROM podmix.channels c LEFT JOIN podmix.podcastindex_guid g ON g.channel_id = c.channel_id WHERE c.channel_feed_link = '{obj}';"
      #print(query)
      cur_channel_read.execute(query)
      for (channel_id, channel_title, channel_link, channel_language, channel_description, channel_guid) in cur_channel_read:
        channel_feed_link = obj

        channel_medium = "podcast"

        if channel_title == None:
          if obj != None:
            redisClient.lpush('podmix_validate', str(obj))
          continue

        if channel_link == None:
          channel_link = ""

        if channel_language == None:
          channel_language = "en"

        if channel_description == None:
          channel_description = ""

        if channel_guid == None or channel_guid == "":
          api_url = f"https://api.podcastindex.org/api/1.0/podcasts/byfeedurl?url={urlEncode(obj)}"
          
          headers = auth.generateHeaders()
          res = doHTTPGET(api_url, headers)

          r_content_type = None
          if res['status'] == 200:
            if "headers" in res:
              if res['headers'] != None:
                if "Content-Type" in res['headers']:
                  r_content_type = res['headers']['Content-Type']
            
            if r_content_type == 'application/json':
              r_obj = json.loads(res['text'])
              r_feed = r_obj['feed']

              if "podcastGuid" in r_feed:
                if r_feed['podcastGuid'] != None:
                  r_guid = r_feed['podcastGuid']
                  channel_guid = r_feed['podcastGuid']
                  channel_title = r_feed['title']
                  channel_link = r_feed['link']
                  channel_description = r_feed['description']
                  channel_medium = r_feed['medium']

                  query = f"INSERT INTO podmix.podcastindex_guid (channel_id, channel_guid) VALUES({channel_id}, '{r_guid}') ON DUPLICATE KEY UPDATE channel_guid = '{r_guid}';"
                  #print(query)
                  cur_channel_write.execute(query)

          conn.commit()

        if channel_guid == None:
          if obj != None:
            redisClient.lpush('podmix_validate', str(obj))
            r_max -= 1
          continue

        if re.search(r"^none$", channel_guid, flags=re.IGNORECASE):
          r_max -= 1
          continue

        if channel_language == "":
          channel_language = "en"

        item = {
          'type':             'link',
          'version':          'RSS',
          'language':         f"{forceISO639(channel_language)}",
          'podcast:feedGuid': f"{channel_guid}",
          'podcast:medium':   f"{channel_medium}",
          'xmlUrl':           f"{channel_feed_link}",
          'htmlUrl':          f"{channel_link}",
          'title':            f"{channel_title}",
          'description':      f"{channel_description}",
        }

        objects['body'].append(item)
        r_count += 1

        if ((r_count % 500) == 0) :
          r_percent = ((r_count / r_max) * 100)
          print(f"\t{r_count} of {r_max}, {round(r_percent, 2)}%")

    opml = expandObjectsToOPML(objects)

    writeFile(opml_filename, opml)
    print(f"\t\tWrote to file {opml_filename} ..")




def connectMySQL(db_host, db_port, db_database, db_username, db_password):
  global conn

  #global cur_channel
  global cur_channel_read
  global cur_channel_write
 
  #while True:
  #  try:
  conn = mysql.connector.connect(
    user=db_username,
    password=db_password,
    host=db_host,
    database=db_database,
    charset='utf8',
    use_unicode=True,
    auth_plugin='mysql_native_password'
  )

  cur_channel_read = conn.cursor(buffered=True)
  cur_channel_write = conn.cursor(buffered=True)

  cur_channel_read.execute('SET NAMES utf8mb4')
  cur_channel_read.execute("SET CHARACTER SET utf8mb4")
  cur_channel_read.execute("SET character_set_connection=utf8mb4")
  cur_channel_read.execute("SET autocommit=0;")

  cur_channel_write.execute('SET NAMES utf8mb4')
  cur_channel_write.execute("SET CHARACTER SET utf8mb4")
  cur_channel_write.execute("SET character_set_connection=utf8mb4")
  cur_channel_write.execute("SET autocommit=0;")

  return

def connectRedis(redis_host, redis_port):
  global redisClient
  redisClient = redis.StrictRedis(host=redis_host, port=redis_port, db=0)

  return

def main():
  global conn
  global redisClient
  global auth

  print(f"{APP_NAME} Version {APP_VERSION}")

  db_host = None
  if "DB_HOST" in os.environ:
    db_host = os.environ['DB_HOST']

  db_port = None
  if "DB_PORT" in os.environ:
    db_port = os.environ['DB_PORT']

  db_database = None
  if "DB_DATABASE" in os.environ:
    db_database = os.environ['DB_DATABASE']
  
  db_username = None
  if "DB_USERNAME" in os.environ:
    db_username = os.environ['DB_USERNAME']
  
  db_password = None
  if "DB_PASSWORD" in os.environ:
    db_password = os.environ['DB_PASSWORD']

  if(
    db_host is not None and
    db_port is not None and
    db_database is not None and
    db_username is not None and
    db_password is not None
  ):

    connectMySQL(db_host, db_port, db_database, db_username, db_password)
    if conn is not None:
      print(f"\t* MySQL client happily connected")
  else:
    print("** No MySQL credentials found")
    exit(1)



  redis_host = None
  if "REDIS_HOST" in os.environ:
    redis_host = os.environ['REDIS_HOST']

  redis_port = None
  if "REDIS_PORT" in os.environ:
    redis_port = os.environ['REDIS_PORT']

  if(
      redis_host is not None and
      redis_port is not None
  ):
    connectRedis(redis_host, redis_port)
    if redisClient is not None:
      print(f"\t* Redis client happily connected")
  else:
    print("** No REDIS credentias found")
    exit(1)


  api_key = None
  if "API_KEY" in os.environ:
    api_key = os.environ["API_KEY"]

  api_secret = None
  if "API_SECRET" in os.environ:
    api_secret = os.environ["API_SECRET"]

  if(
    api_key is not None and
    api_secret is not None
  ):
    auth = APIAuthorization(api_key, api_secret)

    print(f"\t* API credentials happily found")
  else:
    print(f"** NO API credentials found")
    exit(1)

  processQueue()


if __name__ == '__main__':
  main()
