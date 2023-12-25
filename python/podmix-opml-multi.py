#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import mysql.connector
import re
from datetime import datetime

import yaml

import argparse

# Main
global conn
global cur

global cur_channel
global cur_item
global cur_update

global config

global languageLookup


VERSION = "Podmix-OPML/0.4.4-dev"

# OPML 1.0
OPML_1_START = """<?xml version="1.0" encoding="utf-8"?>
<!-- Created with """ + VERSION + """ -->
<!-- Source: https://b19.se/data/opml/podcastindex/%(filename) -->
<opml version="1.0">
  <head>
    <title>%(title)</title>
    <dateModified>%(dateModified)</dateModified>
  </head>
  <body>"""

OPML_1_END = """  </body>
</opml>"""

OPML_1_OUTLINE_FEED = '<outline type="rss" title="%(title)" text="%(title)" xmlUrl="%(xml_url)" htmlUrl="%(html_url)"/>'


# OPML 2.0
OPML_2_START = """<?xml version="1.0" encoding="UTF-8"?>
<!-- Created with """ + VERSION + """ -->
<!-- Source: https://b19.se/data/opml/podcastindex/%(filename) -->
<!-- Items: %(items) -->
<opml version="2.0">
  <head>
    <title>%(title)</title>
    <dateModified>%(dateModified) +0000</dateModified>
  </head>
  <body>"""
OPML_2_END = """  </body>
</opml>"""

# OPML_2_OUTLINE_FEED = '<outline type="Podcast" version="RSS" language="%(language)" title="%(title)" htmlUrl="%(html_url)" xmlUrl="%(xml_url)" />'
# OPML_2_OUTLINE_FEED = '<outline type="Podcast" version="RSS" language="%(language)" title="%(title)" text="%(title)" htmlUrl="%(html_url)" xmlUrl="%(xml_url)" />'
#OPML_2_OUTLINE_FEED = '<outline type="link" version="RSS" language="%(language)" title="%(title)" text="%(title)" htmlUrl="%(html_url)" xmlUrl="%(xml_url)" />'
OPML_2_OUTLINE_FEED = '<outline type="rss" version="RSS2" language="%(language)" title="%(title)" text="%(title)" htmlUrl="%(html_url)" xmlUrl="%(xml_url)" />'

OPML_2_OUTLINE_LANGUAGE_START = '<outline text="%(language)">'
OPML_2_OUTLINE_LANGUAGE_END = '</outline>'

OPML_2_INDENT_SPACE = '  '

def getOPMLFooter():
  data = OPML_2_END
  return data + "\n"
  
def getOPMLHeader(filename, title, items):
  data = OPML_2_START
  data = re.sub(r"\x25\x28filename\x29", str(filename), data, re.IGNORECASE)
  data = re.sub(r"\x25\x28title\x29", str(escapeHTML(title)), data, re.IGNORECASE)

  data = re.sub(r"\x25\x28dateModified\x29", str(getUTCNow()), data, re.IGNORECASE)
  data = re.sub(r"\x25\x28items\x29", str(items), data, re.IGNORECASE)
  return data + "\n"

def getOPML(title,feed_link,link,language):
  data = OPML_2_OUTLINE_FEED

  if language is None:
    language = "en"

  if language == "":
    language = "en"

  title = escapeHTML(title)
  language = escapeHTML(language)
  feed_link = escapeURL(feed_link)
  link = escapeURL(link)


  data = re.sub(r"\x25\x28title\x29", str(title), str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x25\x28html\x5furl\x29", str(link), str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x25\x28xml\x5furl\x29", str(feed_link), str(data), flags=re.IGNORECASE)
  data = re.sub(r"\x25\x28language\x29", str(language), str(data), flags=re.IGNORECASE)

  # Remove empty HTML URL attributes
  data = re.sub(r"\shtmlUrl\x3d\x22\x22", "", str(data), flags=re.IGNORECASE)

  return data

def getOPMLSectionLanguageEnd():
  return OPML_2_OUTLINE_LANGUAGE_END


def getOPMLSectionLanguageStart(language):
  if language is None:
    language = "en"

  if language == "":
    language = "en"

  caption = LookupLanguage(language)
  if caption == "":
    caption = "-{0}-".format(str(language))

  caption = escapeHTML(caption)

  data = OPML_2_OUTLINE_LANGUAGE_START
  data = re.sub(r"\x25\x28language\x29", str(caption), str(data), flags=re.IGNORECASE)
  return data  


def getOPMLSectionLanguageCaptionStart(caption):
  caption = escapeHTML(caption)
  data = OPML_2_OUTLINE_LANGUAGE_START
  data = re.sub(r"\x25\x28language\x29", str(caption), str(data), flags=re.IGNORECASE)
  return data  


def LookupLanguage(language):
  global languageLookup
  result = ""

  if str(language) in languageLookup:
    result = languageLookup[str(language)]

  return result


def LoadYamlConfig(filepath):
  global config
  if os.path.isfile(filepath):
    with open(filepath, 'r') as stream:
      try:
        config = yaml.safe_load(stream)
      except yaml.YAMLError as exc:
        print(exc)
  else:
    print(f"{filepath} was not found")

def ProcessItems(outputfolder):
  global config
  #print(config)
  for item in config['items']:
    do_language_sections = False
    indent_regular = 2

    filename = item['item']['filename']
    title = item['item']['title']
    # sqlfilter = item['item']['filter']
    sort_domain = str(item['item']['domain'])

    language = None
    language_id = None

    if "section_language" in item['item']:
      if item['item']['section_language'] == True:
        do_language_sections = True

        language = None
        language_id = None

    if "language" in item['item']:
      if item['item']['language'] != None:
        language = item['item']['language']

    if sort_domain == "":
      continue

    if language != None:
      query = f"SELECT language_id FROM podmix.languages WHERE language_id >= 10000 AND language_code = '{language}' LIMIT 1;"
      #print(query)

      cur_channel.execute(query)
      rowcount = cur_channel.rowcount
      if int(rowcount) > 0:
        for (language_id) in cur_channel:
          language_id = language_id[0]

    # Translate list into variable for IN() statement
    sort_domain = re.sub(r"\x2c", "','", str(sort_domain), re.IGNORECASE)

    output = ''
    section_old = ''
    line_count = 0

    query = "" \
      "SELECT" \
      "    c.channel_title, " \
      "    c.channel_feed_link, " \
      "    c.channel_link, " \
      "    LOWER(CASE WHEN '' THEN 'en' ELSE LEFT(c.channel_language,2) END) AS channel_language, " \
      "    CASE WHEN (LENGTH(l.native_caption) = 0 OR l.native_caption = l.language_caption) THEN l.language_caption ELSE CONCAT(l.native_caption, ' (', l.language_caption, ')') END AS caption " \
      "FROM " \
      "    podmix.channels c " \
      "LEFT JOIN podmix.languages l ON l.language_id = c.channel_language_id " \
      "WHERE " \
      "    channel_domain IN ('" + sort_domain + "') " \
      "AND " \
      "    l.language_id IS NOT NULL " \
      "AND " \
      "    (c.channel_language_id >= 10000 OR c.channel_language_id = 0) " \
      "AND " \
      "    c.channel_deleted = 0 " \
      "AND " \
      "    c.channel_title NOT IN ('','None') " \
      "AND " \
      "    c.channel_touch > 0 " \
      "AND " \
      "    c.channel_feed_link LIKE 'http%' " \
      "ORDER BY " \
      "    c.channel_language ASC, " \
      "    c.channel_title ASC " \
      "LIMIT 100000; "

    if language_id == None:
      query = f"SELECT c.channel_title, c.channel_feed_link, c.channel_link, LOWER(CASE WHEN '' THEN 'en' ELSE LEFT(c.channel_language,2) END) AS channel_language, CASE WHEN (LENGTH(l.native_caption) = 0 OR l.native_caption = l.language_caption) THEN l.language_caption ELSE CONCAT(l.native_caption, ' (', l.language_caption, ')') END AS caption FROM podmix.channels c LEFT JOIN podmix.languages l ON l.language_id = c.channel_language_id WHERE channel_domain IN ('{sort_domain}') AND l.language_id IS NOT NULL AND (c.channel_language_id >= 10000 OR c.channel_language_id = 0) AND c.channel_deleted = 0 AND c.channel_title NOT IN ('','None') AND c.channel_touch > 0 AND c.channel_feed_link LIKE 'http%' ORDER BY c.channel_language ASC, c.channel_title ASC LIMIT 100000;"

    else:
      query = f"SELECT c.channel_title, c.channel_feed_link, c.channel_link, LOWER(CASE WHEN '' THEN 'en' ELSE LEFT(c.channel_language,2) END) AS channel_language, CASE WHEN (LENGTH(l.native_caption) = 0 OR l.native_caption = l.language_caption) THEN l.language_caption ELSE CONCAT(l.native_caption, ' (', l.language_caption, ')') END AS caption FROM podmix.channels c LEFT JOIN podmix.languages l ON l.language_id = c.channel_language_id WHERE c.channel_deleted = 0 AND c.channel_touch > 0 AND l.language_id IS NOT NULL AND channel_domain IN ('{sort_domain}') AND c.channel_language_id = {language_id} AND c.channel_title NOT IN ('','None') AND c.channel_feed_link LIKE 'http%' ORDER BY c.channel_language ASC, c.channel_title ASC LIMIT 100000;"

    #print(query)

    cur_channel.execute(query)
    rowcount = cur_channel.rowcount
    if int(rowcount) > 0:
      output += getOPMLHeader(filename, title, rowcount)

      in_language_section = False
      language_old = ""


      for (channel_title, channel_feed_link, channel_link, channel_language, caption) in cur_channel:

        if in_language_section == True:
          section_indent = OPML_2_INDENT_SPACE * 2
        else:
          section_indent = OPML_2_INDENT_SPACE * 2


        if do_language_sections == True:
          
          if (
            channel_language != language_old
          ):
            language_old = channel_language

            if in_language_section == False:
              #output += section_indent + getOPMLSectionLanguageStart(channel_language) + "\n"
              output += section_indent + getOPMLSectionLanguageCaptionStart(caption) + "\n"
              in_language_section = True
            else:
              output += section_indent + getOPMLSectionLanguageEnd() + "\n"
              #output += section_indent + getOPMLSectionLanguageStart(channel_language) + "\n"
              output += section_indent + getOPMLSectionLanguageCaptionStart(caption) + "\n"
              in_language_section = True


        if in_language_section == True:
          regular_indent = OPML_2_INDENT_SPACE * 3
        else:
          regular_indent = OPML_2_INDENT_SPACE * 2

        output += regular_indent + getOPML(channel_title,channel_feed_link,channel_link,channel_language) + "\n"
        line_count += 1

      if do_language_sections == True:
        if in_language_section == True:
          output += section_indent + getOPMLSectionLanguageEnd() + "\n"
      
      output += getOPMLFooter()

      if line_count > 0:
        output_file = outputfolder + '/' + filename
        f = open(output_file,"w+")
        f.write(output)
        f.close()
        print("\tWriting file '{0}' - {1} rows".format(str(filename), str(rowcount)))
      else:
        print("\tWriting file '{0}' - skipped, zero rows found".format(str(filename)))
        continue
    else:
      print("\tFile '{0}' returns zero rows -- ".format(str(filename)))
      continue


def ClearFolder(folderpath):
  filelist = os.listdir(folderpath)
  filelist.sort()

  for filename in filelist:

    if filename.endswith(".opml"):
      os.remove(os.path.join(folderpath, filename))
      print("\tRemoving stale file '" + filename + "'")

  return


def getUTCNow():
  return datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S')

def escapeHTML(data):
  data = re.sub(r"\b\x26\b",  "&amp;",    data, flags=re.UNICODE)
  data = re.sub(r"\B\x26\B",  "&amp;",    data, flags=re.UNICODE)
  
  data = re.sub(r"\x22",  "&quot;",  data, flags=re.UNICODE)
  data = re.sub(r"\x27",  "&apos;",    data, flags=re.UNICODE)
  data = re.sub(r"\x27",  "&apos;",    data, flags=re.UNICODE)

  data = re.sub(r"\xc5\x93", "&#339;", data, flags=re.IGNORECASE)
  data = re.sub(r"\x26oelig\x3b", "&#339;", data, flags=re.IGNORECASE)

  data = re.sub(r"\x3c",  "&lt;", data, flags=re.UNICODE)
  data = re.sub(r"\x3e",  "&gt;", data, flags=re.UNICODE)
  # data = re.sub(r"\xa9",  "&copy;", data, flags=re.UNICODE)
  
  data = re.sub(r"\x26(?!(?:amp|gt|lt|aacute|acirc|aelig|agrave|amp|apos|aring|atilde|auml|bull|ccedil|copy|dagger|deg|eacute|ecirc|egrave|eth|euml|euro|hellip|iacute|icirc|iexcl|igrave|iquest|iuml|laquo|mdash|micro|middot|nbsp|ndash|ntilde|oacute|ocirc|oelig|ograve|ordf|ordm|oslash|otilde|ouml|permil|pound|raquo|rsquo|reg|szlig|thorn|trade|uacute|ucirc|ugrave|uuml|yacute|yuml|\x23\d{1,})\x3b)", "&amp;", str(data), flags=re.IGNORECASE)

  data = re.sub(r"\x26apos\x3b\x26apos\x3b", "&apos;", data, flags=re.IGNORECASE)

  data = re.sub(r"\x26amp\x3bquot\x3b", "&quot;", data, flags=re.IGNORECASE)
  data = re.sub(r"\x26amp\x3boelig\x3b", "&#339;", data, flags=re.IGNORECASE)

  return data

def escapeURL(data):
  data = re.sub(r"\x22", "%22", data, re.UNICODE)
  data = re.sub(r"\x26", "&amp;", data, re.UNICODE)
  data = re.sub(r"\x27", "%27", data, re.UNICODE)
  return data


def connectMySQL(db_host, db_port, db_database, db_username, db_password):
  global conn
  global cur
  #global cur_write
  global cur_channel
  global cur_item
  global cur_update

  #if conn != None:
  #  return

  conn = None
  while True:
    try:
      conn = mysql.connector.connect(
        user=db_username,
        password=db_password,
        host=db_host,
        database=db_database,
        charset='utf8',
        use_unicode=True,
        auth_plugin='mysql_native_password'
      )
      break


    except mysql.connector.errors.OperationalError:
      print("MySQL connection attempt failed, waiting ...")
      time.sleep(5)
      pass

    except mysql.connector.errors.DatabaseError as e:
      print("MySQL Database Error: '{0}'".format(str(e)))
      time.sleep(5)
      pass

    except mysql.connector.errors.InterfaceError as e:
      print("MySQL Database Error: '{0}'".format(str(e)))
      time.sleep(5)
      pass

    finally:
      if conn != None:
        break

  cur_channel = conn.cursor(buffered=True)
  cur_item = conn.cursor(buffered=True)
  cur_update = conn.cursor(buffered=True)
  cur = conn.cursor()
  
  cur.execute('SET NAMES utf8mb4')
  cur.execute("SET CHARACTER SET utf8mb4")
  cur.execute("SET character_set_connection=utf8mb4")

  cur_channel.execute('SET NAMES utf8mb4')
  cur_channel.execute("SET CHARACTER SET utf8mb4")
  cur_channel.execute("SET character_set_connection=utf8mb4")

  cur_item.execute('SET NAMES utf8mb4')
  cur_item.execute("SET CHARACTER SET utf8mb4")
  cur_item.execute("SET character_set_connection=utf8mb4")

  cur_update.execute('SET NAMES utf8mb4')
  cur_update.execute("SET CHARACTER SET utf8mb4")
  cur_update.execute("SET character_set_connection=utf8mb4")


def init_argparse() -> argparse.ArgumentParser:
  parser = argparse.ArgumentParser(
    usage="%(prog)s --db-host [OPTION] --db-user [OPTION] --db-password [OPTION]  --db-database [OPTION] ...",
    description="Export OPML based on definitions."
  )

  parser.add_argument(
    "--db-host", action='store', dest='db_host'
  )

  parser.add_argument(
    "--db-user", action='store', dest='db_user'
  )

  parser.add_argument(
    "--db-password", action='store', dest='db_password'
  )

  parser.add_argument(
    "--db-database", action='store', dest='db_database'
  )

  return parser


def main() -> None:
  global languageLookup

  parser = init_argparse()
  args = parser.parse_args()

  if(
    (args.db_host) and
    (args.db_user) and
    (args.db_password) and
    (args.db_database)
  ):
    print(f"host     : {args.db_host}")
    print(f"user     : {args.db_user}")
    print(f"password : {args.db_password}")
    print(f"database : {args.db_database}")


    connectMySQL(args.db_host, '3306', args.db_database, args.db_user, args.db_password)


    TARGET_FOLDER = './'

    LoadYamlConfig('./yaml/podmix-opml.yaml')
    #LoadYamlConfig('../yaml/podmix-opml.yaml')
    

    ClearFolder(TARGET_FOLDER)
    
    
    ProcessItems(TARGET_FOLDER)
  else:
    print("nope")


if __name__ == '__main__':
  main()
