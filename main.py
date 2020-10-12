import sys
import pymysql
import pymysql

import sqlalchemy
print("sqlalchemy imported")
import mysql.connector
print("mysql.connector imported")
import pandas as pd
import requests
import feedparser
from datetime import date
from dateutil.parser import parse
from dateutil.tz import gettz

from sqlalchemy import create_engine

import datetime
import time
print('sqlalchemy version:', sqlalchemy.__version__)
import datetime
start_time  = datetime.datetime.now()

# ====================================================



def read_URL(url):
    try:
        fp = requests.get(url)
    #        print("process read_URL: Access URL: ", url, " : Success")
    except:
        print("Access URL: ", url, " : Failure")


def read_RSS(url):
    print('RSS_feed.py: read_RSS')
    newsFeed = feedparser.parse(url)
    entries = newsFeed.entries[1]
    nf_title = entries['title']
    nf_published = entries['published']
    nf_summary = entries['summary']
    nf_response = [nf_title, nf_published, nf_summary]
    return(response)


def read_RSS_line(url):
    print('RSS_feed.py: read_RSS_line')
    for item in url:
        NewsFeed = feedparser.parse(item)
        #        print('Number of RSS posts :', len(NewsFeed.entries))
        for i in range(1, len(NewsFeed)):
            entry_out = NewsFeed.entries[i]
#            print('Post Title :', entry_out.title)

def read_Lines(url):
    #    print('RSS_feed.py: read_Lines')
    #    print("Number of RSS posts :", url, len(Newsfeed.entries))
    for item in url:
        NewsFeed = feedparser.parse(item)
        #        print('Number of RSS posts :', len(NewsFeed.entries))
        feed_out = pd.DataFrame(columns=['url', 'title', 'summary', 'published'])
        for i in range(1, len(NewsFeed)):
            entry_out = NewsFeed.entries[i]
            full_post = {'url': item, 'title': entry_out.title, 'summary': entry_out.summary,
                         'published ': entry_out.published}
        feed_out = feed_out.append(full_post, ignore_index=True)
        return(feed_out)

def dbConnect(xtpw, db):
    try:
        mydb = mysql.connector.connect(
            host = "178.62.8.181",
            port = 3306,
            user = "metis",
            password = xtpw,
            database = db
        )
        print("Connected to mySQL database:", db )
    except:
        print("Database access fail:", db)


# ==================================




pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 0)

print("0. Database connection initiated")
xtpw = "m3t1sz"
db = "metis"


try:
    mydb = mysql.connector.connect(
        host = "178.62.8.181",
        port = 3306,
        user = "metis",
        password = xtpw,
        database = db
    )
    print("Connected to mySQL database:", db )
except:
    print("Database access fail:", db)
print("0. Database connection complete")

# Retrieve static source data
RSS_feeds = pd.read_sql("SELECT * FROM RSS_feeds", mydb)
print("1.0. RSS_feeds static data retrieved")

rssSources_names = RSS_feeds["Feed"].unique()
rssSources_Countries = RSS_feeds["Country"].unique()
rssSources_Orientation = RSS_feeds["Orientation"].unique()
rssSources_URL = RSS_feeds["URL"].unique()

# Construct RSS enrichment df
rssSources_lookup = pd.DataFrame(columns = ['Feed', 'URL', 'Orientation', 'Country']) #Create empty dataframe
rssSources_lookup.set_index('URL', inplace=True)
for i in range (1, len(RSS_feeds)):
    #for i in range(1, 4):
    rssNewLine = {'Feed' : RSS_feeds.Feed[i],
                  'URL' : RSS_feeds.URL[i],
                  'Orientation' : RSS_feeds.Orientation[i],
                  'Country' : RSS_feeds.Country[i]
                  }
    rssSources_lookup = rssSources_lookup.append(rssNewLine, ignore_index=True)

rssSources_lookup.to_csv("rssSources.csv")  # Write working
print("1.99. Static source fields retrieved")




# --------------------------------------
print('2.0 Newsfeed retrieval initiated')
today = date.today()

print("2.0.1 Initiating sourceURL read validation")
day_success = 0
day_failure = 0
day_stories = 0
#         print("4.2 Processed RSS for ", item)
print("2.1. RSS read commenced")

feed_out = pd.DataFrame(columns = ['url', 'title', 'summary', 'published']) # Define db update
url_count = 0
story_count = 0
for item in rssSources_URL:
    try:                                # Verify URL exits
        read_URL(item)
        print(item)
        day_success = day_success + 1
        day_stories = day_stories +1
        NewsFeed = feedparser.parse(item)
        url_count = url_count + 1
        print("URL:", item, " URL_Count:", URL_count)

        for i in range(len(NewsFeed.entries)): # Read RSS for URL
            entry_out = NewsFeed.entries[i]
            full_post = {'url': item, 'title': entry_out.title, 'summary': entry_out.summary, 'published': entry_out.published}
            feed_out = feed_out.append(full_post, ignore_index=True)
            story_count = story_count + 1
    #
    except:
        day_failure = day_failure + 1



feed_out.to_csv("newsfeed.csv")
blank = "*                              *"
print("2.99 Newsfeed reading complete")
print("********************************")
print(blank)
print(blank)
print(blank)
print("* Newsfeed reading complete    *")
print("* Total feed read = ", day_success + day_failure,      "      *")
print("* Read success = ", day_success,         "         *")
print("* Read fail = ", day_failure, "              *")
print("* Stories read = ", story_count, "        *")
print(blank)
print(blank)
print(blank)
print(blank)
print("********************************")
print("")
print("")

#-----------------
print("3. Newsfeed Enrichment initiated")

feed_out_augmented = pd.merge(feed_out,
                              RSS_feeds,
                              left_on = ['url' ],
                              right_on = ['URL'],
                              how = 'left')

feed_out_augmented.drop(columns = ['url', 'AllSides Rating', 'Language'], inplace = True)
feed_out_augmented.to_csv("newsfeed_augmented.csv")
feed_out_augmented.columns = ['item_title', 'item_description', 'item_date_published', 'ext_name', 'feed_link', 'orientation', 'country']
# Clean dates
print("4. Data cleansing initiated")
error_count = 0
for item in range(0, len(feed_out_augmented)):
    try:
        proper_date = parse(feed_out_augmented.item_date_published[item])

    except:
        error_count = error_count + 1
    finally:
        feed_out_augmented.item_date_published[item] = proper_date
print('Error count (cleansing) = ', error_count)


# Validate contents of columns
defective_records = 0
print("4.1. Create clean_feed")
defective_records = 0
# print(feed_out_augmented[0:20])
clean_feed = pd.DataFrame(columns = ['item_title', 'item_description', 'item_date_published', 'ext_name', 'feed_link', 'orientation', 'country'])
print("4.1.1 Clean feed created")



print("Defective records = ", defective_records)
print("4.99 Data cleansing complete")

from pytz import all_timezones
from pytz import timezone
utc = timezone('UTC')
feed_out_augmented['item_date_published'] = pd.to_datetime(feed_out_augmented['item_date_published'], utc = True)

print("UTC correction complete")



# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠

end_time  = datetime.datetime.now()

print("Process duration = ", end_time - start_time)