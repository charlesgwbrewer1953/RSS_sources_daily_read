import sys
import pymysql

import sqlalchemy
from sqlalchemy import create_engine
print("sqlalchemy imported")
import mysql.connector
print("mysql.connector imported")
import pandas as pd
import requests
import feedparser
from datetime import date
from dateutil.parser import parse
from dateutil.tz import gettz



import datetime
import time

import datetime

import mysql.connector as mc
import pandas as pd
import requests
import feedparser
from datetime import date
from dateutil.parser import parse
from dateutil.tz import gettz
from nltk.corpus import stopwords
from afinn import Afinn


from sqlalchemy import create_engine
import pymysql
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

xtpw = "m3t1sz"
db = "metis"
try:
    conn = mc.connect(host="178.62.8.181",
                      port=3306,
                      user="metis",
                      password=xtpw,
                      database=db)
    print("Connected to mySQL database:", db)
except:
    print("Database access fail:", db)

cursor = conn.cursor()

cursor = conn.cursor()

#sql = '''SELECT item_title, orientation, item_date_published  FROM RSS_library12'''
sql = '''SELECT Feed, URL, Orientation, Country, Language  FROM metis.RSS_feeds'''
cursor.execute(sql)
RSS_feeds = pd.DataFrame(cursor.fetchall())
#result.columns = ['item', 'orientation', 'item_date_published']
RSS_feeds.columns = ['Feed', 'URL', 'Orientation', 'Country', 'Language']
rssSources_URL = RSS_feeds['URL']
print(rssSources_URL)
RSS_feeds.set_index("URL", inplace = True)





# --------------------------------------
print('2.0 Newsfeed retrieval initiated')
today = date.today()

print("2.0.1 Initiating sourceURL read validation")
day_success = 0
day_failure = 0
day_stories = 0
url_count = 0
story_count = 0
#         print("4.2 Processed RSS for ", item)
print("2.1. RSS read commenced")
# define output
out_df = pd.DataFrame(columns = ['feed_title', 'feed_link', 'feed_description', 'feed_last_updated', 'feed_language', 'feed_update_period',
                                 'item_title', 'item_creator', 'item_date_published', 'item_description', 'item_category1',
                                 'item_category2','item_category3','item_category4', 'item_category5', 'item_link', 'ext_name',
                                 'orientation', 'country', 'afinn_score', 'bing_score', 'nrc_scores', 'loughran_scores'])
filler = 'filler'



for item in rssSources_URL:
    try:                                # Verify URL exits
        read_URL(item)
        print(item)
        day_success = day_success + 1
        day_stories = day_stories +1
        NewsFeed = feedparser.parse(item)
        url_count = url_count + 1
        print("URL:", item, " URL_Count:", URL_count)

        for ent in range(1,len(d['entries'])):
            out_df.loc[ent, 'feed_title'] =  d['feed']['title']
            out_df.loc[ent, 'feed_link'] =  d['feed']['link']
            out_df.loc[ent, 'feed_description'] =  d['feed']['description']
            out_df.loc[ent, 'feed_last_updated'] =  d['feed']['updated']
            out_df.loc[ent, 'feed_language'] =  d['feed']['language']
            out_df.loc[ent, 'item_creator'] = d['feed']['author']


            out_df.loc[ent, 'item_title'] = d.entries[ent]['title']
            out_df.loc[ent, 'item_date_published'] = d.entries[ent]['published']
            out_df.loc[ent, 'item_link'] = d['entries'][0]['id']

            out_df.loc[ent, 'ext_name'] = RSS_feeds.loc[url,'Language']
            out_df.loc[ent, 'orientation'] = RSS_feeds.loc[url,'Orientation']
            out_df.loc[ent, 'country'] = RSS_feeds.loc[url,'Country']

            out_df.loc[ent, 'afinn_score'] = filler
            out_df.loc[ent, 'bing_score'] = filler
            out_df.loc[ent, 'nrc_scores'] = filler
            out_df.loc[ent, 'loughran_scores'] = filler
    #
    except:
        day_failure = day_failure + 1



out_df.to_csv("newsfeed.csv")
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

print("4. Data cleansing initiated")
error_count = 0
for item in range(0, len(out_df)):
    try:
        proper_date = parse(out_df.item_date_published[item])

    except:
        error_count = error_count + 1
    finally:
        out_df.item_date_published[item] = proper_date
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
out_df['item_date_published'] = pd.to_datetime(out_df['item_date_published'], utc = True)

print("UTC correction complete")



# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠

end_time  = datetime.datetime.now()

print("Process duration = ", end_time - start_time)