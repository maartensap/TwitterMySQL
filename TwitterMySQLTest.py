#!/usr/bin/env python

import sys, os
from pprint import pprint
# sys.path.append("/home/maarten/TwitterMySQL")
from TwitterMySQL import TwitterMySQL
from TwitterAPI import TwitterAPI

locationPath = '/'.join(os.path.abspath(__file__).split('/')[:-2]) + "/Code/"
locationPath += "PERMA/data/twitter"
print "locationPath: " + locationPath
#locationPath = '/'.join(os.path.abspath(__file__).split('/')[:-2])
#locationPath = "~/PERMA/data/twitter"
sys.path.append(locationPath)
import locationInfo


def testSampleStream(twtSQL):
    twtSQL.randomSampleToMySQL(monthlyTables=True, replace = True)

def testFilterStream(twtSQL):  
    twtSQL.filterStreamToMySQL(locations="5.9559,45.818,10.4921,47.8084")
    # twtSQL.filterStreamToMySQL(locations="-124.848974,24.396308,-66.885444,49.384358")
    
def testUserTimeline(twtSQL, screen_name):
    twtSQL.userTimelineToMySQL(screen_name = screen_name, replace = True)

##(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_SECRET) = ("WEEDIvZsKk7AFYbtGu1li4Z7a", "5sTWzRPsY7SopjH5XT2cHKtM4ANO2jzZ8OqYVoScFizhcXw3q1", "474257339-G8xZZggYL5Eso3xjgBdQ5p115jBHNI9iAAZlGLBz", "jmkBkkebCwnpq47344Mpl5dA0P9ypgH9Cfycal4ncHLRX")
#Selah credentials:
(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_SECRET) = ("bIOuO5utKvtjuqBavYEo3axKx", "qZ4RxGEbpPAcpMPFebwsqAeg6C2oTJmkUq71fVtNVZHASFbB07", "2883093946-yfutAvjhYxsQmTJAOnFptPhdXFKqOjqxwvAebG3", "ktqKUIAHBzRlZvfAq38R8ZTbZuIGQyOaxMlbYzCnoeZjT")

mysql_columns = ["user_id", "message_id", "message", "created_time", "in_reply_to_message_id", "in_reply_to_user_id", "retweet_message_id", "source", "lang", "time_zone", "friend_count", "followers_count", "coordinates", "coordinates_address", "coordinates_state"]
mysql_columns_exp = ["user_id bigint(20)", "message_id bigint(20) primary key", "message text", "created_time datetime", "in_reply_to_message_id bigint(20)", "in_reply_to_user_id bigint(20)", "retweet_message_id bigint(20)", "source varchar(128)", "lang varchar(4)", "time_zone varchar(64)", "friend_count int(6)", "followers_count int(6)", "coordinates varchar(128)", "coordinates_address varchar(64)", "coordinates_state varchar(3)", "index useriddex (user_id)"]
api = TwitterAPI(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

lm = locationInfo.LocationMap()

twtSQL = TwitterMySQL(db="testing", host='localhost', api=api, SQLfieldsExp = mysql_columns_exp, table='deleteMe', noWarnings = True, geoLocate = lm.reverseGeocodeLocal, dropIfExists = True)

print "Testing the twitterMySQL wrapper"
screennames = ["maarten1709", "taylorswift13"]
testUserTimeline(twtSQL, "maarten1709")
testSampleStream(twtSQL)
