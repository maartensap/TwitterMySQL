#! /usr/bin/python

__author__ = "Maarten Sap"
__email__ = "maartensap93@gmail.com"
__version__ = "0.3"


"""
TODOs:
pull retweeted message as well as original
Command Line interface
better geolocation?

"""

import datetime, time
import os, sys
import json, re

import MySQLdb
from TwitterAPI import TwitterAPI
from requests.exceptions import ChunkedEncodingError

import xml.etree.ElementTree as ET
from HTMLParser import HTMLParser


MAX_MYSQL_ATTEMPTS = 5
MAX_TWITTER_ATTEMPTS = 5
TWEET_LIMIT_BEFORE_INSERT = 8000
TWT_REST_WAIT = 15*60

DEFAULT_MYSQL_COL_DESC = ["user_id bigint(20)", "message_id bigint(20) primary key",
                          "message text", "created_time datetime",
                          "in_reply_to_message_id bigint(20)",
                          "in_reply_to_user_id bigint(20)", "retweet_message_id bigint(20)",
                          "source varchar(128)", "lang varchar(4)", "time_zone varchar(64)",
                          "friend_count int(6)", "followers_count int(6)",
                          "user_location varchar(200)", "tweet_location varchar(200)", "coordinates varchar(128)",
                          "coordinates_address varchar(64)", "coordinates_state varchar(3)",
                          "index useriddex (user_id)", "index datedex (created_time)"]
DEFAULT_TWEET_JSON_SQL_CORR = {'id': "['id_str']",
                               'message_id': "['id_str']",
                               'message': "['text']",
                               'created_time': "['created_at']",
                               'user_id': "['user']['id_str']",
                               'in_reply_to_message_id': "['in_reply_to_status_id_str']",
                               'in_reply_to_user_id': "['in_reply_to_user_id_str']",
                               'retweet_message_id': "['retweeted_status']['id']",
                               'user_location': "['user']['location']",
                               'tweet_location': "['place']['full_name']",
                               'friend_count': "['user']['friends_count']",
                               'followers_count': "['user']['followers_count']",
                               'time_zone': "['user']['id_str']",
                               'lang': "['lang']",
                               'source': "['source']",
                               }
    

class TwitterMySQL:
    """Wrapper for the integration of Twitter APIs into MySQL
    Turns JSON tweets into row format
    Failsafe connection to MySQL servers
    Geolocates if tweet contains coordinates in the US
    [TODO] Geolocates using the Google Maps API    
    """

    def _warn(self, *objs):
        errorStream = open(self.errorFile, "a+") if self.errorFile else sys.stderr
        print >> errorStream, "\rWARNING: ", " ".join(str(o) for o in objs)

    def __init__(self, **kwargs):
        """
        Required parameters:
          - db              MySQL database to connect to
          - table           table to insert Twitter responses in
          - API_KEY         Twitter API key (to connect to Twitter)
          - API_SECRET      Twitter API Secret
          - ACCESS_TOKEN    Twitter App Access token
          - ACCESS_SECRET   Twitter App Access token secret

        Optional parameters:
          - noWarnings      disable MySQL warnings [Default: False]
          - dropIfExists    set to True to delete the existing table
          - geoLocate       a function that converts coordinates to state
                            and/or address.
                            Format:
                            (state, address) = your_method(lat, long)
          - errorFile       error logging file - warnings will be written to it
                            [Default: stderr]
          - jTweetToRow     JSON tweet to MySQL row tweet correspondence
                            (see help file for more info)
                            [Default: DEFAULT_TWEET_JSON_SQL_CORR]
          - SQLfieldsExp    SQL column description for MySQL table
                            [Default: DEFAULT_MYSQL_COL_DESC]
          - host            host where the MySQL database is on
                            [Default: localhost]
          - any other MySQL.connect argument
        """
        
        if "table" in kwargs:
            self.table = kwargs["table"]
            del kwargs["table"]
        else:
            raise ValueError("Table name missing")

        if "dropIfExists" in kwargs:
            self.dropIfExists = kwargs["dropIfExists"]
            del kwargs["dropIfExists"]
        else:
            self.dropIfExists = False
            
        if "geoLocate" in kwargs:
            self.geoLocate = kwargs["geoLocate"]
            del kwargs["geoLocate"]
        else:
            self.geoLocate = None

        if "noWarnings" in kwargs and kwargs["noWarnings"]:
            del kwargs["noWarnings"]
            from warnings import filterwarnings
            filterwarnings('ignore', category = MySQLdb.Warning)

        if "errorFile" in kwargs:
            self.errorFile = kwargs["errorFile"]
            del kwargs["errorFile"]
        else:
            self.errorFile = None

        if "jTweetToRow" in kwargs:
            self.jTweetToRow = kwargs["jTweetToRow"]
            del kwargs["jTweetToRow"]
        else:
            self.jTweetToRow = DEFAULT_TWEET_JSON_SQL_CORR
            
        if "fields" in kwargs and "SQLfieldsExp" in kwargs:
            # Fields from the JSON Tweet to pull out
            self.columns = kwargs["fields"]
            del kwargs["fields"]
            self.columns_description = kwargs["SQLfieldsExp"]
            del kwargs["SQLfieldsExp"]
            if len([f for f in self.columns_description if "index" != f[:5]]) != len(self.columns):
                raise ValueError("There was a mismatch between the number of columns in the 'fields' and the 'field_expanded' variable. Please check those and try again.")

        elif "fields" in kwargs:
            raise ValueError("Please provide a detailed MySQL column description of the fields you want grabbed. (keyword argument: 'SQLfieldsExp')")
            
        elif "SQLfieldsExp" in kwargs:
            self.columns_description = kwargs["SQLfieldsExp"]
            del kwargs["SQLfieldsExp"]
            self.columns = [f.split(' ')[0]
                            for f in self.columns_description
                            if f.split(' ')[0][:5] != "index"]
        else:
            self.columns_description = DEFAULT_MYSQL_COL_DESC
            self.columns = [f.split(' ')[0]
                            for f in self.columns_description
                            if f.split(' ')[0][:5] != "index"]

        if "api" in kwargs:
            self._api = kwargs["api"]
            del kwargs["api"]
        elif ("API_KEY" in kwargs and
              "API_SECRET" in kwargs and
              "ACCESS_TOKEN" in kwargs and
              "ACCESS_SECRET" in kwargs):
            self._api = TwitterAPI(kwargs["API_KEY"], kwargs["API_SECRET"], kwargs["ACCESS_TOKEN"], kwargs["ACCESS_SECRET"])
            del kwargs["API_KEY"], kwargs["API_SECRET"], kwargs["ACCESS_TOKEN"], kwargs["ACCESS_SECRET"]
        else:
            raise ValueError("TwitterAPI object or API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_SECRET needed to connect to Twitter. Please see dev.twitter.com for the keys.")

        if not "charset" in kwargs:
            kwargs["charset"] = 'utf8'

        try:
            self._connect(kwargs)
        except TypeError as e:
            print "You're probably using the wrong keywords, here's a list:\n"+self.__init__.__doc__
            raise TypeError(e)

    def _connect(self, kwargs = None):
        """Connecting to MySQL sometimes has to be redone"""
        if kwargs:
            self._SQLconnectKwargs = kwargs
        elif not kwargs and self._SQLconnectKwargs:
            kwargs = self._SQLconnectKwargs

        self._connection = MySQLdb.connect(**kwargs)
        self.cur = self._connection.cursor()

    def _wait(self, t, verbose = True):
        """Wait function, offers a nice countdown"""
        for i in xrange(t):
            if verbose:
                print "\rDone waiting in: %s" % datetime.timedelta(seconds=(t-i)),
                sys.stdout.flush()
            time.sleep(1)
        if verbose:
            print "\rDone waiting!           "
        
    def _execute(self, query, nbAttempts = 0, verbose = True):
        if nbAttempts >= MAX_MYSQL_ATTEMPTS:
            self._warn("Too many attempts to execute the query, moving on from this [%s]" % query[:300])
            return 0
        
        if verbose: print "SQL:\t%s" % query[:200]

        try:
            ret = self.cur.execute(query)
        except Exception as e:
            if "MySQL server has gone away" in str(e):
                self._connect()
            nbAttempts += 1
            if not verbose: print "SQL:\t%s" % query[:200]
            self._warn("%s [Attempt: %d]" % (str(e), nbAttempts))
            self._wait(nbAttempts * 2)
            ret = self._execute(query, nbAttempts, False)
        
        return ret

    def _executemany(self, query, values, nbAttempts = 0, verbose = True):
        if nbAttempts >= MAX_MYSQL_ATTEMPTS:
            self._warn("Too many attempts to execute the query, moving on from this [%s]" % query[:300])
            return 0

        if verbose: print "SQL:\t%s" % query[:200]
        ret = None
        try:
            ret = self.cur.executemany(query, values)
        except Exception as e:
            if "MySQL server has gone away" in str(e):
                self._connect()
            nbAttempts += 1
            if not verbose: print "SQL:\t%s" % query[:200]
            self._warn("%s [Attempt: %d]" % (str(e), nbAttempts))
            self._wait(nbAttempts * 2)
            ret = self._executemany(query, values, nbAttempts, False)

        return ret
    
    def createTable(self, table = None):
        """
        Creates the table specified during __init__().
        By default, the table will be deleted if it already exists,
        but there will be a 10 second grace period for the user
        to cancel the deletion (by hitting CTRL-c).
        To disable the grace period and have it be deleted immediately,
        please use dropIfExists = True during construction
        """
        table = self.table if not table else table

        # Checking if table exists
        SQL = """show tables like '%s'""" % table
        self._execute(SQL)
        SQL = """create table %s (%s)""" % (table, ', '.join(self.columns_description))
        if not self.cur.fetchall():
            # Table doesn't exist
            self._execute(SQL)
        else:
            # table does exist
            if not self.dropIfExists:
                USER_DELAY = 10

                for i in xrange(USER_DELAY):
                    print "\rTable %s already exists, it will be deleted in %s, please hit CTRL-C to cancel the deletion" % (table, datetime.timedelta(seconds=USER_DELAY-i)), 
                    sys.stdout.flush()
                    time.sleep(1)

            print "\rTable %s already exists, it will be deleted" % table, " " * 150
            SQL_DROP = """drop table %s""" % table
            self._execute(SQL_DROP)
            self._execute(SQL)
            
    def insertRow(self, row, table = None, columns = None, verbose = True):
        """Inserts a row into the table specified using an INSERT SQL statement"""
        return self.insertRows([row], table, columns, verbose)

    def replaceRow(self, row, table = None, columns = None, verbose = True):
        """Inserts a row into the table specified using a REPLACE SQL statement."""
        return self.replaceRows([row], table, columns, verbose)

    def insertRows(self, rows, table = None, columns = None, verbose = True):
        """Inserts multiple rows into the table specified using an INSERT SQL statement"""
        table = self.table if not table else table
        columns = self.columns if not columns else columns

        EXISTS = "SHOW TABLES LIKE '%s'" % table
        if not self._execute(EXISTS, verbose = False): self.createTable(table)

        SQL = "INSERT INTO %s (%s) VALUES (%s)" % (table,
                                                   ', '.join(columns),
                                                   ', '.join("%s" for r in rows[0]))
        return self._executemany(SQL, rows, verbose = verbose)

    def replaceRows(self, rows, table = None, columns = None, verbose = True):
        """Inserts multiple rows into the table specified using a REPLACE SQL statement"""
        table = self.table if not table else table
        columns = self.columns if not columns else columns
        
        EXISTS = "SHOW TABLES LIKE '%s'" % table
        if not self._execute(EXISTS, verbose = False): self.createTable(table)

        SQL = "REPLACE INTO %s (%s) VALUES (%s)" % (table,
                                                    ', '.join(columns),
                                                    ', '.join("%s" for r in rows[0]))
        return self._executemany(SQL, rows, verbose = verbose)

    def _tweetTimeToMysql(self, timestr, parseFormat = '%a %b %d %H:%M:%S +0000 %Y'):
        # Mon Jan 25 05:02:27 +0000 2010
        return str(time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(timestr, parseFormat)))

    def _yearMonth(self, mysqlTime):
        return time.strftime("%Y_%m",time.strptime(mysqlTime,"%Y-%m-%d %H:%M:%S"))

    def _prepTweet(self, jTweet):
        
        tweet = {}

        for SQLcol in self.columns:

            try:
                if SQLcol in self.jTweetToRow:
                    tweet[SQLcol] = eval("jTweet%s" % self.jTweetToRow[SQLcol])
                    if isinstance(tweet[SQLcol], str) or isinstance(tweet[SQLcol], unicode):
                        tweet[SQLcol] = HTMLParser().unescape(tweet[SQLcol]).encode("utf-8")
                    if SQLcol == "created_time":
                        tweet[SQLcol] = self._tweetTimeToMysql(tweet[SQLcol])
                    if SQLcol == "source":
                        try:
                            tweet[SQLcol] = ET.fromstring(re.sub("&", "&amp;", tweet[SQLcol])).text
                        except Exception as e:
                            raise NotImplementedError("OOPS", type(e), e, [tweet[SQLcol]])
                else:
                    tweet[SQLcol] = None
            except KeyError:
                tweet[SQLcol] = None

        if not any(tweet.values()):
            raise NotImplementedError("OOPS", jTweet, tweet)

        # Coordinates state and address
        if "coordinates" in jTweet and jTweet["coordinates"]:
            lon, lat = map(lambda x: float(x), jTweet["coordinates"]["coordinates"])
            if self.geoLocate:
                (state, address) = self.geoLocate(lat, lon)
            else:
                (state, address) = (None, None)
            tweet["coordinates"] = str(jTweet["coordinates"]["coordinates"])
            tweet["coordinates_state"] = str(state) if state else None
            tweet["coordinates_address"] = str(address) if address else str({"lon": lon, "lat": lat})
        
        # Tweet is dictionary of depth one, now has to be linearized
        tweet = [tweet[SQLcol] for SQLcol in self.columns]
        
        return tweet
    
    def _apiRequest(self, twitterMethod, params):
        done = False
        nbAttempts = 0
        
        while not done and nbAttempts < MAX_TWITTER_ATTEMPTS:
            try:
                r = self._api.request(twitterMethod, params)
            except Exception as e:
            # If the request doesn't work
                if "timed out" in str(e).lower():
                    self._warn("Time out encountered, reconnecting immediately.")
                    self._wait(1, False)
                else:
                    self._warn("Unknown error encountered: [%s]" % str(e))
                    self._wait(10)
                nbAttempts += 1
                continue

            # Request was successful in terms of http connection
            try:
                for i, response in enumerate(r.get_iterator()):
                    # Checking for error messages
                    if isinstance(response, int) or "delete" in response:
                        continue
                    if i == 0 and "message" in response and "code" in response:
                        if response['code'] == 88: # Rate limit exceeded
                            self._warn("Rate limit exceeded, waiting 15 minutes before a restart")
                            self._wait(TWT_REST_WAIT)
                        else:
                            self._warn("Error message received from Twitter %s" % str(response))
                        continue
                    
                    yield self._prepTweet(response)
                done = True
            except ChunkedEncodingError as e:
                # nbAttempts += 1
                self._warn("ChunkedEncodingError encountered, reconnecting immediately: [%s]" % e)
                continue
            except Exception as e:
                nbAttempts += 1
                self._warn("unknown exception encountered, waiting %d second: [%s]" % (nbAttempts * 2, str(e)))
                self._wait(nbAttempts * 2)
                continue
            # If it makes it all the way here, there was no error encountered
            nbAttempts = 0
            
        if nbAttempts >= MAX_TWITTER_ATTEMPTS:
            self._warn("Request attempted too many times (%d), it will not be executed anymore [%s]" % (nbAttempts, twitterMethod + str(params)))
            return

    def apiRequest(self, twitterMethod, **params):
        """
        Takes in a Twitter API request and yields formatted responses in return

        Use as follows:
        for tweet in twtSQL.apiRequest('statuses/filter', track="Twitter API"):
            print tweet

        For more info (knowing which twitterMethod to use) see:
        http://dev.twitter.com/rest/public
        http://dev.twitter.com/streaming/overview
        """ 
        for response in self._apiRequest(twitterMethod, params):
            yield response

    def _tweetsToMySQL(self, tweetsYielder, replace = False, monthlyTables = False):
        """
        Tool function to insert tweets into MySQL tables in chunks,
        while outputting counts.
        """
        tweetsDict = {}
        i = 0
        
        # TWEET_LIMIT_BEFORE_INSERT = 100

        for tweet in tweetsYielder:
            i += 1
            
            try:
                tweetsDict[self._yearMonth(tweet[3])].append(tweet)
            except KeyError:
                tweetsDict[self._yearMonth(tweet[3])] = [tweet]
            
            if i % 10 == 0:
                print "\rNumber of tweets grabbed: %d" % i,
                sys.stdout.flush()
            
            if i % TWEET_LIMIT_BEFORE_INSERT == 0:
                print
                if monthlyTables:
                    for yearMonth, twts in tweetsDict.iteritems():
                        table = self.table+"_"+yearMonth
                        if replace:
                            print "Sucessfully replaced %4d tweets into '%s' (%4d rows affected) [%s]" % (i, table, self.replaceRows(twts, table = table, verbose = False), time.strftime("%c"))
                        else:
                            print "Sucessfully inserted %4d tweets into '%s' [%s]" % (self.insertRows(twts, table = table, verbose = False), table, time.strftime("%c"))
                else:
                    tweets = [twt for twts in tweetsDict.values() for twt in twts]
                    if replace:
                        print "Sucessfully replaced %4d tweets into '%s' (%4d rows affected) [%s]" % (i, self.table, self.replaceRows(tweets, verbose = False), time.strftime("%c"))
                    else:
                        print "Sucessfully inserted %4d tweets into '%s' [%s]" % (self.insertRows(tweets, verbose = False), self.table, time.strftime("%c"))
                i, tweetsDict = (0, {})

        # If there are remaining tweets
        if any(tweetsDict.values()):
            print
            if monthlyTables:
                for yearMonth, twts in tweetsDict.iteritems():
                    table = self.table+"_"+yearMonth
                    if replace:
                        print "Sucessfully replaced %4d tweets into '%s' (%4d rows affected) [%s]" % (i, table, self.replaceRows(twts, table = table, verbose = False), time.strftime("%c"))
                    else:
                        print "Sucessfully inserted %4d tweets into '%s' [%s]" % (self.insertRows(twts, table = table, verbose = False), table, time.strftime("%c"))
            else:
                tweets = [twt for twts in tweetsDict.values() for twt in twts]
                if replace:
                    print "Sucessfully replaced %4d tweets into '%s' (%4d rows affected) [%s]" % (i, self.table, self.replaceRows(tweets, verbose = False), time.strftime("%c"))
                else:
                    print "Sucessfully inserted %4d tweets into '%s' [%s]" % (self.insertRows(tweets, verbose = False), self.table, time.strftime("%c"))
            i, tweetsDict = (0, {})

    def tweetsToMySQL(self, twitterMethod, **params):
        """
        Ultra uber awesome function that takes in a Twitter API
        request and inserts it into MySQL, all in one call
        
        Here's some examples on how to use it:
            For the Search API
            twtSQL.tweetsToMySQL('search/tweets', q='"Taylor Swift" OR "Jennifer Lawrence"')

            For hydrating (getting all available details) for a tweet
            twtSQL.tweetsToMySQL('statuses/lookup', id="504710715954188288")

        For more twitterMethods and info on how to use them, see:
        http://dev.twitter.com/rest/public
        http://dev.twitter.com/streaming/overview
        """

        # Replace SQL command instead of insert
        if "replace" in params:
            replace = params["replace"]
            del params["replace"]
        else:
            replace = False

        if "monthlyTables" in params:
            monthlyTables = params["monthlyTables"]
            del params["monthlyTables"]
        else:
            monthlyTables = False
        
        self._tweetsToMySQL(self._apiRequest(twitterMethod, params), replace = replace, monthlyTables = monthlyTables)
        

    def randomSampleToMySQL(self, replace = False, monthlyTables = True):
        """
        Takes the random sample of all tweets (~ 1%) and
        inserts it into monthly table [tableName_20YY_MM].
        For more info, see:
        http://dev.twitter.com/streaming/reference/get/statuses/sample
        """
        self.tweetsToMySQL('statuses/sample', replace = replace, monthlyTables = monthlyTables)

    def filterStreamToMySQL(self, **params):
        """
        Use this to insert the tweets from the FilterStream into MySQL

        Here's an example:
            twtSQL.filterStreamToMySQL(track="Taylor Swift")
        Here's a second example (Continental US bounding box):
            twtSQL.filterStreamToMySQL(locations="-124.848974,24.396308,-66.885444,49.384358")

        More info here:
        http://dev.twitter.com/streaming/reference/post/statuses/filter
        """
        self.tweetsToMySQL('statuses/filter', **params)

    def userTimeline(self, **params):
        """
        For a given user, returns all the accessible tweets from that user,
        starting with the most recent ones (Twitter imposes a 3200 tweet limit).

        Here's an example of how to use it:
        for tweet in userTimeline(screen_name = "taylorswift13"):
            print tweet

        See http://dev.twitter.com/rest/reference/get/statuses/user_timeline for details        
        """
        ok = True
        print "Finding tweets for %s" % ', '.join(str(k)+': '+str(v) for k,v in params.iteritems())
        params["count"] = 200 # Twitter limits to 200 returns
        
        i = 0

        while ok:
            
            tweets = [tweet for tweet in self._apiRequest('statuses/user_timeline', params)]
            if not tweets:
                # Warn about no tweets?
                ok = False
                if i != 0: print 
            else:
                i += len(tweets)
                
                print "\rNumber of tweets grabbed: %d" % i,
                sys.stdout.flush()

                params["max_id"] = str(long(tweets[-1][1])-1)
                for tweet in tweets:
                    yield tweet
    
    def userTimelineToMySQL(self, **params):
        """
        For a given user, inserts all the accessible tweets from that user into,
        the current table. (Twitter imposes a 3200 tweet limit).

        Here's an example of how to use it:
        userTimelineToMySQL(screen_name = "taylorswift13")

        For details on keywords to use, see
        http://dev.twitter.com/rest/reference/get/statuses/user_timeline
        """
        print "Grabbing users tweets and inserting into MySQL"
        
        # Replace SQL command instead of insert
        if "replace" in params:
            replace = params["replace"]
            del params["replace"]
        else:
            replace = False

        if "monthlyTables" in params:
            monthlyTables = params["monthlyTables"]
            del params["monthlyTables"]
        else:
            monthlyTables = False

        self._tweetsToMySQL(self.userTimeline(**params), replace = replace, monthlyTables = monthlyTables)

    def search(self, **params):
        """
        Search API
        """
        ok = True
        print "Finding tweets for %s" % ', '.join(str(k)+': '+str(v) for k,v in params.iteritems())
        params["count"] = 200 # Twitter limits to 200 returns
        
        i = 0

        while ok:
            
            tweets = [tweet for tweet in self._apiRequest('search/tweets', params)]
            if not tweets:
                # Warn about no tweets?
                ok = False
                if i != 0: print 
            else:
                i += len(tweets)
                
                print "\rNumber of tweets grabbed: %d" % i,
                sys.stdout.flush()

                params["max_id"] = str(long(tweets[-1][1])-1)
                for tweet in tweets:
                    yield tweet


    def searchToMySQL(self, **params):
        """
        Queries the Search API and pulls as many results as possible

        Here's an example of how to use it:
        userTimelineToMySQL(screen_name = "taylorswift13")

        For details on keywords to use, see
        http://dev.twitter.com/rest/reference/get/statuses/user_timeline
        """
        print "Grabbing users tweets and inserting into MySQL"
        
        # Replace SQL command instead of insert
        if "replace" in params:
            replace = params["replace"]
            del params["replace"]
        else:
            replace = False

        if "monthlyTables" in params:
            monthlyTables = params["monthlyTables"]
            del params["monthlyTables"]
        else:
            monthlyTables = False

        self._tweetsToMySQL(self.search(**params), replace = replace, monthlyTables = monthlyTables)
