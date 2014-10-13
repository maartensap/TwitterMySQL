#!/bin/usr/env python
import argparse

parser = argparse.ArgumentParser(description = "Command line interface for the TwitterMySQL package")

mysql = parser.add_argument_group("Required MySQL parameters",
                                  "Setting connection parameters for the MySQL side of the wrapper")

mysql.add_argument('-d', "-db", dest="database",
                   help="Database to be connected to")
mysql.add_argument('-t', "--table", dest="table",
                   help="Name of table to create or prefix to the monthly table to be created")

twt = parser.add_argument_group("Required Twitter API parameters",
                                "Parameters for the Twitter API connection, "
                                + "see https://dev.twitter.com/rest/public or "
                                + "https://dev.twitter.com/streaming/overview for info")

twt.add_argument("--apiKey", dest="API_KEY", 
                 help="Twitter API key")
twt.add_argument("--apiSecret", dest="API_SECRET",
                 help="Twitter API Secret")
twt.add_argument("--accessToken", dest="ACCESS_TOKEN",
                 help="Twitter App Access token")
twt.add_argument("--accessSecret", dest="ACCESS_SECRET",
                 help="Twitter App Access token secret")
twt.add_argument("-k", "--apiKeysFile", dest="keysFile",
                 help="You can store your keys in a file. Put the 4 keys on separate lines, preceded by their label followed by a space.")

mysql_opt = parser.add_argument_group("Optional MySQL parameters", "Setting optional parameters for the MySQL connection [not an exhaustive list though]")
mysql_opt.add_argument("-H", "--host", dest="host", default="localhost",
                   help="Host that the MySQL server is on")
"""
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


args = parser.parse_args()
params = vars(args)

if args.API_KEY and args.API_SECRET and args.ACCESS_TOKEN and args.ACCESS_SECRET:
    if args.keysFile:
        print "Found both a keyFile and the 4 TwitterAPI keys in command line, using the command line keys over the file"
elif args.keysFile and not (args.API_KEY or args.API_SECRET or args.ACCESS_TOKEN or args.ACCESS_SECRET):
    with open(args.keysFile) as a:
        params.update({line.split(" ")[0]: line.split(" ")[1] for line in [l.strip() for l in a]})
    print "Read keyfile, should be okay"
else:
    raise ValueError("Missing twitter API keys, please include them either as arguments or in a file, or use -h for help")

del params["keysFile"]
from pprint import pprint
pprint(params)
