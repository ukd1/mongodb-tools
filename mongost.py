#!/usr/bin/python
#
#
# Simple script I use with MongoDB - it helps out in shell scripts
# mainly for doing backups and maintenance.
#
#
# Usage:
#   ./mongost.py --host 127.0.0.1:27017 --max-lag 20
#
# This will exit 0 if mongod at the --host is SECONDARY
# and less than --max-lag seconds of optimeDate from the PRIMARY.
#
# @author:      Russell Smith <russell.smith@ukd1.co.uk>
# @copyright:   UKD1 Limited 2011
# @licence:     MIT Licence

from pymongo import Connection
from datetime import *
import argparse


parser = argparse.ArgumentParser(description='mongost')
parser.add_argument('--host', help='Host to connect to - default 127.0.0.1:27017')
parser.add_argument('--max-lag', help='Max acceptable lag from PRIMARY in seconds')
parser.add_argument('--freeze', help='Seconds to freeze this server for if otherwise ok')
parser.add_argument('--quiet', help='Do not display as much output')
parser.set_defaults(queit=True, freeze=0, host='127.0.0.1:27017', max_lag=10)
args = parser.parse_args()


max_lag = int(args.max_lag)
freeze = int(args.freeze)
host = str(args.host)
quiet = bool(args.quiet)


def debug_output(message):
    if not quiet:
        print host + ":\t" + message


c = Connection(host)
masterInfo = c.admin.command('isMaster')


if not masterInfo['ismaster']:
    members = c.admin.command('replSetGetStatus')['members']

    primary_time = 0
    me_time = 0

    for member in members:
        if member['stateStr'] == 'PRIMARY':
            primary_time = member['optimeDate']
        elif member['name'] == masterInfo['me']:
            me = member

    lag = primary_time - me['optimeDate']

    if lag < timedelta(seconds=max_lag) and me['stateStr'] == 'SECONDARY':
        debug_output('Lag under ' + str(max_lag) + ' seconds and host is SECONDARY.')

        if freeze > 0:
            debug_output('Freezing host for ' + str(freeze) + ' seconds')
            c.admin.command({'replSetFreeze': freeze})

        debug_output('Exit(0).')
        exit(0)
    elif lag < timedelta(seconds=max_lag) and me['stateStr'] != 'SECONDARY':
        debug_output('Lag under ' + str(max_lag) + ' seconds, but host is ' + str(me['stateStr']) + '.')
        debug_output('Exit(1).')
        exit(1)
    else:
        debug_output('Lag is ' + str(lag) + '. This is more than max-lag: ' + str(timedelta(seconds=max_lag)) + '.')
        debug_output('Exit(1).')
        exit(1)
else:
    debug_output('host is currently PRIMARY.')
    debug_output('Exit(1).')
    exit(1)
