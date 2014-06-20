#!/usr/bin/env python
# Copyright 2014 David Irvine
#
# This file is part of MQTT2RRD
#
# MQTT2RRD is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# MQTT2RRD is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with MQTT2RRD.  If not, see "http://www.gnu.org/licenses/".
#
import sys, os, unicodedata, re, argparse

import mosquitto, rrdtool



parser = argparse.ArgumentParser()
parser.add_argument("hostname", help="The hostname or IP address of the MQTT server", type=str)
parser.add_argument('--username', help="The user name to use when authenticating", type=str, default=None)
parser.add_argument('--password', help="The password to use when authenticating", type=str, default=None)
parser.add_argument("--port", help="The port the MQTT server is listening on", type=int, default=1883)
parser.add_argument("--keepalive", help="The keepalive time in seconds", type=int, default=60)
parser.add_argument("--data_dir", help="The directory to store RRD graphs", type=str, default="/tmp")
parser.add_argument("--client_name", help="The client name to provide to the MQTT server", type=str, default="MQTT2RRD")
parser.add_argument("--subscribe", help="The topics to subscribe to", type=str, default="#")

args = parser.parse_args()

# Check the data directory exists
if not os.path.isdir(args.data_dir):
    sys.stderr.write("%s: Error: data directory %s does not exist or is not a directory" % (sys.argv[0],args.data_dir))
    sys.exit(1)

def on_connect(client, userdata, rc):
    for i in args.subscribe.split(" "):
        client.subscribe(i)


def extract_float(pl):
    try:
        return float(pl)
    except ValueError:
        pass

    try:
        return float( pl.split(" ")[0] )
    except ValueError:
        pass

    return None

def pathify(name):
    name=unicode(name)
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore')
    name = unicode(re.sub('[^\w\s-]', '', name).strip().lower())
    name = re.sub('[-\s]+', '-', name)
    return name

import string


def on_message(mosq, obj, msg):
    pl = extract_float(msg.payload)
    if pl == None:
        return
    print("Message received on topic "+msg.topic+" with QoS "+str(msg.qos)+" and payload %d "% pl)
    components = msg.topic.split("/")
    file_name = components.pop()
    file_name = "%s.rrd" % file_name
    dir_name = args.data_dir
    while(len(components) > 0):
        dir_name=os.path.join(dir_name, components.pop(0))
        if not os.path.isdir(dir_name):
            os.mkdir(dir_name)

    file_path = os.path.join(dir_name, file_name)
    graph_name= msg.topic.replace("/","_")
    graph_name= graph_name.replace(".","_")
    if len(graph_name) > 19:
        graph_name=graph_name[:19]

    ds="DS:%s:GAUGE:120:U:U" % graph_name
    ds=str(ds)
    times={}
    if not os.path.exists(file_path):
        # Create the RRD file
        try:
            rrdtool.create(str(file_path), "--step", "60", "--start", "0", ds, str('RRA:AVERAGE:0.5:2:30'),str('RRA:AVERAGE:0.5:5:288'),str('RRA:AVERAGE:0.5:30:336'), str('RRA:AVERAGE:0.5:60:1488'), str('RRA:AVERAGE:0.5:720:744'), str('RRA:AVERAGE:0.5:1440:265'))
        except rrdtool.error as e:
            print "FAIL: " + ds + " " + str(e)

        #                'RRA:AVERAGE:0.5:10:10000'
        #                'RRA:AVERAGE:0.5:100:10000000'
    try:
        rrdtool.update(str(file_path), str("N:%d" % pl))
        if pl > 0:
            print "Logged value: %d" % pl
    except:
        print "FAIL: " + pl + " " + str(e)




client = mosquitto.Mosquitto(args.client_name)
client.on_message = on_message
client.on_connect = on_connect



if args.username:
    client.username_pw_set(args.username, args.password)

client.connect(args.hostname, port=args.port, keepalive=args.keepalive)
client.loop_forever()




class Daemon:
        """
        A generic daemon class.

        Usage: subclass the Daemon class and override the run() method

        From:
        http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/

        """
        def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
                self.stdin = stdin
                self.stdout = stdout
                self.stderr = stderr
                self.pidfile = pidfile

        def daemonize(self):
                """
                do the UNIX double-fork magic, see Stevens' "Advanced
                Programming in the UNIX Environment" for details (ISBN 0201563177)
                http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
                """
                try:
                        pid = os.fork()
                        if pid > 0:
                                # exit first parent
                                sys.exit(0)
                except OSError, e:
                        sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
                        sys.exit(1)

                # decouple from parent environment
                os.chdir("/")
                os.setsid()
                os.umask(0)
