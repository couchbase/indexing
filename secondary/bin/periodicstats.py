#! /usr/bin/env python3

import sys
import re
import json
import argparse

import plotly.plotly as py
from plotly.graph_objs import *

parser = argparse.ArgumentParser(description='periodic stats')
parser.add_argument('--params', dest='params', default="all",
    help='parameters to plot')
parser.add_argument('logfile', nargs=1, help='logfile to parse')
args = parser.parse_args()
args.params = args.params.split(",")

stats = []

def handler_periodicstats(lines, line, dstr) :
    lines = [ line.strip("\n") for line in lines ]
    dstr = dstr.strip("\n")
    dstr = dstr + "".join(lines[1:])
    try :
        val = json.loads(dstr)
    except:
        val = {}
    nval = {}
    for param, value in val.items() :
        try :
            nval[param] = int(value)
        except ValueError :
            nval[param] = 0
            x = value.split(" ")
            if x[0] != '0' :
                nval[param] = int(x[1].strip()) / int(x[0].strip())
    stats.append(nval)

re_log = re.compile(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}T.*')
matchers = [
  [ re.compile(r'.*\[Info\].*PeriodicStats = (.*)'),
    handler_periodicstats ],
]

def exec_matchers(lines) :
    if len(lines) == 0 :
        return
    for regx, fn in matchers :
        m = regx.match(lines[0])
        if m :
            fn(lines, m.group(), *m.groups())

print("parsing lines ...")
loglines = []
for line in open(args.logfile[0]).readlines() :
    if re_log.match(line) :
        exec_matchers(loglines)
        loglines = []
    loglines.append(line)
exec_matchers(loglines)


def indexerGraph() :
    x, params, scatters = {}, {}, []
    print("gathering data ...")
    if len(args.params) == 1 and args.params[0] == "all" :
        for i, m in enumerate(stats) :
            for param, value in m.items() :
                params.setdefault(param, []).append(int(value))
                x.setdefault(param, []).append(i)
    else :
        for i, m in enumerate(stats) :
            for param_patt in args.params :
                for param, value in m.items() :
                    if re.compile(param_patt).match(param) :
                        x.setdefault(param, []).append(i)
                        params.setdefault(param, []).append(m[param])

    mode, line = "lines+markers", Line(shape='spline')
    print("composing plot ...")
    print("parameters: %s" % ",".join(params.keys()))
    for param, value in params.items() :
        s = Scatter(x=x[param], y=value, mode=mode, name=param, line=line)
        scatters.append(s)
    data = Data(scatters)
    if len(data) > 0 :
        print(py.plot(data, filename='indexer-graph'))
    else :
        print("warn: no data to plot !!")

indexerGraph()
