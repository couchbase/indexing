#! /usr/bin/env python3

import sys
import re
import json
import argparse

import plotly.plotly as py
from plotly.graph_objs import *

parser = argparse.ArgumentParser(description='parse and plot stats')
parser.add_argument('--kind', nargs=1, type=str, default="",
       help='kind of statistics')
parser.add_argument('--params', dest='params', default="all",
    help='parameters to plot')
parser.add_argument('logfile', nargs=1, help='logfile to parse')
args = parser.parse_args()
args.params = args.params.split(",")

stats = []

def tryhandler(handler) :
    try :
        handler()
    except:
        pass

def normalizeJson(value) :
    try :
        return int(value)
    except ValueError :
        val = 0
        try :
            x = value.split(" ")
            if x[0] != '0' :
                val = int(x[1].strip()) / int(x[0].strip())
        except:
            val = value
        return val

def loadJson(dstr) :
    nval, val = {}, {}
    try :
        val = json.loads(dstr)
    except:
        pass
    for param, value in val.items() :
        nval[param] = normalizeJson(value)
    return nval

def exec_matchers(lines, matchers) :
    if len(lines) == 0 :
        return
    for regx, fn in matchers :
        m = regx.match(lines[0])
        if m :
            fn(lines, m.group(), *m.groups())

def graph_idxstats(stats) :
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

def graph_count(stats) :
    mallocs, frees = [], []
    print("composing plot ...")
    for i, m in enumerate(stats) :
        mallocs.append(m["Mallocs"]); frees.append(m["Frees"]);

    x = list(range(1, len(stats)+1))
    mode, line = "lines+markers", Line(shape='spline')
    data = Data([
        Scatter(x=x, y=mallocs, mode=mode, name="mallocs", line=line),
        Scatter(x=x, y=frees, mode=mode, name="frees", line=line),
    ])
    print(py.plot(data, filename='count-graph'))

def graph_pausetimes(stats) :
    pauseTimes = []
    print("composing plot ...")
    [ pauseTimes.extend(m["PauseNs"]) for i, m in enumerate(stats) ]

    x = list(range(1, len(pauseTimes)+1))
    mode, line = "lines+markers", Line(shape='spline')
    data = Data([
        Scatter(x=x, y=pauseTimes, mode=mode, name="gc-stw", line=line)
    ])
    print(py.plot(data, filename='gc-pause-time-graph'))

def graph_dcplatency(stats) :
    mins, maxs, mean, variance = [], [], [], []
    print("composing plot ...")
    for i, m in enumerate(stats) :
        mins.append(m["min"]); maxs.append(m["max"])
        mean.append(m["mean"]); variance.append(m["variance"])

    x = list(range(1, len(mins)+1))
    mode, line = "lines+markers", Line(shape='spline')
    data = Data([
        Scatter(x=x, y=mins, mode=mode, name="minimum-latency", line=line),
        Scatter(x=x, y=maxs, mode=mode, name="maximum-latency", line=line),
        Scatter(x=x, y=mean, mode=mode, name="mean-latency", line=line),
        Scatter(x=x, y=variance, mode=mode, name="varnc-in-latency", line=line),
    ])
    print(py.plot(data, filename='dcp-latency-graph'))

def graph_allocation(stats) :
    allocs, total = [], []
    heapalloc, heapsys, heapidle, heapinuse, heaprels = [], [], [], [], []
    print("composing plot ...")
    for i, m in enumerate(stats) :
        total.append(m["TotalAlloc"]); allocs.append(m["Alloc"]);
        heapsys.append(m["HeapSys"])
        heapalloc.append(m["HeapAlloc"])
        heapidle.append(m["HeapIdle"])
        heapinuse.append(m["HeapInuse"])
        heaprels.append(m["HeapReleased"])

    x = list(range(1, len(stats)+1))
    mode, line = "lines+markers", Line(shape='spline')
    data = Data([
        Scatter(x=x, y=total, mode=mode, name="total-allocs", line=line),
        Scatter(x=x, y=allocs, mode=mode, name="allocs", line=line),
        Scatter(x=x, y=heapsys, mode=mode, name="heap-sys", line=line),
        Scatter(x=x, y=heapalloc, mode=mode, name="heap-allocs", line=line),
        Scatter(x=x, y=heapidle, mode=mode, name="heap-idle", line=line),
        Scatter(x=x, y=heapinuse, mode=mode, name="heap-inuse", line=line),
        Scatter(x=x, y=heaprels, mode=mode, name="heap-released", line=line),
    ])
    print(py.plot(data, filename='allocation-graph'))

def kind_memstats(logfile):
    print("parsing lines ...")
    stats = []
    handler_memstats = lambda line, dstr : stats.append(eval(dstr))
    matchers = [
      [ re.compile(r'.*\[Info\].*memstats(.*)'),
        handler_memstats ],
    ]
    for line in open(logfile).readlines() :
        for regx, fn in matchers :
            m = regx.match(line)
            if m : fn(m.group(), *m.groups())

    graph_allocation(stats)
    graph_count(stats)
    graph_pausetimes(stats)

def kind_dcplatency(logfile):
    print("parsing lines ...")
    stats = []
    handler_dcpstats = lambda line, dstr : stats.append(eval(dstr))
    matchers = [
      [ re.compile(r'.*\[Info\].*dcp latency stats(.*)'),
        handler_dcpstats ],
    ]
    for line in open(logfile).readlines() :
        for regx, fn in matchers :
            m = regx.match(line)
            if m : tryhandler(lambda : fn(m.group(), *m.groups()))
    graph_dcplatency(stats)

def kind_idxstats() :
    print("parsing lines ...")
    stats = []

    def handler_periodicstats(lines, line, dstr) :
        lines = [ line.strip("\n") for line in lines ]
        dstr = dstr.strip("\n")
        dstr = dstr + "".join(lines[1:])
        val = loadJson(dstr)
        stats.append(val)

    re_log = re.compile(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}T.*')
    matchers = [
      [ re.compile(r'.*\[Info\].*PeriodicStats = (.*)'),
        handler_periodicstats ],
    ]
    loglines = []
    for line in open(args.logfile[0]).readlines() :
        if re_log.match(line) :
            exec_matchers(loglines, matchers)
            loglines = []
        loglines.append(line)
    exec_matchers(loglines, matchers)
    graph_idxstats(stats)

if args.kind[0] == "dcplatency" :
    kind_dcplatency(args.logfile[0])
elif args.kind[0] == "memstats" :
    kind_memstats(args.logfile[0])
elif args.kind[0] == "idxstats" :
    kind_idxstats()
