#! /usr/bin/env python3

import sys
import re
import argparse

import plotly.plotly as py
from plotly.graph_objs import *

def graph_allocation(stats) :
    allocs, total = [], []
    heapalloc, heapsys, heapidle, heapinuse, heaprels = [], [], [], [], []
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

def graph_count(stats) :
    mallocs, frees = [], []
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
    [ pauseTimes.extend(m["PauseNs"]) for i, m in enumerate(stats) ]

    x = list(range(1, len(pauseTimes)+1))
    mode, line = "lines+markers", Line(shape='spline')
    data = Data([
        Scatter(x=x, y=pauseTimes, mode=mode, name="gc-stw", line=line)
    ])
    print(py.plot(data, filename='gc-pause-time-graph'))

def graph_dcplatency(stats) :
    mins, maxs, mean, variance = [], [], [], []
    for i, m in enumerate(stats) :
        mins.append(m["min"]); maxs.append(m["max"])
        mean.append(m["mean"]); variance.append(m["variance"])

    x = list(range(1, len(mins)+1))
    mode, line = "lines+markers", Line(shape='spline')
    data = Data([
        Scatter(x=x, y=mins, mode=mode, name="minimum-latency", line=line),
        Scatter(x=x, y=maxs, mode=mode, name="maximum-latency", line=line),
        Scatter(x=x, y=mean, mode=mode, name="mean-latency", line=line),
        Scatter(x=x, y=variance, mode=mode, name="variance-in-latency", line=line),
    ])
    print(py.plot(data, filename='dcp-latency-graph'))

def kind_memstats(logfile):
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

def tryhandler(handler) :
    try :
        handler()
    except:
        pass

parser = argparse.ArgumentParser(description='plot statistics')
parser.add_argument('--kind', nargs=1, type=str, default="",
       help='kind of statistics')
parser.add_argument('logfile', nargs=1, type=str, default="",
       help='log file')

args = parser.parse_args()

if args.kind[0] == "dcplatency" : kind_dcplatency(args.logfile[0])
if args.kind[0] == "memstats" : kind_memstats(args.logfile[0])
