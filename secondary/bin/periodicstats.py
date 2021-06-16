#! /usr/bin/env python3

import sys
import re
import json
import argparse
import datetime

import plotly.plotly as py
from plotly.graph_objs import *

parser = argparse.ArgumentParser(description='parse and plot stats')
parser.add_argument('--kind', nargs=1, type=str, default="",
       help='kind of statistics')
parser.add_argument('--params', dest='params', default="all",
    help='parameters to plot')
parser.add_argument('--opaques', dest='opaques', default="",
    help='graph for opaque tokens')
parser.add_argument('--buckets', dest='buckets', default="",
    help='graph for buckets')
parser.add_argument('--indexes', dest='indexes', default="",
    help='graph for index')
parser.add_argument('--topic', dest='topic', default="",
    help='graph for specified topic')
parser.add_argument('--raddr', dest='raddr', default="",
    help='graph for specified endpoint raddr')
parser.add_argument('logfile', nargs=1, help='logfile to parse')
args = parser.parse_args()
args.params = [ re.compile(p) for p in args.params.split(",") ]
args.opaques = [ x.strip() for x in args.opaques.split(",") if x.strip() ]
args.buckets = [ x.strip() for x in args.buckets.split(",") if x.strip() ]

stats = []

def tryhandler(handler) :
    try :
        handler()
    except:
        raise

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
    if len(args.params) == 1 and args.params[0].match("all") :
        for i, m in enumerate(stats) :
            for param, value in m.items() :
                params.setdefault(param, []).append(int(value))
                x.setdefault(param, []).append(i)
    else :
        for i, m in enumerate(stats) :
            for param_patt in args.params :
                for param, value in m.items() :
                    if param_patt.match(param) :
                        x.setdefault(param, []).append(i)
                        params.setdefault(param, []).append(m[param])

    mode, line = "lines+markers", Line(shape='spline')
    print("composing plot ...")
    print("parameters: %s" % ",".join(params.keys()))
    for param, value in params.items() :
        s = Scatter(x=x[param], y=value, mode=mode, name=param, line=line)
        scatters.append(s)
    data = Data(scatters)
    name = 'indexer-graph-%s' % datetime.datetime.now().microsecond
    if len(data) > 0 :
        print(py.plot(data, filename=name))
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

def graph_numgc(stats) :
    numgc = []
    print("composing plot ...")
    [ numgc.append(m["NumGC"]) for i, m in enumerate(stats) ]

    x = list(range(1, len(numgc)+1))
    mode, line = "lines+markers", Line(shape='spline')
    data = Data([
        Scatter(x=x, y=numgc, mode=mode, name="gc-numgc", line=line)
    ])
    print(py.plot(data, filename='gc-numgc-graph'))

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

def graph_kvdata(opq, stats) :
    hbs, evs, reqs, ends, ups, dels, exprs = [], [], [], [], [], [], []
    smin, smax, savg = [], [], []
    print("composing plot ...")
    for i, m in enumerate(stats) :
        hbs.append(m["hbCount"]); evs.append(m["eventCount"])
        reqs.append(m["reqCount"]); ends.append(m["endCount"])
        smin.append(m["snapStat.min"]); smax.append(m["snapStat.max"])
        savg.append(m["snapStat.avg"])
        ups.append(m["upsertCount"]); dels.append(m["deleteCount"])

    x = list(range(1, len(hbs)+1))
    mode, line = "lines+markers", Line(shape='spline')
    name = "%s" % opq
    data = Data([
        Scatter(x=x, y=hbs, mode=mode, name=name+"-heartbeat", line=line),
        Scatter(x=x, y=evs, mode=mode, name=name+"-events", line=line),
        Scatter(x=x, y=reqs, mode=mode, name=name+"-strmreq", line=line),
        Scatter(x=x, y=ends, mode=mode, name=name+"-strmend", line=line),
        Scatter(x=x, y=ups, mode=mode, name=name+"-upserts", line=line),
        Scatter(x=x, y=dels, mode=mode, name=name+"-deletes", line=line),
        Scatter(x=x, y=exprs, mode=mode, name=name+"-expires", line=line),
        Scatter(x=x, y=smin, mode=mode, name=name+"-snapmin", line=line),
        Scatter(x=x, y=smax, mode=mode, name=name+"-snapmax", line=line),
        Scatter(x=x, y=savg, mode=mode, name=name+"-snapavg", line=line),
    ])
    print(py.plot(data, filename='kvdata-graph-%s'%opq))

def graph_endp(topic, raddr, stats) :
    muts, ups, dels, uds = [], [], [], []
    syncs, begins, ends, snaps, fls = [], [], [], [], []
    lmin, lmax, lavg = [], [], []
    print("composing plot ...")
    for i, m in enumerate(stats) :
        muts.append(m["mutCount"]); ups.append(m["upsertCount"])
        dels.append(m["deleteCount"]); uds.append(m["upsdelCount"])
        syncs.append(m["syncCount"]); begins.append(m["beginCount"])
        ends.append(m["endCount"])
        snaps.append(m["snapCount"]); fls.append(m["flushCount"])
        lmin.append(m["latency.min"]); lmax.append(m["latency.max"])
        lavg.append(m["latency.avg"])

    x = list(range(1, len(muts)+1))
    mode, line = "lines+markers", Line(shape='spline')
    name = "" # "%s" % raddr
    data = Data([
        Scatter(x=x, y=muts, mode=mode, name=name+"-mutations", line=line),
        Scatter(x=x, y=ups, mode=mode, name=name+"-upserts", line=line),
        Scatter(x=x, y=dels, mode=mode, name=name+"-deletes", line=line),
        Scatter(x=x, y=uds, mode=mode, name=name+"-upsdel", line=line),
        Scatter(x=x, y=syncs, mode=mode, name=name+"-syncs", line=line),
        Scatter(x=x, y=begins, mode=mode, name=name+"-begins", line=line),
        Scatter(x=x, y=ends, mode=mode, name=name+"-ends", line=line),
        Scatter(x=x, y=snaps, mode=mode, name=name+"-snaps", line=line),
        Scatter(x=x, y=fls, mode=mode, name=name+"-flushes", line=line),
        Scatter(x=x, y=lmin, mode=mode, name=name+"-latency.min", line=line),
        Scatter(x=x, y=lmax, mode=mode, name=name+"-latency.max", line=line),
        Scatter(x=x, y=lavg, mode=mode, name=name+"-latency.avg", line=line),
    ])
    print(py.plot(data, filename='endp-graph'))

def graph_load(indexes, loadstats) :
    mode, line = "lines+markers", Line(shape='spline')
    scatters = []
    for index in args.indexes.split(",") :
        loads = loadstats[index]
        x = list(range(1, len(loads)+1))
        scatters.append(
            Scatter(x=x, y=loads, mode=mode, name=index, line=line)
        )
    data = Data(scatters)
    print(py.plot(data, filename='with patch concur > poolsize'))

def graph_gsi(buckets, gsistats) :
    mode, line = "lines+markers", Line(shape='spline')
    bk = buckets[0]
    vs = gsistats[bk]
    x = list(range(1, len(vs["gsi_scan_count"])+1))
    a, b = vs["gsi_scan_count"], vs["gsi_scan_average"]
    c, d = vs["gsi_prime_average"], vs["gsi_throttle_average"]
    e = vs["gsi_blocked_average"]
    data = Data([
        Scatter(x=x, y=a, mode=mode, name=bk+"gsi_scan_count", line=line),
        Scatter(x=x, y=b, mode=mode, name=bk+"gsi_scan_average", line=line),
        Scatter(x=x, y=c, mode=mode, name=bk+"gsi_prime_average", line=line),
        Scatter(x=x, y=d, mode=mode, name=bk+"gsi_throttle_average", line=line),
        Scatter(x=x, y=e, mode=mode, name=bk+"gsi_blocked_average", line=line),
    ])
    print(py.plot(data, filename='gsi-stats'))

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
    graph_numgc(stats)

def kind_dcplatency(logfile):
    print("parsing lines ...")
    stats = []
    handler_dcpstats = lambda line, dstr : stats.append(eval(dstr.replace("NaN","None")))
    matchers = [
      [ re.compile(r'.*\[Info\].*dcp latency stats(.*)'),
        handler_dcpstats ],
    ]
    for line in open(logfile).readlines() :
        for regx, fn in matchers :
            m = regx.match(line)
            if m : tryhandler(lambda : fn(m.group(), *m.groups()))
    graph_dcplatency(stats)

def kind_idxstats(logfile) :
    print("parsing lines ...")
    stats = []

    def handler_periodicstats(lines, line, dstr) :
        lines = [ line.strip("\n") for line in lines ]
        dstr = dstr.strip("\n")
        dstr = dstr + "".join(lines[1:])
        dstr = dstr.split("}")[0] + "}"
        val = loadJson(dstr)
        stats.append(val)

    re_log = re.compile(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}T.*')
    matchers = [
      [ re.compile(r'.*\[Info\].*PeriodicStats = (.*)'),
        handler_periodicstats ],
    ]
    loglines = []
    for line in open(logfile).readlines() :
        if re_log.match(line) :
            exec_matchers(loglines, matchers)
            loglines = []
        loglines.append(line)
    exec_matchers(loglines, matchers)
    graph_idxstats(stats)

def kind_kvdata(logfile):
    print("parsing lines ...")
    opqstats = {} # opaque -> bucket -> stats
    def handler_kvdata(line, bucket, opq, dstr):
        opqstats.setdefault(opq, {}).setdefault(bucket, []).append(eval(dstr))

    matchers = [
      [ re.compile(r'.*\[Info\] KVDT\[<-(.*)<-.* (##[0-9a-z]*) stats (.*)'),
        handler_kvdata ],
    ]
    for line in open(logfile).readlines() :
        for regx, fn in matchers :
            m = regx.match(line)
            if m : tryhandler(lambda : fn(m.group(), *m.groups()))

    if len(args.opaques) == 0 or len(args.buckets) == 0 :
        [ print("for {%s,%s} - %s lines" % (opq, bucket, len(opqstats[opq])))
          for opq in sorted(opqstats.keys())
          for bucket in sorted(opqstats[opq].keys()) ]
    else :
        [ graph_kvdata(opq, opqstats[opq][bucket])
          for opq in args.opaques for bucket in args.buckets ]

def kind_endp(logfile):
    print("parsing lines ...")
    allstats = {} # (topic, raddr) -> stat
    def handler_endp(line, dstr):
        d = eval(dstr)
        allstats.setdefault((d["topic"], d["raddr"]), []).append(d)

    matchers = [
      [ re.compile(r'.*\[Info\] ENDP.* stats (.*)'),
        handler_endp ],
    ]
    for line in open(logfile).readlines() :
        for regx, fn in matchers :
            m = regx.match(line)
            if m : tryhandler(lambda : fn(m.group(), *m.groups()))

    if args.topic == "" or args.raddr == "" :
        [ print(k) for k in sorted(allstats.keys()) ]
        return

    graph_endp(args.topic, args.raddr, allstats[(args.topic, args.raddr)])

def kind_query(logfile):
    print("parsing lines ...")
    loadstats = {} # index -> stat
    def handler_loadstats(line, dstr):
        for index, value in eval(dstr).items() :
            loadstats.setdefault(index, []).append(value)

    gsistats = {} # bucket -> stat
    def handler_logstats(line, bucket, dstr):
        d = eval(dstr)
        for key, value in d.items() :
            key = key.replace("duration", "average")
            value = value / d["gsi_scan_count"]
            gsistats.setdefault(bucket, {}).setdefault(key, []).append(value)

    matchers = [
      [ re.compile(r'.*\[Info\] client load stats (.*)'),
        handler_loadstats ],
      [ re.compile(r'.*\[Info\].* logstats "(.*)" (.*)'),
        handler_logstats ],
    ]
    for line in open(logfile).readlines() :
        for regx, fn in matchers :
            m = regx.match(line)
            if m : tryhandler(lambda : fn(m.group(), *m.groups()))

    if len(args.indexes) == 0 :
        [ print(k) for k in sorted(loadstats.keys()) ]
    else :
        graph_load(args.indexes, loadstats)

    if len(args.buckets) == 0 :
        [ print(k) for k in sorted(gsistats.keys()) ]
    else :
        graph_gsi(args.buckets, gsistats)

    return


if len(args.kind) == 0 :
    print("please provide --kind")
if args.kind[0] == "dcplatency" :
    kind_dcplatency(args.logfile[0])
elif args.kind[0] == "memstats" :
    kind_memstats(args.logfile[0])
elif args.kind[0] == "idxstats" :
    kind_idxstats(args.logfile[0])
elif args.kind[0] == "kvdata" :
    kind_kvdata(args.logfile[0])
elif args.kind[0] == "endp" :
    kind_endp(args.logfile[0])
elif args.kind[0] == "query" :
    kind_query(args.logfile[0])

#{"bucket":"default","hbCount":1,"eventCount":512,"reqCount":512,"endCount":0,"snapStat.min":0,"snapStat.max":0,"snapStat.avg":-9223372036854775808,"upsertCount":0,"deleteCount":0,"ainstCount":1,"dinstCount":0,"tsCount":0}
