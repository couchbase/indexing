import sys
import re

def checkcalls(logfile) :
    enter_re = re.compile(r'.*\[Info\].*PROJ.*(##[a-f0-9]*).*do.*".*')
    exit_re = re.compile(r'.*\[Info\].*PROJ.*(##[a-f0-9]*).*do.*return')
    tokens = {}
    for line in open(logfile).readlines() :
        m = enter_re.match(line)
        if m :
            token = m.groups()[0]
            tokens[token] = (True, line)
        m = exit_re.match(line)
        if m :
            token = m.groups()[0]
            del tokens[token]

    if len(tokens) > 0 :
        print("pending projector calls ...")
        keys = sorted([ int(token[2:], 16) for token in tokens.keys() ])
        for key in keys :
            key = "##%s"%hex(key)[2:]
            print(key, tokens[key])
    else :
        print("no pending calls")

checkcalls(sys.argv[1])
