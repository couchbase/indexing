from __future__ import with_statement
from __future__ import print_function
import fabric
import fabric.utils
from fabric.api import *
from fabric.contrib.console import confirm
import os

# cluster identifications.
#   i-5a39a497: admin@ec2-122-248-204-207.ap-southeast-1.compute.amazonaws.com
#   i-4339a48e: admin@ec2-54-179-76-33.ap-southeast-1.compute.amazonaws.com
#   i-5239a49f: admin@ec2-54-179-233-148.ap-southeast-1.compute.amazonaws.com
#   i-4839a485: admin@ec2-54-254-63-224.ap-southeast-1.compute.amazonaws.com
# Root is on network drive. So please be sure to configure couchbase to store
# data at /data and index at /index (which are two local SSDs).

host207 = "ec2-122-248-204-207.ap-southeast-1.compute.amazonaws.com"
host33  = "ec2-54-179-76-33.ap-southeast-1.compute.amazonaws.com"
host148 = "ec2-54-179-233-148.ap-southeast-1.compute.amazonaws.com"
host224 = "ec2-54-254-63-224.ap-southeast-1.compute.amazonaws.com"
nodes = {
    host33  : ["data", "loadgen"],
    host207 : ["data", "loadgen", "test"],
    host148 : ["data", "index", "loadgen"],
    host224 : ["data", "query", "loadgen"],
}
cb_services = ["data", "index", "query"]
cluster_node = host33 # node treated as the cluster-node

def setenv(attr="", force=None, default=None) :
    """set environment attributes defined below"""
    if force :
        setattr(env, attr, force)
    elif attr != "" and (not hasattr(env, attr) or not getattr(env, attr)) :
        setattr(env, attr, default)

map(lambda kwargs: setenv(**kwargs), [
    {"attr": "user", "force": "admin"},
    {"attr": "hosts", "default": nodes.keys()},
    {"attr": "colorize_errors", "default": True},
    {"attr": "pkgdir2i", "default": "/opt/cbpkg"},
    {"attr": "cmdlog2i", "default": False},
    {"attr": "user2i", "default": "Administrator"},
    {"attr": "passw2i", "default": "asdasd"},
    {"attr": "ramsize2i", "default": 8192},
    {"attr": "cluster2i", "default": cluster_node},
    {"attr": "gopath2i", "default": "/opt/goproj"},
    {"attr": "goroot2i", "default": ""},
])

fabric.state.output["running"] = False
fabric.state.output["stdout"] = False

#--- tasks

@task
@parallel
def uname():
    """uname returns the OS installed on all remote nodes"""
    trycmd("uname -s", v=True)

@task
@parallel
def setup():
    """setup target nodes in the cluster
    - create package dir under /opt
    - create gopath dir under /opt
    - install os packages
    - install golang 1.3.3 version
    - go get github.com/couchbase/indexing repository and all its deps
    """
    trycmd("mkdir -p %s" % env.pkgdir2i, op="sudo")
    trycmd("chown %s:%s %s" % (env.user, env.user, env.pkgdir2i), op="sudo")
    trycmd("mkdir -p %s" % env.gopath2i, op="sudo")
    trycmd("chown %s:%s %s" % (env.user, env.user, env.gopath2i), op="sudo")
    trycmd("apt-get install git mercurial --assume-yes", op="sudo")
    install_golang()
    with shell_env(GOPATH=env.gopath2i, GOROOT=env.goroot2i) :
        trycmd("go get -d github.com/couchbase/indexing/...")

@task
@parallel
def fix_dpkg() :
    """fix dpkg in case of broken ssh connection"""
    trycmd("dpkg --configure -a", op="sudo")

@task
@parallel
def cb_install(url=""):
    """install specified couchbase version from tar file"""
    pp = pp_for_host(env.host_string)
    if url == "" :
        pp("error please provide a url")
        return
    installfile = url.split("/")[-1]
    commands = [
        ["rm -f couchbase-server* installer*", {}],
        ["wget %s" % url, {}],
        ["tar xvf %s" % installfile, {}],
        ["rm -rf /opt/couchbase/", {"op":"sudo"}],
        ["dpkg -i couchbase-server_*", {"op":"sudo"}],
        ["dpkg -i couchbase-server-dbg*", {"op":"sudo"}],
    ]
    with cd(env.pkgdir2i) :
        all(map(lambda x: trycmd(x[0], **x[1]), commands))

@task
@parallel
def cb_uninstall():
    """uninstall couchbase server and debug symbols"""
    trycmd("dpkg -r couchbase-server-dbg", op="sudo")
    trycmd("dpkg -r couchbase-server", op="sudo")

fmt_cluster_init = "\
./couchbase-cli cluster-init \
--cluster=%s:8091 --cluster-username=%s --cluster-password=%s \
--cluster-ramsize=8192 -d --services=%s"
fmt_server_add = "\
./couchbase-cli rebalance --cluster=%s:8091 --user=%s --password=%s \
--server-add=%s --server-add-username=%s --server-add-password=%s \
--services='%s'"

@task
@hosts(cluster_node)
def cluster_init():
    """initialize couchbase cluster and rebalance them"""
    pp = pp_for_host(env.host_string)
    with cd("/opt/couchbase/bin"):
        ss = ";".join(filter(lambda s: s in cb_services, nodes[env.host_string]))
        params = (env.host_string, env.user2i, env.passw2i, ss)
        cmd = fmt_cluster_init % params
        trycmd(cmd, op="run")
        for node in nodes :
            if node == env.host_string : continue
            ss = ";".join(filter(lambda s: s in cb_services, nodes[node]))
            params = (env.cluster2i,  env.user2i, env.passw2i, node,
                      env.user2i, env.passw2i, ss)
            cmd = fmt_server_add % params
            trycmd(cmd, op="run")


fmt_create_bucket = "\
./couchbase-cli bucket-create \
--cluster=%s:8091 --user=%s --password=%s \
--bucket=%s \
--bucket-password="" \
--bucket-ramsize=%s \
--bucket-replica=1 \
--bucket-type=couchbase \
--enable-flush=1 \
--wait"

@task
@hosts(cluster_node)
def create_buckets(buckets="default", ramsize="4096"):
    """create one or more buckets"""
    for bucket in buckets.split(",") :
        params = (env.cluster2i, env.user2i, env.passw2i, bucket, ramsize)
        with cd("/opt/couchbase/bin"):
            cmd = fmt_create_bucket % params
            trycmd(cmd, op="run")

fmt_loadgen = "\
go run ./loadgen.go -bagdir %s -count %s -par %s -buckets %s -prods %s %s"

@task
@parallel
def loadgen(count=100, par=1, buckets="default", prods="users.prod") :
    """genetate load over couchbase buckets"""
    if "loadgen" not in nodes[env.host_string] :
        return
    path = os.sep.join(
        [env.gopath2i, "src", "github.com", "couchbase", "indexing",
         "secondary", "tools", "loadgen"])
    bagdir = os.sep.join(
        [env.gopath2i, "src", "github.com", "prataprc", "monster", "bags"])
    prodpath = os.sep.join(
        [env.gopath2i, "src", "github.com", "prataprc", "monster", "prods"])
    cluster = "http://%s:%s@%s:8091" % (env.user2i, env.passw2i, env.cluster2i)
    prodfiles = [ os.sep.join([prodpath, prod]) for prod in prods.split(",") ]
    prodfiles = ",".join(list(prodfiles))
    with shell_env(GOPATH=env.gopath2i, GOROOT=env.goroot2i), cd(path) :
        params = (bagdir, count, par, buckets, prodfiles, cluster)
        trycmd(fmt_loadgen % params, op="run")

@task
@parallel
def indexing_master():
    """switch to github.com/couchbase/indexing:master branch on all nodes"""
    path =os.sep.join([env.gopath2i,"src","github.com","couchbase","indexing"])
    with cd(path) :
        trycmd("git checkout master")

@task
@parallel
def indexing_unstable():
    """switch to github.com/couchbase/indexing:unstable branch on all nodes"""
    path =os.sep.join([env.gopath2i,"src","github.com","couchbase","indexing"])
    with cd(path) :
        trycmd("git checkout unstable")

#---- local functions

def install_golang():
    tarlink = "https://storage.googleapis.com/golang/go1.3.3.linux-amd64.tar.gz"
    with cd(env.pkgdir2i):
        trycmd("wget %s" % tarlink, op="sudo")
        trycmd("tar -C /usr/local -xzf go1.3.3.linux-amd64.tar.gz", op="sudo")
        trycmd('echo "PATH=/usr/local/go/bin:$PATH" >> /etc/profile', op="sudo")

def pp_for_host(host_string) :
    def fn(*args, **kwargs) :
        msg = "[%s] " % host_string
        msg += " ".join(map(str, args))
        msg += "\n".join(map(lambda k, v: "   %s: %s" % (k, v), kwargs.items()))
        if msg.lower().find("error") > 0 :
            fabric.utils.error(msg)
        else :
            print(msg)
    return fn

def trycmd(cmd, op="run", v=False):
    pp = pp_for_host(env.host_string)
    out = {"sudo": sudo, "run": run, "local": local}[op](cmd) # execute

    if env.cmdlog2i :
        logfile = env.host_string + ".log"
        open(logfile, "a").write("%s\n%s\n\n" % (cmd, out))

    if out.failed :
        pp("cmd failed: %s" % cmd)
        pp(out)
        return out.failed
    elif v :
        pp(cmd, ":", out)
    else :
        pp(cmd, ": ok")
    return out.succeeded
