from __future__ import with_statement
from __future__ import print_function
import fabric
import fabric.utils
from fabric.api import *
from fabric.contrib.console import confirm
import os
import time

# cluster identifications.
#   i-5a39a497: admin@ec2-122-248-204-207.ap-southeast-1.compute.amazonaws.com
#   i-4339a48e: admin@ec2-54-179-76-33.ap-southeast-1.compute.amazonaws.com
#   i-5239a49f: admin@ec2-54-179-233-148.ap-southeast-1.compute.amazonaws.com
#   i-4839a485: admin@ec2-54-254-63-224.ap-southeast-1.compute.amazonaws.com
# Root is on network drive. So please be sure to configure couchbase to store
# data at /data and index at /index (which are two local SSDs).

user = "admin"
pkgdir2i = "/opt/cbpkg"
user2i = "Administrator"
passw2i = "asdasd"
ramsize2i = 8192
gopath2i = "/opt/goproj"
goroot2i = "/usr/local/go"
install2i = "/opt/couchbase"
binpath2i = "/opt/goproj/bin:$PATH"
# TODO: will IP address change with node restart ?
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
    {"attr": "user", "force": user},
    {"attr": "hosts", "default": nodes.keys()},
    {"attr": "colorize_errors", "default": True},
    {"attr": "pkgdir2i", "default": pkgdir2i},
    {"attr": "cmdlog2i", "default": False},
    {"attr": "user2i", "default": user2i},
    {"attr": "passw2i", "default": passw2i},
    {"attr": "ramsize2i", "default": ramsize2i},
    {"attr": "cluster2i", "default": cluster_node},
    {"attr": "gopath2i", "default": gopath2i},
    {"attr": "goroot2i", "default": goroot2i},
    {"attr": "install2i", "default": install2i},
    {"attr": "binpath2i", "default": binpath2i},
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
    packages = "git mercurial libsasl2-2 sasl2-bin gcc cmake make " \
               "libsnappy-dev g++"
    trycmd("apt-get install %s --assume-yes" % packages, op="sudo")

    install_golang()

    # install 2i repository and all its dependencies
    with shell_env(GOPATH=env.gopath2i, GOROOT=env.goroot2i) :
        trycmd("go get -d github.com/couchbase/indexing/...")

    install_protobuf()

@task
@parallel
def indexing_master():
    """switch to github.com/couchbase/indexing:master branch on all nodes"""
    repo2i =os.sep.join([env.gopath2i,"src","github.com","couchbase","indexing"])
    with cd(repo2i) :
        trycmd("git checkout .")
        trycmd("git clean -f -d")
        trycmd("git checkout master")
        trycmd("git pull --rebase origin master")

@task
@parallel
def indexing_unstable():
    """switch to github.com/couchbase/indexing:unstable branch on all nodes"""
    path =os.sep.join([env.gopath2i,"src","github.com","couchbase","indexing"])
    with cd(path) :
        trycmd("git checkout .")
        trycmd("git clean -f -d")
        trycmd("git checkout unstable")
        trycmd("git pull --rebase origin unstable")

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

@task
@parallel
def cb_service(do="restart"):
    """start/stop/restart couchbase server"""
    trycmd("/etc/init.d/couchbase-server %s" % do, op="sudo")

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
    """create one or more buckets (input received as csv of buckets)"""
    for bucket in buckets.split(",") :
        params = (env.cluster2i, env.user2i, env.passw2i, bucket, ramsize)
        with cd("/opt/couchbase/bin"):
            cmd = fmt_create_bucket % params
            trycmd(cmd, op="run")

@task
@parallel
def patch_target(R1="",abort=False):
    """patch the target node
    - if R1 is provided, `format-patch` to apply revisions from R1 to target.
      else, `diff` will be used to apply the uncommited patch to target.
    - if abort, then any incomplete patch on the target will be aborted.
    """
    pp = pp_for_host(env.host_string)
    path =os.sep.join([env.gopath2i,"src","github.com","couchbase","indexing"])

    with cd(path):
        if abort :
            trycmd("git am --abort", v=True)
            return

    if R1 == "" :
        patchfile = "/tmp/patch-%s-%s.diff" % (env.host_string, str(time.time()))
        pp("patchfile: %s" % patchfile)
        cmd = "git diff > %s" % patchfile
    else :
        patchfile = "/tmp/patch-%s-%s.am" % (env.host_string, str(time.time()))
        pp("patchfile: %s" % patchfile)
        cmd = "git format-patch -k %s..HEAD --stdout > %s" % (R1, patchfile)
    trycmd(cmd, op="local")

    put(patchfile, patchfile)

    with cd(path):
        trycmd("git checkout .")
        trycmd("git clean -f -d")
        if R1 == "" :
            trycmd("git apply %s" % patchfile)
        else :
            trycmd("git am -3 -k < %s || git am --abort" % patchfile)

@task
@parallel
def rebuild_forestdb():
    """rebuild and install forestdb to remote's source path"""
    path = os.sep.join([env.pkgdir2i, "forestdb"])
    with cd(env.pkgdir2i), shell_env(GOPATH=env.gopath2i, GOROOT=env.goroot2i) :
        trycmd("rm -rf %s" % path)
        trycmd("git clone https://github.com/couchbase/forestdb.git")

    with cd(path), shell_env(GOPATH=env.gopath2i, GOROOT=env.goroot2i) :
        trycmd("mkdir -p build")
        trycmd("cd build; cmake ..; cd ..")
        trycmd("cd build; make; cd ..")
        trycmd("cd build; make install; cd ..", op="sudo")
        target = os.sep.join([env.install2i, "lib"])
        trycmd("cp build/libforestdb.so %s" % target, op="sudo")
        trycmd("ldconfig", op="sudo")

@task
@parallel
def rebuild_indexing(R1=""):
    """patch indexing and rebuild projector and indexer"""
    if R1 : patch_target(R1=R1)
    patch_target()

    path = os.sep.join([env.gopath2i,"src","github.com","couchbase","indexing"])
    target = os.sep.join([install2i, "bin"])
    with cd(path), shell_env(PATH=binpath2i,GOPATH=env.gopath2i, GOROOT=env.goroot2i):
        trycmd("cd secondary; ./build.sh; cd ..", v=True)
        trycmd("mv secondary/cmd/projector/projector %s" % target, op="sudo", v=True)
        trycmd("mv secondary/cmd/indexer/indexer %s" % target, op="sudo", v=True)


fmt_loadgen = "\
GOMAXPROCS=%s go run ./loadgen.go -auth %s:%s -bagdir %s -count %s -par %s \
-buckets %s -prods %s %s"

@task
@parallel
def loadgen(count=100, par=1, procs=4, buckets="default", prods="users.prod") :
    """genetate load over couchbase buckets"""
    repopath = os.sep.join(["src", "github.com", "couchbase", "indexing"])
    path_loadgen = os.sep.join(["secondary", "tools", "loadgen"])
    path = os.sep.join([env.gopath2i, repopath, path_loadgen])
    path_monster = os.sep.join(["src", "github.com", "prataprc", "monster"])
    bagdir = os.sep.join([env.gopath2i, path_monster, "bags"])
    prodpath = os.sep.join([env.gopath2i, path_monster, "prods"])

    cluster = "http://%s:8091" % env.cluster2i
    prodfiles = [ os.sep.join([prodpath, prod]) for prod in prods.split(",") ]
    prodfiles = ",".join(list(prodfiles))
    with shell_env(GOPATH=env.gopath2i, GOROOT=env.goroot2i), cd(path) :
        params = (
            procs, user2i, passw2i, bagdir, count, par, buckets, prodfiles,
            cluster)
        trycmd(fmt_loadgen % params, op="run")

@task
@parallel
def gitcmd(path="", cmd=""):
    """run a git command on all nodes"""
    if cmd == "" :
        return
    if path == "" :
        path = os.sep.join([env.gopath2i,"src","github.com","couchbase","indexing"])
    with cd(path), shell_env(GOPATH=env.gopath2i, GOROOT=env.goroot2i) :
        trycmd(cmd, v=True)


@task
@parallel
def cleanall():
    trycmd("rm -rf /opt/cbpkg", op="sudo")
    trycmd("rm -rf /opt/goproj", op="sudo")
    trycmd("rm -f /tmp/patch*", op="sudo")


#---- local functions

def install_golang():
    tarlink = "https://storage.googleapis.com/golang/go1.3.3.linux-amd64.tar.gz"
    with cd(env.pkgdir2i):
        trycmd("wget %s" % tarlink, op="sudo")
        trycmd("tar -C /usr/local -xzf go1.3.3.linux-amd64.tar.gz", op="sudo")
        #trycmd('echo "PATH=/usr/local/go/bin:$PATH" >> /etc/profile', op="sudo")

def install_protobuf():
    packages = "protobuf-compiler"
    trycmd("apt-get install %s --assume-yes" % packages, op="sudo")
    path = os.sep.join([gopath2i, "src", "code.google.com", "p", "goprotobuf"])
    with shell_env(GOPATH=env.gopath2i, GOROOT=env.goroot2i):
        trycmd("go get -d github.com/couchbase/indexing/...")
    with cd(path), shell_env(GOPATH=env.gopath2i, GOROOT=env.goroot2i) :
        trycmd("go install ./...", v=True)

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
        pp(cmd, ":\n", out)
    else :
        pp(cmd, ": ok")
    return out.succeeded
