# fab --version
# fab -h

# fab -i <pem-file> -f 2ifab.py -u ubuntu -H <host> <command>

# to list all commands,
#
# $ fab -f 2ifab.py -l
#

# to list remote machine detail,
#
# $ fab -i <pem> -u <user> -H <host> -f 2ifab.py uname
#

# to setup remote machine directories, packages, golang, clone 2i repo.
#
# $ fab -i <pem> -u <user> -H <host> -f 2ifab.py setup
#

# to cleanup remote machine,
#
# $ fab -i <pem> -u <user> -H <host> -f 2ifab.py cleanall
#

# to uninstall and install couchbase,
#
# fab -i <pem> -f 2ifab.py cb_uninstall cb_install:url=<url-link>
#

# to stop, start or restart couchbase service,
#
# fab -i <pem> -u <user> -H <host> -f 2ifab.py cb_service:do=stop
# fab -i <pem> -u <user> -H <host> -f 2ifab.py cb_service:do=start
# fab -i <pem> -u <user> -H <host> -f 2ifab.py cb_service:do=restart

# to initialize a node and start a cluster,
#
# fab -i <pem> -f 2ifab.py cluster_init:services="data\,index"
#
# to initialize a node and add to a cluster,
#
# fab -i <pem> -f 2ifab.py server_add:services="data\,index",cluster=<node>
#

# to create one or more buckets,
#
# $ fab -i <pem> -f 2ifab.py create_buckets:buckets="default\,users"
#

# to load documents,
#
# fab -i <pem> -f 2ifab.py loadgen:procs=32,count=625000,par=2
#

from __future__ import with_statement
from __future__ import print_function
import fabric
import fabric.utils
from fabric.api import *
from fabric.contrib.console import confirm
import os
import time

packages = [
    "git", "mercurial", "libsasl2-2", "sasl2-bin", "gcc", "cmake", "make",
    "libsnappy-dev", "g++", "protobuf-compiler",
]

(user2i, passw2i) = "Administrator", "asdasd"
ramsize2i = 8192
pkgdir = "/opt/pkgs"
installdir = "/opt/couchbase"
goproj, godeps = "/opt/goproj", "/opt/godeps"
gopath = ":".join([goproj, godeps])
goroot = "/usr/local/go"
shpath = goroot + "/bin" + ":$PATH"

fabric.state.output["running"] = False
fabric.state.output["stdout"] = False

#---- node tasks

@task
@parallel
def uname():
    """uname returns the OS installed on all remote nodes"""
    trycmd("uname -a", v=True)


govers = {
    "133": "https://storage.googleapis.com/golang/go1.3.3.linux-amd64.tar.gz",
    "141": "https://storage.googleapis.com/golang/go1.5.1.linux-amd64.tar.gz",
    "143": "https://storage.googleapis.com/golang/go1.4.3.linux-amd64.tar.gz",
}
@task
@parallel
def setup(gover="143"):
    """setup target nodes in the cluster
    - create package dir under /opt
    - create gopath dir under /opt
    - install os packages
    - install golang 1.3.3 version
    - go get github.com/couchbase/indexing repository and all its deps
    """
    for package in packages :
        trycmd("apt-get install %s --assume-yes" % package, op="sudo")

    for d in [pkgdir, installdir, goproj, godeps] :
        trycmd("rm -rf %s" % d, op="sudo")
        trycmd("mkdir -p %s" % d, op="sudo")
        trycmd("chown %s:%s %s" % (env.user, env.user, d), op="sudo")

    # install golang
    trycmd("rm -rf %s" % goroot, op="sudo") # first un-install
    with cd(pkgdir):
        link = govers[gover]
        targz = link.split("/")[-1]
        trycmd("wget %s" % link, op="sudo")
        trycmd("tar -C /usr/local -xzf %s" % targz, op="sudo")
    with shell_env(PATH=shpath, GOPATH=gopath, GOROOT=goroot):
         trycmd("go version", v=True)

    # clone 2i repository and all its dependencies
    with shell_env(GOPATH=gopath, GOROOT=goroot) :
        trycmd("go get -d github.com/couchbase/indexing/...")

    # set up protobuf
    path = os.sep.join([goproj, "src", "github.com", "golang", "protobuf"])
    with cd(path), shell_env(GOPATH=gopath, GOROOT=goroot) :
        trycmd("go install ./...", v=True)

    ## set up loadgen dependencies
    #path = os.sep.join([
    #    goproj, "src", "github.com", "couchbase", "indexing",
    #    "secondary", "tools", "loadgen"
    #])
    #with cd(path), shell_env(GOPATH=gopath, GOROOT=goroot) :
    #    trycmd("go get ./...", v=True)

@task
@parallel
def cleanall():
    for d in [pkgdir, installdir, goproj, godeps] :
        trycmd("rm -rf %s" %d , op="sudo")
    trycmd("rm -f /tmp/patch*", op="sudo")
    trycmd("rm -rf %s" % goroot, op="sudo")


@task
@parallel
def fix_dpkg() :
    """fix dpkg in case of broken ssh connection"""
    trycmd("dpkg --configure -a", op="sudo")


#---- coucbase node tasks

@task
@parallel
def cb_install(url=""):
    """install specified couchbase version from tar file"""
    pp = pp_for_host(env.host)

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
    with cd(pkgdir) :
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
--cluster-ramsize=%s -d --services=%s"
@task
def cluster_init(services="",ramsize=8192):
    """initialize couchbase cluster and rebalance them"""
    pp = pp_for_host(env.host)
    if services == "" :
        print("please provide the services to start for this node")

    with cd("/opt/couchbase/bin"):
        # cluster-init
        params = (env.host, user2i, passw2i, ramsize, services)
        cmd = fmt_cluster_init % params
        trycmd(cmd, op="run")

fmt_server_add = "\
./couchbase-cli rebalance --cluster=%s:8091 --user=%s --password=%s \
--server-add=%s --server-add-username=%s --server-add-password=%s \
--services='%s'"
@task
def server_add(services="", cluster=""):
    pp = pp_for_host(env.host)
    if services == "" :
        print("please provide the services to start for this node")
        return
    if cluster == "" :
        print("please provide the cluster address to this node")
        return

    with cd("/opt/couchbase/bin"):
        # server-add
        cmd = fmt_server_add % (cluster,  user2i, passw2i, env.host,
              user2i, passw2i, services)
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
def create_buckets(buckets="", ramsize="2048"):
    """create one or more buckets (input received as csv of buckets)"""
    if buckets == "" :
        print("please provided comma-separated list of buckets to create")

    for bucket in buckets.split(",") :
        with cd("/opt/couchbase/bin"):
            cmd = fmt_create_bucket % (env.host, user2i, passw2i, bucket, ramsize)
            trycmd(cmd, op="run")

#---- patching and building target

fmt_loadgen = "\
GOMAXPROCS=%s go run ./loadgen.go -auth %s:%s -bagdir %s -count %s -par %s \
-buckets %s -prods %s %s"

@task
@parallel
def loadgen(procs=4, count=100, par=1, buckets="default", prods="projects.prod") :
    """genetate load over couchbase buckets"""
    repopath = os.sep.join(["src", "github.com", "couchbase", "indexing"])
    path_loadgen = os.sep.join(["secondary", "tools", "loadgen"])
    path = os.sep.join([goproj, repopath, path_loadgen])

    path_monster = os.sep.join(["src", "github.com", "prataprc", "monster"])
    bagdir = os.sep.join([goproj, path_monster, "bags"])
    prodpath = os.sep.join([goproj, path_monster, "prods"])

    cluster = "%s:8091" % env.host
    prodfiles = [ os.sep.join([prodpath, prod]) for prod in prods.split(",") ]
    prodfiles = ",".join(list(prodfiles))
    with shell_env(GOPATH=gopath, GOROOT=goroot), cd(path) :
        params = (
            procs, user2i, passw2i, bagdir, count, par, buckets, prodfiles,
            cluster)
        trycmd(fmt_loadgen % params, op="run")

@task
@parallel
def indexing_master():
    """switch to github.com/couchbase/indexing:master branch on all nodes"""
    repo2i =os.sep.join([gopath,"src","github.com","couchbase","indexing"])
    with cd(repo2i) :
        trycmd("git checkout .")
        trycmd("git clean -f -d")
        trycmd("git checkout master")
        trycmd("git pull --rebase origin master")

@task
@parallel
def indexing_unstable():
    """switch to github.com/couchbase/indexing:unstable branch on all nodes"""
    path =os.sep.join([gopath,"src","github.com","couchbase","indexing"])
    with cd(path) :
        trycmd("git checkout .")
        trycmd("git clean -f -d")
        trycmd("git checkout unstable")
        trycmd("git pull --rebase origin unstable")

@task
@parallel
def patch_target(R1="",abort=False):
    """patch the target node
    - if R1 is provided, `format-patch` to apply revisions from R1 to target.
      else, `diff` will be used to apply the uncommited patch to target.
    - if abort, then any incomplete patch on the target will be aborted.
    """
    pp = pp_for_host(env.host)
    path =os.sep.join([gopath,"src","github.com","couchbase","indexing"])

    with cd(path):
        if abort :
            trycmd("git am --abort", v=True)
            return

    if R1 == "" :
        patchfile = "/tmp/patch-%s-%s.diff" % (env.host, str(time.time()))
        pp("patchfile: %s" % patchfile)
        cmd = "git diff > %s" % patchfile
    else :
        patchfile = "/tmp/patch-%s-%s.am" % (env.host, str(time.time()))
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
    path = os.sep.join([pkgdir, "forestdb"])
    with cd(pkgdir), shell_env(GOPATH=gopath, GOROOT=goroot) :
        trycmd("rm -rf %s" % path)
        trycmd("git clone https://github.com/couchbase/forestdb.git")

    with cd(path), shell_env(GOPATH=gopath, GOROOT=goroot) :
        trycmd("mkdir -p build")
        trycmd("cd build; cmake ..; cd ..")
        trycmd("cd build; make; cd ..")
        trycmd("cd build; make install; cd ..", op="sudo")
        target = os.sep.join([installdir, "lib"])
        trycmd("cp build/libforestdb.so %s" % target, op="sudo")
        trycmd("ldconfig", op="sudo")

@task
@parallel
def rebuild_indexing(R1=""):
    """patch indexing and rebuild projector and indexer"""
    if R1 : patch_target(R1=R1)
    patch_target()

    path = os.sep.join([gopath,"src","github.com","couchbase","indexing"])
    target = os.sep.join([installdir, "bin"])
    with cd(path), shell_env(PATH=binpath2i,GOPATH=gopath, GOROOT=goroot):
        trycmd("cd secondary; ./build.sh; cd ..", v=True)
        trycmd("mv secondary/cmd/projector/projector %s" % target, op="sudo", v=True)
        trycmd("mv secondary/cmd/indexer/indexer %s" % target, op="sudo", v=True)


@task
@parallel
def gitcmd(path="", cmd=""):
    """run a git command on all nodes"""
    if cmd == "" :
        return
    if path == "" :
        path = os.sep.join([gopath,"src","github.com","couchbase","indexing"])
    with cd(path), shell_env(GOPATH=gopath, GOROOT=goroot) :
        trycmd(cmd, v=True)

#---- local functions

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
    pp = pp_for_host(env.host)
    out = {"sudo": sudo, "run": run, "local": local}[op](cmd) # execute

    if out.failed :
        pp("cmd failed: %s" % cmd)
        pp(out)
        return out.failed
    elif v :
        pp(cmd, ":\n", out)
    else :
        pp(cmd, ": ok")
    return out.succeeded

