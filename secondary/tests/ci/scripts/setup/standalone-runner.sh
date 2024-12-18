#!/bin/bash

# this file is supposed to be a trimmed down version of dobuild
# dobuild clones gerrit patches. here the expectation is that
# all the clone has happened already in the $WORKSPACE

source $HOME/.cienv

if [ ! -d "/mnt/project/goproj/src/github.com/couchbase/indexing" ];
then
	domain
	exit $?
fi;

echo "cloning custom repo into $WORKSPACE"
cp -r /mnt/project/* $WORKSPACE
cd $WORKSPACE/goproj/src/github.com/couchbase/indexing
cp -r secondary/tests/ci/scripts/* /home/bot/bin/

export PATH=/home/bot/bin:$PATH

echo "running standalone runner - $WORKSPACE"

export TS="$(date +%d.%m.%Y-%H.%M)"
echo '<html><head></head><body><pre>'
echo 'Building using ' $(which builder) '...'

cd $WORKSPACE
make clean
rm -rf build
rm -rf analytics/CMakeLists.txt

# add 2ici_test flag
sed -i 's/SET (TAGS "jemalloc")/SET (TAGS "jemalloc 2ici_test")/' $WORKSPACE/goproj/src/github.com/couchbase/indexing/CMakeLists.txt

builder
rc=$?
echo "builder exit code $rc"
test $rc -eq 0 || (cat $WORKSPACE/make.log && exit 2)

echo 'Build done'
echo 'Testing using ' $(which dotest) '...'

dotest
rc=$?
echo 'Test done'
echo '</pre>'

if [ $rc -eq 0 ]; then status=pass; else status=fail; fi
echo '<pre>'
gzip ${WORKSPACE}/logs.tar 2>&1 1>/dev/null
echo "Version: <a href='versions-$TS.cfg'>versions-$TS.cfg</a>"
echo "Build Log: <a href='make-$TS.log'>make-$TS.log</a>"
echo "Server Log: <a href='logs-$TS.tar.gz'>logs-$TS.tar.gz</a>"
echo "</pre><h1>Finished</h1></body></html>"

cp $WORKSPACE/logs.tar /mnt/project/
cp $WORKSPACE/make.log /mnt/project/
cp $WORKSPACE/test.log /mnt/project/
