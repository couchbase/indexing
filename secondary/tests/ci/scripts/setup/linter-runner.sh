#!/bin/bash

# Entrypoint for the linter docker-compose service.
# Mirrors container-runner.sh but defaults CONTAINER_INIT_SCRIPT to lint-loop.

perl -pi -e "s/export STORAGE=.*/export STORAGE=plasma/g" ~/.cienv

# The baked .cienv is role-neutral; inject this service's runtime config from
# the compose environment. Strip any prior role lines first so this is
# idempotent across `restart: unless-stopped` restarts (writable layer persists).
perl -ni -e 'print unless /^export (MODE|CINAME|CIBOT|TEST_NAME)=/' ~/.cienv
{
  echo "export MODE=\"${MODE:-lint}\""
  echo "export CIBOT=${CIBOT:-true}"
  echo "export CINAME=\"${CINAME:-ci2i-unstable}\""
} >> ~/.cienv

source $HOME/.cienv

echo "linter runner started"

echo '#include <unistd.h>
int main(){for(;;)pause();}
' >pause.c
gcc -o pause pause.c

[ -z "$CONTAINER_INIT_SCRIPT" ] && \
  export CONTAINER_INIT_SCRIPT="$ciscripts_dir/secondary/tests/ci/scripts/lint-loop"

echo "running $CONTAINER_INIT_SCRIPT"

bash $CONTAINER_INIT_SCRIPT &
./pause
