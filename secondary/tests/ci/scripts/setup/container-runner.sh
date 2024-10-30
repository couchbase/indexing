#!/bin/bash
source $HOME/.cienv

echo "container runner started"

echo '#include <unistd.h>
int main(){for(;;)pause();}
' >pause.c
gcc -o pause pause.c

[ -z "$CONTAINER_INIT_SCRIPT" ] && export CONTAINER_INIT_SCRIPT="$ciscripts_dir/secondary/tests/ci/scripts/build"

echo "running $CONTAINER_INIT_SCRIPT"

bash $CONTAINER_INIT_SCRIPT &
./pause
