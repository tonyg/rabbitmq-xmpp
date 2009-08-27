#!/bin/sh

which spec >/dev/null
[ "$?" = "1" ] && echo "Test cases require ruby and the rspec gem to be installed" && exit 1

SPEC_DIR=`dirname $0`
#make -C $SPEC_DIR/../deps/ejabberd start-ejabberd
OK=true

spec -c -f s $SPEC_DIR/adapter.rb || OK=false

#make -C $SPEC_DIR/../deps/ejabberd stop-ejabberd
$OK
