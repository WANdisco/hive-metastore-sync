#!/usr/bin/env bash
scp *box1*.q vagrant@box1:~
scp *box2*.q vagrant@box2:~

BEELINE="/usr/local/apache-hive-1.2.1-bin/bin/beeline -u jdbc:hive2://localhost:10000 -n vagrant -p vagrant "

echo "=== Creating tables..."

echo "=== BOX1"
ssh vagrant@box1 "$BEELINE -f test2.box1.q"

echo "=== BOX2"
ssh vagrant@box2 "$BEELINE -f test2.box2.q"

echo "=== Syncing..."

../../target/hive-metastore-sync-0.0.1-SNAPSHOT/hivesync -src jdbc:hive2://box1:10000/default -srcUser vagrant -srcPass vagrant -dst jdbc:hive2://box2:10000/default -dstUser vagrant -dstPass vagrant -database db*

echo "=== Checking..."

ssh vagrant@box2 "$BEELINE -f test2.box2.check.q" | grep -vi "time" > /tmp/test2.box2.result.log

echo "=== Comparing..."

diff /tmp/test2.box2.result.log test2.box2.result.txt
if [ $? -eq 0 ]
then
   echo "=== Files are the same"
   ssh vagrant@box1 "$BEELINE -f test2.box1.cleanup.q"
   ssh vagrant@box2 "$BEELINE -f test2.box2.cleanup.q"
else
   echo "=== Files differ"
   exit 1
fi
