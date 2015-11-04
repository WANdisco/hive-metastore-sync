#!/usr/bin/env bash
echo "=== Restarting services..."

ssh vagrant@box1 "/usr/local/hadoop-2.6.0/sbin/stop-all.sh"
ssh vagrant@box1 "ps auxw | grep HiveServer2 | awk '{ print \$2; }' | xargs kill"
ssh vagrant@box1 "/usr/local/hadoop-2.6.0/sbin/start-all.sh"
ssh vagrant@box1 "nohup /usr/local/apache-hive-1.2.1-bin/bin/hiveserver2 >/dev/null 2>&1 &"

ssh vagrant@box2 "/usr/local/hadoop-2.6.0/sbin/stop-all.sh"
ssh vagrant@box2 "ps auxw | grep HiveServer2 | awk '{ print \$2; }' | xargs kill"
ssh vagrant@box2 "/usr/local/hadoop-2.6.0/sbin/start-all.sh"
ssh vagrant@box2 "nohup /usr/local/apache-hive-1.2.1-bin/bin/hiveserver2 >/dev/null 2>&1 &"

echo "=== Waiting 5 seconds..."
sleep 5

(cd test1 && ./test1.sh)
(cd test2 && ./test2.sh)

echo "=== ALL TESTS ARE PASSED ==="
