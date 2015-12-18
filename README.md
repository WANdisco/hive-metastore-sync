# hive-metastore-sync
Replicate hive metastore from the cluster to another one

## How to build?
* Run maven build:
```
mvn clean package -DskipTests
```

Maven will produce two results:

1. directory ```target/hive-metastore-sync-<version>/```
2. zip archive ```target/hive-metastore-sync-<version>.zip```

## How to test?
* Create two single-node clusters, expected host names: box1.lxc & box2.lxc
* Install hadoop and hive on each one
* Run the tests:

```
mvn test
```

Test suite starts/restarts hadoop and hive every time you run it. You can cut this time by specifying parameter ```skipStart```:

```
mvn test -DskipStart=true
````

##Running hive-metastore-sync

To run hive-metastore-sync from shell:

```
<install-dir>/bin/hivesync [parameters]
```

##Configuration
Log4j2 configuration is stored in ```<install-dir>/conf/log4j2.xml```
The default configuration file produces log file ```/tmp/hive-metastore-sync.txt```

## Creating test boxes with vagrant-lxc

To simplify the testing process, there is a vagrant-lxc template which could be used to create box1 and box2.
You have to install lxc, vagrant (https://www.vagrantup.com) and vagrant-lxc plugin (https://github.com/fgrehm/vagrant-lxc) to use it. Then run the following commands:

```
cd vagrant/lxc
vagrant up
```

This command creates and runs two containers: box1.lxc and box2.lxc, both provisioned with hadoop and hive.

To remove created containers use ```destroy```:

```
vagrant destroy
```
