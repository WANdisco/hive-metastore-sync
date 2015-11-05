# hive-metastore-sync
Replicate hive metastore from the cluster to another one

## How to build?
* Run maven build:
```
mvn clean package -DskipTests
```

## How to test?
* Create two single-node clusters with hadoop and hive. Expected host names: box1 & box2
* Start hadoop and hive on each one
* Run the tests:

```
mvn test
```

## Creating test boxes with vagrant-lxc

To simplify the testing process, there is a vagrant-lxc template which could be used to create box1 and box2.
You have to install vagrant (https://www.vagrantup.com) and vagrant-lxc plugin (https://github.com/fgrehm/vagrant-lxc) to use it. Then run the following commands:

```
cd vagrant/lxc
vagrant up
```

This command creates and runs two containers: box1 and box2, both provisioned with hadoop and hive.
To setup a password-less ssh access:

```
ssh-copy-id vagrantbox1
ssh-copy-id vagrant@box2
```

To remove created containers use ```destroy```:

```
vagrant destroy
```
