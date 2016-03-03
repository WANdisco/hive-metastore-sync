package com.wandisco.hivesync.events;

import java.sql.Connection;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;

public class EventListener extends MetaStoreEventListener {
  Connection outgoing = null;

  private static final Logger LOG = LogManager.getLogger(EventListener.class);

  private static final String REPL_SERVER = "metastore.replicator";

  private static final String CONNECTION = "metastore.replicator.connection";

  private static final String USER = "metastore.replicator.user";

  private static final String PASS = "metastore.replicator.password";

  private static IQueue<ReplicationEvent> processingQueue = null;
  private static Map<String, String> configuration = null;

  public EventListener(Configuration config) {
    super(config);
    try {
      Config cfg = new Config();
      HazelcastInstance hc = Hazelcast.newHazelcastInstance(cfg);
      processingQueue = hc.getQueue("PROCESSING_QUEUE");

      configuration = hc.getMap("CONFIG");
      configuration.put("CONNECTION", config.get(CONNECTION));
      configuration.put("USER", config.get(USER));
      configuration.put("PASS", config.get(PASS));
    } catch (Exception e) {
      LOG.error("Cannot connect to replication server!");
    }
  }

  @Override
  public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
    LOG.trace("Add partition event for : " + partitionEvent.getTable().getTableName());

    try {
      AddPartEvent ape = new AddPartEvent(partitionEvent.getTable(), partitionEvent.getPartitions());

      processingQueue.add(ape);
    } catch (Exception e) {
      LOG.error("Cannot add partition on remote side.", e);
    }

  }

  @Override
  public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException {
    LOG.trace("Alter partition event for : " + partitionEvent.getOldPartition().getTableName());

    try {
      AlterPartEvent ape = new AlterPartEvent(partitionEvent.getNewPartition(), partitionEvent.getOldPartition());
      processingQueue.add(ape);
    } catch (Exception e) {
      LOG.error("Cannot alter partition table on remote side.", e);
    }
  }

  @Override
  public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
    LOG.trace("Alter table event for : " + tableEvent.getNewTable().getTableName());

    try {
      AlterTBEvent cde = new AlterTBEvent(tableEvent.getNewTable(), tableEvent.getOldTable());
      processingQueue.add(cde);
    } catch (Exception e) {
      LOG.error("Cannot alter table on remote side.", e);
    }

  }

  @Override
  public void onCreateDatabase(CreateDatabaseEvent dbEvent) throws MetaException {
    LOG.trace("Create database event for : " + dbEvent.getDatabase().getName());

    try {
      CreateDBEvent cde = new CreateDBEvent(dbEvent.getDatabase());
      processingQueue.add(cde);
    } catch (Exception e) {
      LOG.error("Cannot create database on remote side.", e);
    }
  }

  @Override
  public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
    LOG.error("Create table event for : " + tableEvent.getTable().getTableName());

    try {
      CreateTBEvent cte = new CreateTBEvent(tableEvent.getTable());
      processingQueue.add(cte);
    } catch (Exception e) {
      LOG.error("Cannot create table on remote side.", e);
    }
  }

  @Override
  public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
    LOG.trace("Drop database event for : " + dbEvent.getDatabase().getName());

    try {
      DropDBEvent dde = new DropDBEvent(dbEvent.getDatabase());
      processingQueue.add(dde);
    } catch (Exception e) {
      LOG.error("Cannot drop database on remote side.", e);
    }
  }

  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
    Partition partition = partitionEvent.getPartition();
    LOG.trace("Drop partition event for : " + partition.getTableName());

    try {
      DropPartEvent dte = new DropPartEvent(partitionEvent.getTable(), partitionEvent.getPartition(), partitionEvent.getDeleteData());
      processingQueue.add(dte);
    } catch (Exception e) {
      LOG.error("Cannot drop partition on remote side.", e);
    }
  }

  @Override
  public void onDropTable(DropTableEvent tableEvent) throws MetaException {
    LOG.trace("Drop table event for : " + tableEvent.getTable().getTableName());

    try {
      DropTBEvent dte = new DropTBEvent(tableEvent.getTable(), tableEvent.getDeleteData());
      processingQueue.add(dte);
    } catch (Exception e) {
      LOG.error("Cannot drop table on remote side.", e);
    }
  }

}
