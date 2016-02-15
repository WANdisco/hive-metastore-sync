package com.wandisco.hivesync.events;

import java.sql.Connection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
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

import com.wandisco.hivesync.common.Tools;
import com.wandisco.hivesync.hive.Commands;

public class EventListener extends MetaStoreEventListener {
  Connection outgoing = null;

  boolean isMonitoring = false;

  private static final Logger LOG = LogManager.getLogger(EventListener.class);

  private static final String CONNECTION_STRING = "metastore.replicator.connection";

  private static final String USER = "metastore.replicator.user";

  private static final String PASS = "metastore.replicator.password";

  public EventListener(Configuration config) {
    super(config);
    try {
      outgoing = Tools.createNewConnection(config.get(CONNECTION_STRING), config.get(USER), config.get(PASS));
    } catch (Exception e) {
      isMonitoring = true;
      LOG.error("Connection string : " + config.get(CONNECTION_STRING));
      LOG.error("User : " + config.get(USER));
      LOG.error("Password : " + config.get(PASS));
      LOG.error("Cannot create outgoing connection. Only monitoring mode active!");
    }
  }

  @Override
  public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {

  }

  @Override
  public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException {

  }

  @Override
  public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {

  }

  @Override
  public void onCreateDatabase(CreateDatabaseEvent dbEvent) throws MetaException {
    LOG.trace("Create database event for : " + dbEvent.getDatabase().getName());
    if (isMonitoring)
      return;
    try {
      Commands.createDatabase(outgoing, dbEvent.getDatabase().getName());
    } catch (Exception e) {
      LOG.error("Cannot create database on remote side.", e);
    }
  }

  @Override
  public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
    Table table = tableEvent.getTable();
    String tableName = table.getTableName();
    LOG.error("Create table event for : " + tableName);
    if (isMonitoring)
      return;
    try {
      StringBuilder createCommand = new StringBuilder();
      StringBuilder fieldList = new StringBuilder();
      StringBuilder partList = new StringBuilder();
      for (FieldSchema f : table.getPartitionKeys())
        partList.append(f.getName()).append(' ');

      for (FieldSchema f : table.getSd().getCols()) {
        fieldList.append(f.getName()).append(' ').append(f.getType()).append(',');
      }
      fieldList.deleteCharAt(fieldList.length() - 1);

      createCommand.append("CREATE TABLE ").append(tableName).append('(').append(fieldList).append(')').append("PARTITIONED BY(").append(partList).append(')');
      Commands.executeQuery(outgoing, createCommand.toString());
    } catch (Exception e) {
      LOG.error("Cannot create table on remote side.", e);
    }
  }

  @Override
  public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
    LOG.trace("Drop database event for : " + dbEvent.getDatabase().getName());
    if (isMonitoring)
      return;
    try {
      Commands.dropDatabase(outgoing, dbEvent.getDatabase().getName());
    } catch (Exception e) {
      LOG.error("Cannot drop database on remote side.", e);
    }
  }

  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
    Partition partition = partitionEvent.getPartition();
    LOG.trace("Drop partition event for : " + partition.getTableName());
    if (isMonitoring)
      return;
    try {
      Table table = partitionEvent.getTable();
      Commands.dropPartition(outgoing, table.getTableName(), Tools.buildPartitionSpec(partition, table));
    } catch (Exception e) {
      LOG.error("Cannot drop partition on remote side.", e);
    }
  }

  @Override
  public void onDropTable(DropTableEvent tableEvent) throws MetaException {
    LOG.trace("Drop table event for : " + tableEvent.getTable().getTableName());
    if (isMonitoring)
      return;
    try {
      Commands.dropTable(outgoing, tableEvent.getTable().getTableName());
    } catch (Exception e) {
      LOG.error("Cannot drop table on remote side.", e);
    }
  }

}
