package com.wandisco.hivesync.events;

import java.sql.Connection;

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
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wandisco.hivesync.common.Tools;
import com.wandisco.hivesync.hive.Commands;
import com.wandisco.hivesync.rest.EventsQueue;

public class EventHandler extends Thread {

  Connection outgoing = null;

  boolean isMonitoring = false;

  private static final Logger LOG = LogManager.getLogger(EventHandler.class);

  boolean running = false;

  public EventHandler() {
  }

  public boolean isRunning() {
    return running;
  }

  public void shutdown() {
    running = false;
  }

  public void run() {
    running = true;

    while (running) {
      try {
        ListenerEvent e = EventsQueue.tableEvents.poll();
        if (e == null) {
          wait(1000);
          continue;
        }

        if (e instanceof CreateTableEvent) {
          CreateTableEvent cte = (CreateTableEvent) e;
          onCreateTable(cte);
        }
        if (e instanceof AlterTableEvent) {
          AlterTableEvent ate = (AlterTableEvent) e;
          onAlterTable(ate);
        }
        if (e instanceof DropTableEvent) {
          DropTableEvent dte = (DropTableEvent) e;
          onDropTable(dte);
        }
        if (e instanceof AddPartitionEvent) {
          AddPartitionEvent dte = (AddPartitionEvent) e;
          onAddPartition(dte);
        }
        if (e instanceof AlterPartitionEvent) {
          AlterPartitionEvent dte = (AlterPartitionEvent) e;
          onAlterPartition(dte);
        }
        if (e instanceof DropPartitionEvent) {
          DropPartitionEvent dte = (DropPartitionEvent) e;
          onDropPartition(dte);
        }
        if (e instanceof CreateDatabaseEvent) {
          CreateDatabaseEvent cte = (CreateDatabaseEvent) e;
          onCreateDatabase(cte);
        }
        if (e instanceof DropDatabaseEvent) {
          DropDatabaseEvent dte = (DropDatabaseEvent) e;
          onDropDatabase(dte);
        }
      } catch (Exception e1) {
      }

    }
  }

  public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {

  }

  public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException {

  }

  public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {

  }

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
