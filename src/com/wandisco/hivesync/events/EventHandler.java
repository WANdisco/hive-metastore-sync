package com.wandisco.hivesync.events;

import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;

import com.wandisco.hivesync.rest.EventsQueue;

public class EventHandler extends Thread {
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
    running = true; //xxx

    while (running) {
      ListenerEvent e = EventsQueue.tableEvents.poll();
      if (e == null) {
        try {
          wait(1000);
        } catch (InterruptedException e1) {
        }
        continue;
      }

      if (e instanceof CreateTableEvent) {
        CreateTableEvent cte = (CreateTableEvent) e;

      }
      if (e instanceof AlterTableEvent) {
        AlterTableEvent ate = (AlterTableEvent) e;

      }
      if (e instanceof DropTableEvent) {
        DropTableEvent dte = (DropTableEvent) e;

      }
      if (e instanceof AddPartitionEvent) {
        AddPartitionEvent dte = (AddPartitionEvent) e;

      }
      if (e instanceof AlterPartitionEvent) {
        AlterPartitionEvent dte = (AlterPartitionEvent) e;

      }
      if (e instanceof DropPartitionEvent) {
        DropPartitionEvent dte = (DropPartitionEvent) e;

      }
      if (e instanceof CreateDatabaseEvent) {
        CreateDatabaseEvent cte = (CreateDatabaseEvent) e;

      }
      if (e instanceof DropDatabaseEvent) {
        DropDatabaseEvent dte = (DropDatabaseEvent) e;

      }

    }
  }
}
