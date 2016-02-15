package com.wandisco.hivesync.rest;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;

@Path("/table")
public class TableResource {

  @Path("/create")
  @POST
  @Consumes(value = { "text/xml", "application/json" })
  public void createTable(CreateTableEvent cte) {
    EventsQueue.tableEvents.add(cte);
  }

  @Path("/alter")
  @POST
  @Consumes(value = { "text/xml", "application/json" })
  public void alterTable(AlterTableEvent ate) {
    EventsQueue.tableEvents.add(ate);
  }

  @Path("/drop")
  @POST
  @Consumes(value = { "text/xml", "application/json" })
  public void dropTable(DropTableEvent ate) {
    EventsQueue.tableEvents.add(ate);
  }

  @Path("/partition_add")
  @POST
  @Consumes(value = { "text/xml", "application/json" })
  public void addPartition(AddPartitionEvent ape) {
    EventsQueue.tableEvents.add(ape);
  }

  @Path("/partition_alter")
  @POST
  @Consumes(value = { "text/xml", "application/json" })
  public void alterPartition(AlterPartitionEvent ape) {
    EventsQueue.tableEvents.add(ape);
  }

  @Path("/partition_drop")
  @POST
  @Consumes(value = { "text/xml", "application/json" })
  public void dropPartition(DropPartitionEvent dpe) {
    EventsQueue.tableEvents.add(dpe);
  }

  @Path("/list")
  @GET
  public List<String> getPendingList() {
    List<String> res = new ArrayList<String>();
    for (ListenerEvent c : EventsQueue.tableEvents) {
      StringBuilder sb = new StringBuilder();
      if (c instanceof CreateTableEvent) {
        CreateTableEvent cte = (CreateTableEvent) c;
        sb.append("Create table event : ").append(cte.getTable().getDbName()).append(".").append(cte.getTable().getTableName());
      }
      if (c instanceof AlterTableEvent) {
        AlterTableEvent ate = (AlterTableEvent) c;
        sb.append("Alter table event : ").append(ate.getNewTable().getDbName()).append(".").append(ate.getNewTable().getTableName());
      }
      if (c instanceof DropTableEvent) {
        DropTableEvent dte = (DropTableEvent) c;
        sb.append("Drop table event : ").append(dte.getTable().getDbName()).append(".").append(dte.getTable().getTableName());
      }
      if (c instanceof AddPartitionEvent) {
        AddPartitionEvent dte = (AddPartitionEvent) c;
        sb.append("Add partition event : ").append(dte.getTable().getDbName()).append(".").append(dte.getTable().getTableName());
      }
      if (c instanceof AlterPartitionEvent) {
        AlterPartitionEvent dte = (AlterPartitionEvent) c;
        sb.append("Alter partition event : ").append(dte.getNewPartition().getDbName()).append(".").append(dte.getNewPartition().getTableName());
      }
      if (c instanceof DropPartitionEvent) {
        DropPartitionEvent dte = (DropPartitionEvent) c;
        sb.append("Drop partiotion event : ").append(dte.getTable().getDbName()).append(".").append(dte.getTable().getTableName());
      }
      res.add(sb.toString());
    }
    return res;
  }

}
