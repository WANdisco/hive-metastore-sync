package com.wandisco.hivesync.rest;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;

@Path("/database")
public class DatabaseResource {

  @Path("/create")
  @POST
  @Consumes(value = { "text/xml", "application/json" })
  public void createTable(CreateTableEvent cte) {
    EventsQueue.tableEvents.add(cte);
  }

  @Path("/drop")
  @POST
  @Consumes(value = { "text/xml", "application/json" })
  public void dropTable(DropTableEvent ate) {
    EventsQueue.tableEvents.add(ate);
  }

  @Path("/list")
  @GET
  public List<String> getPendingList() {
    List<String> res = new ArrayList<String>();
    for (ListenerEvent c : EventsQueue.tableEvents) {
      StringBuilder sb = new StringBuilder();
      if (c instanceof CreateDatabaseEvent) {
        CreateDatabaseEvent cte = (CreateDatabaseEvent) c;
        sb.append("Create database event : ").append(cte.getDatabase().getName());
      }
      if (c instanceof DropDatabaseEvent) {
        DropDatabaseEvent dte = (DropDatabaseEvent) c;
        sb.append("Drop database event : ").append(dte.getDatabase().getName());
      }
      res.add(sb.toString());
    }
    return res;
  }

}
