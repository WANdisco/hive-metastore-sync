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

@Path("/config")
public class ConfigurationResource {

  @Path("/connection")
  @POST
  public void setConnectionString(String conn) {
    EventsQueue.setConnectionString(conn);
  }

  @Path("/user")
  @POST
  public void setUser(String user) {
    EventsQueue.setUser(user);
  }

  @Path("/pass")
  @GET
  public void setPass(String pass) {
    EventsQueue.setPass(pass);
  }

}
