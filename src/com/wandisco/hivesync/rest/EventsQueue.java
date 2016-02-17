package com.wandisco.hivesync.rest;

import java.util.LinkedList;

import org.apache.hadoop.hive.metastore.events.ListenerEvent;

public class EventsQueue {
  public static final LinkedList<ListenerEvent> tableEvents = new LinkedList<ListenerEvent>();

  public static String CONNECTION_STRING = null;

  public static String USER = null;

  public static String PASS = null;

  public static void setConnectionString(String conn) {
    CONNECTION_STRING = conn;
  }

  public static void setUser(String user) {
    USER = user;
  }

  public static void setPass(String pass) {
    PASS = pass;
  }

}
