package com.wandisco.hivesync.rest;

import java.util.LinkedList;

import org.apache.hadoop.hive.metastore.events.ListenerEvent;

public class EventsQueue {
  public static final LinkedList<ListenerEvent> tableEvents = new LinkedList<ListenerEvent>();

}
