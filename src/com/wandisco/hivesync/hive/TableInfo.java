package com.wandisco.hivesync.hive;

import java.util.List;

public class TableInfo {

  private String name;
  private String createCommand;
  private boolean isManaged;
  private List<String> partitions;

  public TableInfo(String name, String createCommand, List<String> partitions, boolean isManaged) {
    this.name = name;
    this.createCommand = createCommand;
    this.partitions = partitions;
    this.isManaged = isManaged;
  }

  public String getName() {
    return name;
  }

  public String getCreateCommand() {
    return createCommand;
  }

  public boolean isManaged() {
    return isManaged;
  }

  @Override
  public String toString() {
    return "Table: " + name + " Managed: " + isManaged + " Create Command: " + createCommand;
  }

  public List<String> getPartitions() {
    return partitions;
  }

}
