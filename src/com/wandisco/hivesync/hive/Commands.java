package com.wandisco.hivesync.hive;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wandisco.hivesync.common.Tools;
import com.wandisco.hivesync.main.HiveSync;

public abstract class Commands {

  private static final Logger LOG = LogManager.getLogger(HiveSync.class);

  private static String dryRunFile = null;

  public static void setDryRunFile(String name) {
    dryRunFile = name;
  }

  public static void createTable(Connection con, TableInfo table, String fs1, String fs2)
      throws Exception {

    createTableWithPartitions(con, table, fs1, fs2);

    if (table.isManaged()) {
      Commands.executeQuery(con,
          "ALTER TABLE " + table.getName() + " SET TBLPROPERTIES('EXTERNAL'='FALSE')");
    } else {
      Commands.executeQuery(con,
          "ALTER TABLE " + table.getName() + " SET TBLPROPERTIES('EXTERNAL'='TRUE')");
    }
  }

  private static void createTableWithPartitions(Connection con, TableInfo table, String fs1,
      String fs2) throws Exception {
    String cc = table.getCreateCommand().replace(fs1, fs2);

    Commands.executeQuery(con, cc);
    for (String partition : table.getPartitions()) {
      LOG.debug("- create partition: " + partition);
      Commands.executeQuery(con,
          "ALTER TABLE " + table.getName() + " ADD PARTITION (" + partition + ")");
    }
  }

  public static void dropTable(Connection con, TableInfo table) throws Exception {
    Commands.executeQuery(con, "DROP TABLE " + table.getName());
  }

  public static void recreateTable(Connection con, TableInfo src, TableInfo dst, String fs1,
      String fs2) throws Exception {

    if (dst.isManaged()) {
      LOG.debug("- change table to unmanaged");
      Commands.executeQuery(con,
          "ALTER TABLE " + dst.getName() + " SET TBLPROPERTIES('EXTERNAL'='TRUE')");
    }

    LOG.debug("- drop existing table keeping data");
    Commands.executeQuery(con, "DROP TABLE " + dst.getName());
    LOG.debug("- create table");
    createTableWithPartitions(con, src, fs1, fs2);

    if (src.isManaged()) {
      LOG.debug("- change table to managed");
      Commands.executeQuery(con,
          "ALTER TABLE " + dst.getName() + " SET TBLPROPERTIES('EXTERNAL'='FALSE')");
    } else {
      LOG.debug("- change table to unmanaged");
      Commands.executeQuery(con,
          "ALTER TABLE " + dst.getName() + " SET TBLPROPERTIES('EXTERNAL'='TRUE')");
    }
  }

  public static ArrayList<TableInfo> getTables(Connection connection, String database)
      throws Exception {
    ArrayList<TableInfo> tablesInfo = new ArrayList<>();
    List<String> srcTables = Commands.forceExecuteQuery(connection, "SHOW TABLES IN " + database);
    for (String srcTable : srcTables) {
      // Generate CreateCommand

      List<String> createCommand = queryCreateCommand(connection, database + "." + srcTable);

      // Check if it is managed table

      boolean isManaged = queryIsManaged(connection, database + "." + srcTable);

      // Get partitions

      List<String> partitions = queryPartitions(connection, database + "." + srcTable);

      TableInfo ti = new TableInfo(database + "." + srcTable, Tools.join(createCommand), partitions,
          isManaged);

      tablesInfo.add(ti);
    }
    return tablesInfo;
  }

  private static List<String> queryCreateCommand(Connection connection, String srcTable)
      throws Exception {
    List<String> createCommand =
        Commands.forceExecuteQuery(connection, "SHOW CREATE TABLE " + srcTable);
    Iterator<String> i = createCommand.iterator();
    // Strip TBLPROPERIES
    boolean remove = false;
    while (i.hasNext()) {
      String s = i.next();
      if (remove) {
        i.remove();
        if (s.endsWith(")")) {
          remove = false;
        }
      } else {
        if (s.startsWith("TBLPROPERTIES (")) {
          i.remove();
          remove = true;
        }
      }
    }
    return createCommand;
  }

  private static boolean queryIsManaged(Connection con, String tableName) throws Exception {
    List<String> descFormatted = Commands.forceExecuteQuery(con, "DESC FORMATTED " + tableName);
    for (String s : descFormatted) {
      if (s.startsWith("Table Type:")) {
        if (s.contains("MANAGED_TABLE")) {
          return true;
        }
      }
    }
    return false;
  }

  private static List<String> queryPartitions(Connection con, String tableName) throws Exception {
    try {
      return Commands.forceExecuteQuery(con, "SHOW PARTITIONS " + tableName);
    } catch (SQLException e) {
      return Collections.emptyList();
    }
  }

  public static String getFsDefaultName(Connection con) throws Exception {
    String result = Tools.join(Commands.forceExecuteQuery(con, "SET fs.default.name"));
    if (!result.startsWith("fs.default.name=")) {
      throw new Exception("Can't detect file system");
    }
    return result.replaceAll("fs.default.name=", "");
  }

  public final static List<String> executeQuery(Connection con, String query) throws Exception {
    if (dryRunFile != null) {
      LOG.trace("Dry run: " + query);
      try (
          PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(dryRunFile, true)))) {
        out.println(query);
      }
      return Collections.emptyList();
    } else {
      return forceExecuteQuery(con, query);
    }
  }

  private final static List<String> forceExecuteQuery(Connection con, String query)
      throws Exception {
    List<String> al = new ArrayList<>();
    Statement s = con.createStatement();
    ResultSet rs = null;
    try {
      LOG.trace("Executing: " + query);
      s.execute(query);
      rs = s.getResultSet();

      if (rs == null) {
        LOG.trace("Empty result.");
      } else {
        LOG.trace("Result:");
        int colN = rs.getMetaData().getColumnCount();
        while (rs.next()) {
          StringBuffer sb = new StringBuffer();
          for (int cid = 1; cid <= colN; cid++) {
            sb.append(rs.getString(cid));
            if (cid < colN) {
              sb.append("\t");
            }
          }
          al.add(sb.toString());
          LOG.trace(sb.toString());
        }
      }
    } finally {
      if (rs != null) {
        rs.close();
      }
      s.close();
    }
    return al;
  }

  public static List<String> getDatabases(Connection connection, String pattern) throws Exception {
    LOG.trace("Getting database list");
    return Commands.forceExecuteQuery(connection, "SHOW DATABASES LIKE '" + pattern + "'");
  }

  public static void createDatabase(Connection con, String db) throws Exception {
    LOG.trace("Creating database");
    executeQuery(con, "CREATE DATABASE " + db);
  }

}
