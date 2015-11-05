package com.wandisco.hivesync.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wandisco.hivesync.common.Tools;
import com.wandisco.hivesync.main.HiveSync;

public abstract class Commands {

	private static final Logger LOG = LogManager.getLogger(HiveSync.class);

	public static void createTable(Connection con, TableInfo table, String fs1, String fs2) throws Exception {

		boolean dryRun = false;

		createTableWithPartitions(con, table, fs1, fs2, dryRun);

		if (table.isManaged()) {
			Commands.executeQuery(con, "ALTER TABLE " + table.getName() + " SET TBLPROPERTIES('EXTERNAL'='FALSE')",
					dryRun);
		} else {
			Commands.executeQuery(con, "ALTER TABLE " + table.getName() + " SET TBLPROPERTIES('EXTERNAL'='TRUE')",
					dryRun);
		}
	}

	private static void createTableWithPartitions(Connection con, TableInfo table, String fs1, String fs2,
			boolean dryRun) throws Exception {
		String cc = table.getCreateCommand().replace(fs1, fs2);

		Commands.executeQuery(con, cc, dryRun);
		for (String partition : table.getPartitions()) {
			LOG.debug("- create partition: " + partition);
			Commands.executeQuery(con, "ALTER TABLE " + table.getName() + " ADD PARTITION (" + partition + ")");
		}
	}

	public static void dropTable(Connection con, TableInfo table) throws Exception {
		Commands.executeQuery(con, "DROP TABLE " + table.getName());
	}

	public static void recreateTable(Connection con, TableInfo src, TableInfo dst, String fs1, String fs2)
			throws Exception {

		boolean dryRun = false;

		if (dst.isManaged()) {
			LOG.debug("- change table to unmanaged");
			Commands.executeQuery(con, "ALTER TABLE " + dst.getName() + " SET TBLPROPERTIES('EXTERNAL'='TRUE')",
					dryRun);
		}

		LOG.debug("- drop existing table keeping data");
		Commands.executeQuery(con, "DROP TABLE " + dst.getName(), dryRun);
		LOG.debug("- create table");
		createTableWithPartitions(con, src, fs1, fs2, dryRun);

		if (src.isManaged()) {
			LOG.debug("- change table to managed");
			Commands.executeQuery(con, "ALTER TABLE " + dst.getName() + " SET TBLPROPERTIES('EXTERNAL'='FALSE')",
					dryRun);
		} else {
			LOG.debug("- change table to unmanaged");
			Commands.executeQuery(con, "ALTER TABLE " + dst.getName() + " SET TBLPROPERTIES('EXTERNAL'='TRUE')",
					dryRun);
		}
	}

	public static ArrayList<TableInfo> getTables(Connection connection) throws Exception {
		ArrayList<TableInfo> tablesInfo = new ArrayList<>();
		List<String> srcTables = Commands.executeQuery(connection, "SHOW TABLES");
		for (String srcTable : srcTables) {
			// Generate CreateCommand

			List<String> createCommand = queryCreateCommand(connection, srcTable);

			// Check if it is managed table

			boolean isManaged = queryIsManaged(connection, srcTable);

			// Get partitions

			List<String> partitions = queryPartitions(connection, srcTable);

			TableInfo ti = new TableInfo(srcTable, Tools.join(createCommand), partitions, isManaged);

			tablesInfo.add(ti);
		}
		return tablesInfo;
	}

	private static List<String> queryCreateCommand(Connection connection, String srcTable) throws Exception {
		List<String> createCommand = Commands.executeQuery(connection, "SHOW CREATE TABLE " + srcTable);
		Iterator<String> i = createCommand.iterator();
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
		List<String> descFormatted = Commands.executeQuery(con, "DESC FORMATTED " + tableName);
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
			return Commands.executeQuery(con, "SHOW PARTITIONS " + tableName);
		} catch (SQLException e) {
			return new ArrayList<>();
		}
	}

	public static String getFsDefaultName(Connection con) throws Exception {
		String result = Tools.join(Commands.executeQuery(con, "SET fs.default.name"));
		if (!result.startsWith("fs.default.name=")) {
			throw new Exception("Can't detect file system");
		}
		return result.replaceAll("fs.default.name=", "");
	}

	public final static List<String> executeQuery(Connection con, String query) throws Exception {
		return executeQuery(con, query, false);
	}

	public final static List<String> executeQuery(Connection con, String query, boolean dryRun) throws Exception {
		List<String> al = new ArrayList<>();
		Statement s = con.createStatement();
		ResultSet rs = null;
		try {
			LOG.trace("Executing: " + query);
			if (dryRun) {
				return al;
			}
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
		return Commands.executeQuery(connection, "SHOW DATABASES LIKE '" + pattern + "'");
	}

	public static void useDatabase(Connection con, String database) throws Exception {
		LOG.trace("Selecting database " + database);
		Commands.executeQuery(con, "USE " + database);
	}

}
