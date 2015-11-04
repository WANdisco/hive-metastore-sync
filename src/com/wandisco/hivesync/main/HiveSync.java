package com.wandisco.hivesync.main;

import java.sql.Connection;
import java.util.HashSet;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wandisco.hivesync.common.Tools;
import com.wandisco.hivesync.hive.Commands;
import com.wandisco.hivesync.hive.TableInfo;

/**
 * 
 * @author Oleg Danilov
 * 
 */
public class HiveSync {

	private Connection con2;
	private Connection con1;
	private List<String> dbWildcards;

	private static final Logger LOG = LogManager.getLogger(HiveSync.class);

	public HiveSync(String src, String srcUser, String srcPass, String dst, String dstUser, String dstPass,
			List<String> databases) throws Exception {
		con1 = Tools.createNewConnection(src, srcUser, srcPass);
		con2 = Tools.createNewConnection(dst, dstUser, dstPass);
		this.dbWildcards = databases;
	}

	public void execute() throws Exception {
		HashSet<String> dbList1 = new HashSet<>();
		for (String database : dbWildcards) {
			dbList1.addAll(Commands.getDatabases(con1, database));
		}
		HashSet<String> dbList2 = new HashSet<>();
		for (String database : dbWildcards) {
			dbList2.addAll(Commands.getDatabases(con2, database));
		}

		LOG.trace("Detect file system information");
		String fs1 = Commands.getFsDefaultName(con1);
		String fs2 = Commands.getFsDefaultName(con2);
		LOG.info("Source file system: " + fs1);
		LOG.info("Destination file system: " + fs2);
		
		for (String db: dbList1) {
			LOG.info("Syncing database: " + db);
			if (dbList2.contains(db)) {
				syncDatabase(db, fs1, fs2);
			} else {
				LOG.warn("destination host doesn't have database '" + db + "', ignored");
			}
		}
	}

	private void syncDatabase(String database, String fs1, String fs2) throws Exception {
		LOG.trace("Collect table information");
		
		Commands.useDatabase(con1, database);
		Commands.useDatabase(con2, database);
		
		List<TableInfo> srcTables = Commands.getTables(con1);
		List<TableInfo> dstTables = Commands.getTables(con2);

		for (TableInfo srcTable : srcTables) {
			TableInfo dstTable = findTable(dstTables, srcTable.getName());
			if (dstTable != null) {
				LOG.info("Re-create existing table: " + dstTable.getName());
				Commands.recreateTable(con2, srcTable, dstTable, fs1, fs2);
			} else {
				LOG.info("Create non-existing table: " + srcTable.getName());
				Commands.createTable(con2, srcTable, fs1, fs2);
			}
		}
		for (TableInfo dstTable : dstTables) {
			if (findTable(srcTables, dstTable.getName()) == null) {
				LOG.info("Drop table: " + dstTable.getName());
				Commands.dropTable(con2, dstTable);
			}
		}
	}

	private TableInfo findTable(List<TableInfo> tables, String tableName) {
		for (TableInfo ti : tables) {
			if (ti.getName().equals(tableName)) {
				return ti;
			}
		}
		return null;
	}

}
