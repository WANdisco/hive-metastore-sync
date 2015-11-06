package test.java;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.wandisco.hivesync.hive.Commands;
import com.wandisco.hivesync.hive.TableInfo;

public class CommandsTest extends AbstractTest {
	@BeforeClass
	public void setup() throws Exception {
		AbstractSuite.fullCleanup("BEFORE TEST CLEANUP");

		Statement s1 = con1.createStatement();
		s1.execute("create database db1");
		s1.execute("create table db1.table1 (col1 int)");
		s1.execute("create database db2");
		s1.execute("create table db2.table2 (col1 int)");
		s1.execute("create database db3");
		s1.execute("create table db3.table3 (col1 int)");
		s1.close();

		Statement s2 = con2.createStatement();
		s2.execute("create database db1");
		s2.execute("create table db1.table1 (col1 int, col2 int)");
		s2.execute("create database db3");
		s2.close();
	}

	@Test
	public void getDatabases() throws Exception {
		List<String> result = Commands.getDatabases(con1, "db*");
		Assert.assertEquals(result.size(), 3);
		Assert.assertTrue(result.contains("db1"));
		Assert.assertTrue(result.contains("db2"));
		Assert.assertTrue(result.contains("db3"));
	}

	@Test
	public void getTables() throws Exception {
		List<TableInfo> result = Commands.getTables(con1, "db1");
		Assert.assertEquals(result.size(), 1);
		Assert.assertEquals(result.get(0).getName(), "db1.table1");
		result = Commands.getTables(con1, "db2");
		Assert.assertEquals(result.size(), 1);
		Assert.assertEquals(result.get(0).getName(), "db2.table2");
	}

	@AfterClass
	public void cleanup() throws SQLException {
		AbstractSuite.fullCleanup("AFTER TEST CLEANUP");
	}
}
