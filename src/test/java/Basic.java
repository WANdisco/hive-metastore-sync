package test.java;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.wandisco.hivesync.main.HiveSync;

public class Basic extends AbstractTest {
	@BeforeClass
	public void setup() throws Exception {
		AbstractSuite.fullCleanup("BEFORE TEST CLEANUP");
		Statement s1 = con1.createStatement();
		s1.execute("create table table1 (col1 int)");
		s1.execute("create table table2 (col1 int)");
		s1.close();

		Statement s2 = con2.createStatement();
		s2.execute("create table table1 (col1 int, col2 int)");
		s2.execute("create table table3 (col1 int)");
		s2.close();
	}

	@Test
	public void f() throws Exception {
		List<String> dbs = Arrays.asList("default");
		HiveSync hs = new HiveSync(url1, user1, pass1, url2, user2, pass2, dbs);
		hs.execute();

		Statement s2 = con2.createStatement();
		checkResult(s2, "show tables", new String[] { "table1", "table2" });
		checkResult(s2, "describe table1", new String[] { "col1" });
		s2.close();
	}

	@AfterClass
	public void cleanup() throws SQLException {
		AbstractSuite.fullCleanup("AFTER TEST CLEANUP");
	}
}
