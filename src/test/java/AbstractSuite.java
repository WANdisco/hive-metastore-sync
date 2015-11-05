package test.java;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Parameters;

import com.wandisco.hivesync.common.Tools;

public class AbstractSuite {
	private static Connection con1;
	private static Connection con2;
	private static String url1;
	private static String user1;
	private static String pass1;
	private static String url2;
	private static String user2;
	private static String pass2;

	@Parameters({ "box1_connect", "box1_user", "box1_password", "box2_connect", "box2_user", "box2_password" })
	@BeforeSuite
	public void setupSuite(String url1, String user1, String pass1, String url2, String user2, String pass2)
			throws Exception {
		con1 = Tools.createNewConnection(url1, user1, pass1);
		con2 = Tools.createNewConnection(url2, user2, pass2);
		AbstractSuite.url1 = url1;
		AbstractSuite.user1 = user1;
		AbstractSuite.pass1 = pass1;
		AbstractSuite.url2 = url2;
		AbstractSuite.user2 = user2;
		AbstractSuite.pass2 = pass2;
		fullCleanup("BEFORE SUITE CLEANUP");
	}

	public static void fullCleanup(String name) throws SQLException {
		fullCleanup(con1, name, url1);
		fullCleanup(con2, name, url2);
	}

	private static void fullCleanup(Connection con, String name, String url) throws SQLException {
		System.err.println(name + ": " + url);
		Statement stm1 = con.createStatement();
		Statement stm2 = con.createStatement();
		Statement stm3 = con.createStatement();
		ResultSet rs1 = stm1.executeQuery("show databases");
		while (rs1.next()) {
			String db = rs1.getString(1);
			ResultSet rs2 = stm2.executeQuery("show tables in " + db);
			while (rs2.next()) {
				String tbl = rs2.getString(1);
				System.err.println("DROP TABLE: " + db + "." + tbl);
				stm3.execute("drop table " + db + "." + tbl);
			}
			if (!"default".equals(db)) {
				System.err.println("DROP DATABASE: " + db);
				stm3.execute("drop database " + db);
			}
		}
	}

	public static String getUser1() {
		return user1;
	}

	public static String getUser2() {
		return user2;
	}

	public static String getPass1() {
		return pass1;
	}

	public static String getPass2() {
		return pass2;
	}

	public static String getUrl1() {
		return url1;
	}

	public static String getUrl2() {
		return url2;
	}

	public static Connection getCon1() {
		return con1;
	}

	public static Connection getCon2() {
		return con2;
	}

	@AfterSuite
	public void cleanupSuite() throws SQLException {
		fullCleanup("AFTER SUITE CLEANUP");
		try {
			con1.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try {
			con2.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
