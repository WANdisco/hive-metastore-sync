package test.java;

import java.io.IOException;
import java.net.Socket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Parameters;

import com.wandisco.hivesync.common.Tools;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Session.Command;

public class AbstractSuite {
	private static Connection con1;
	private static Connection con2;
	private static String user1;
	private static String pass1;
	private static String user2;
	private static String pass2;
	private static String url1;
	private static String url2;

	@Parameters({ "box1_host", "box1_jdbc_port", "box1_user", "box1_password", "box2_host", "box2_jdbc_port",
			"box2_user", "box2_password", "hadoop_home", "hive_home" })
	@BeforeSuite
	public void setupSuite(String host1, String port1, String user1, String pass1, String host2, String port2,
			String user2, String pass2, String hadoopHome, String hiveHome) throws Exception {
		runHadoop(host1, hadoopHome, user1, pass1);
		runHadoop(host2, hadoopHome, user2, pass2);
		runHive(host1, port1, hiveHome, user1, pass1);
		runHive(host2, port2, hiveHome, user2, pass2);
		url1 = "jdbc:hive2://" + host1 + ":" + port1;
		url2 = "jdbc:hive2://" + host2 + ":" + port2;
		con1 = Tools.createNewConnection(url1, user1, pass1);
		con2 = Tools.createNewConnection(url2, user2, pass2);
		AbstractSuite.user1 = user1;
		AbstractSuite.pass1 = pass1;
		AbstractSuite.user2 = user2;
		AbstractSuite.pass2 = pass2;
		fullCleanup("BEFORE SUITE CLEANUP");
	}

	private void runHive(String host, String port, String hiveHome, String user, String password)
			throws IOException, InterruptedException {
		SSHClient ssh = new SSHClient();
		ssh.loadKnownHosts();
		ssh.connect(host);
		ssh.authPassword(user, password);
		Session session = ssh.startSession();
		Command cmd = session.exec("ps auxw | grep HiveServer2 | grep -v grep | awk '{ print $2; }' | xargs kill -9");
		cmd.join(5, TimeUnit.SECONDS);
		session.close();

		System.err.println("RUNNING HIVE ON " + host);

		session = ssh.startSession();
		cmd = session.exec("nohup " + hiveHome + "/bin/hiveserver2 > /dev/null 2>&1 &");
		cmd.join();
		for (int i = 0; i < 30; i++) {
			if (serverListening(host, Integer.parseInt(port))) {
				break;
			}
			System.err.print(".");
			Thread.sleep(1000);
		}
		System.err.println();
		session.close();

		ssh.disconnect();
		ssh.close();

	}

	public static boolean serverListening(String host, int port) {
		Socket s = null;
		try {
			s = new Socket(host, port);
			return true;
		} catch (Exception e) {
			return false;
		} finally {
			if (s != null)
				try {
					s.close();
				} catch (Exception e) {
				}
		}
	}

	private void runHadoop(String host, String hadoopHome, String user, String password) throws IOException {
		System.err.println("RUNNING HADOOP ON " + host);
		SSHClient ssh = new SSHClient();
		ssh.loadKnownHosts();
		ssh.connect(host);
		ssh.authPassword(user, password);
		Session session = ssh.startSession();
		Command cmd = session.exec(hadoopHome + "/sbin/start-all.sh");
		cmd.join(30, TimeUnit.SECONDS);
		session.close();
		ssh.disconnect();
		ssh.close();
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

	public static String getUrl1() {
		return url1;
	}
	
	public static String getUrl2() {
		return url2;
	}
}
