package com.wandisco.hivesync.common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Utility class
 * 
 * @author Oleg Danilov
 * 
 */
public class Tools {

	/**
	 * Read file line by line and return list of strings
	 **/
	public final static List<String> readTextFile(String filename) {
		ArrayList<String> list = new ArrayList<>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line;
			while ((line = br.readLine()) != null) {
				list.add(line);
			}
			br.close();
		} catch (Exception _) {
			System.err.println("Can't read file " + filename);
		}
		return list;
	}

	public final static String join(List<String> list) {
		StringBuffer sb = new StringBuffer();
		Iterator<String> iter = list.iterator();
		while (iter.hasNext()) {
			sb.append(iter.next());
			if (iter.hasNext()) {
				sb.append(System.getProperty("line.separator"));
			}
		}
		return sb.toString();
	}

	public final static Connection createNewConnection(String connectionString, String userName, String password)
			throws Exception {
		if (connectionString.startsWith("jdbc:hive2"))
			Class.forName("org.apache.hive.jdbc.HiveDriver");
		else
			Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
		return DriverManager.getConnection(connectionString, userName, password);
	}

}
