package com.wandisco.hivesync.main;

import com.beust.jcommander.JCommander;

/**
 * 
 * @author Oleg Danilov
 * 
 */
public class Main {

	private static Params p;

	public static void main(String[] args) throws Exception {
		p = new Params();
		JCommander jce = new JCommander(p, args);

		if (p.getHelp() != null) {
			jce.setProgramName("hive-metastore-sync");
			jce.usage();
			return;
		}

		HiveSync hs = new HiveSync(p.getSrc(), p.getSrcUser(), p.getSrcPass(), p.getDst(), p.getDstUser(),
				p.getDstPass(), p.getDatabases());
		hs.execute();
	}

}
