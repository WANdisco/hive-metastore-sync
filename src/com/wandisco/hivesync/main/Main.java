package com.wandisco.hivesync.main;

import java.io.File;

import com.beust.jcommander.JCommander;
import com.wandisco.hivesync.hive.Commands;

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

		// Delete dryRun file
		Commands.setDryRunFile(p.getDryRunFile());
		(new File(p.getDryRunFile())).delete();

		HiveSync hs = new HiveSync(p.getSrc(), p.getSrcUser(), p.getSrcPass(), p.getDst(), p.getDstUser(),
				p.getDstPass(), p.getDatabases());
		hs.execute();
	}

}
