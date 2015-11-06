package com.wandisco.hivesync.main;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;

/**
 * Command line parameters
 * 
 * @author Oleg Danilov
 * 
 */
public class Params {

	@Parameter(names = { "-?", "--help" }, description = "Show help", help = true)
	private Boolean help;

	@Parameter(names = "-src", description = "Connection string ('hiveserver' and 'hiveserver2' can be used as default connection string for the corresponding services)", required = true)
	private String src;

	@Parameter(names = "-srcUser", description = "User")
	private String srcUser;

	@Parameter(names = "-srcPass", description = "Password")
	private String srcPass;

	@Parameter(names = "-dst", description = "Connection string ('hiveserver' and 'hiveserver2' can be used as default connection string for the corresponding services)", required = true)
	private String dst;

	@Parameter(names = "-dstUser", description = "User")
	private String dstUser;

	@Parameter(names = "-dstPass", description = "Password")
	private String dstPass;

	@Parameter(names = "-database", description = "Database(s), comma-separated list with wildcards")
	private List<String> databases = new ArrayList<>(Arrays.asList("default"));

	@Parameter(names = "-dryRun", description = "Don't run, but output commands to the specified file")
	private String dryRunFile = null;

	public Boolean getHelp() {
		return help;
	}

	public String getSrc() {
		return src;
	}

	public String getSrcUser() {
		return srcUser;
	}

	public String getSrcPass() {
		return srcPass;
	}

	public String getDst() {
		return dst;
	}

	public String getDstUser() {
		return dstUser;
	}

	public String getDstPass() {
		return dstPass;
	}

	public List<String> getDatabases() {
		return databases;
	}

	public String getDryRunFile() {
		return dryRunFile;
	}

}
