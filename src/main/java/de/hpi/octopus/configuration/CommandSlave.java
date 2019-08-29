package de.hpi.octopus.configuration;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(commandDescription = "start a slave actor system")
public class CommandSlave extends Command {

	@Override
	int getDefaultPort() {
		return Configuration.DEFAULT_SLAVE_PORT;
	}

	@Parameter(names = { "-mh", "--masterhost" }, description = "The host name or IP of the master", required = true)
	String masterhost;

	@Parameter(names = { "-mp", "--masterport" }, description = "The port of the master", required = false)
	int masterport = Configuration.DEFAULT_MASTER_PORT;

	@Parameter(names = { "-pcpl", "--pliCachePrefixLength" }, description = "The maximum number of lhs prefix attributes for which the FD candidate validation should calculate and cache intermediate plis; e.g. for prefix 3 and candidate ABCD->E, we calculate the plis for A, AB, and ABC, cache them and use ABC for validation", required = false)
	int pliCachePrefixLength = ConfigurationSingleton.get().getPliCachePrefixLength();
	
	@Parameter(names = { "-vscs", "--validationSmallClusterSize" }, description = "The maximum size of pli clusters that are validated (i.e. intersected) via nested-loops; larger clusters use hash-maps for validation", required = false)
	int validationSmallClusterSize = ConfigurationSingleton.get().getPliCachePrefixLength();
	
}
