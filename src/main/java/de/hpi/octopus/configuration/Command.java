package de.hpi.octopus.configuration;

import com.beust.jcommander.Parameter;

public abstract class Command {

	abstract int getDefaultPort();

	@Parameter(names = { "-h", "--host" }, description = "This machine's host name or IP that we use to bind this application against", required = false)
	String host = ConfigurationSingleton.get().getHost();

	@Parameter(names = { "-p", "--port" }, description = "This machines port that we use to bind this application against", required = false)
	int port = this.getDefaultPort();

	@Parameter(names = { "-w", "--numWorkers" }, description = "The number of workers (indexers/validators) to start locally; should be at least one if the algorithm is started standalone (otherwise there are no workers to run the discovery)", required = false)
	int numWorkers = ConfigurationSingleton.get().getNumWorkers();
	
	@Parameter(names = { "-mms", "--maxMessageSize" }, description = "Maximum size of messages in bytes; larger messages will be broken into chunks of this size; needs to be the same value for all actor systems in the cluster", required = false)
	int maxMessageSize = ConfigurationSingleton.get().getMaxMessageSize();
	
	@Parameter(names = { "-pcpl", "--pliCachePrefixLength" }, description = "The maximum number of lhs prefix attributes for which the FD candidate validation should calculate and cache intermediate plis; e.g. for prefix 3 and candidate ABCD->E, we calculate the plis for A, AB, and ABC, cache them and use ABC for validation", required = false)
	int pliCachePrefixLength = ConfigurationSingleton.get().getPliCachePrefixLength();
	
	@Parameter(names = { "-vscs", "--validationSmallClusterSize" }, description = "The maximum size of pli clusters that are validated (i.e. intersected) via nested-loops; larger clusters use hash-maps for validation", required = false)
	int validationSmallClusterSize = ConfigurationSingleton.get().getPliCachePrefixLength();
	
}
