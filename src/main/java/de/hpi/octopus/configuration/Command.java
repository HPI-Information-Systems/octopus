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
}
