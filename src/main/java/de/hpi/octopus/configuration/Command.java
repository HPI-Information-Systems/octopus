package de.hpi.octopus.configuration;

import com.beust.jcommander.Parameter;

public abstract class Command {

	abstract int getDefaultPort();

	@Parameter(names = { "-h", "--host" }, description = "this machine's host name or IP to bind against", required = false)
	String host = ConfigurationSingleton.get().getHost();

	@Parameter(names = { "-p", "--port" }, description = "port to bind against", required = false)
	int port = this.getDefaultPort();

	@Parameter(names = { "-w", "--numWorkers" }, description = "number of workers to start locally", required = false)
	int numWorkers = ConfigurationSingleton.get().getNumWorkers();
	
	@Parameter(names = { "-mms", "--maxMessageSize" }, description = "max message size; larger messages will be broken into chunks of this size; needs to be the same value for all actor systems in the cluster", required = false)
	int maxMessageSize = ConfigurationSingleton.get().getMaxMessageSize();
}
