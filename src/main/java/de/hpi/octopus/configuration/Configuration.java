package de.hpi.octopus.configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

import lombok.Data;

@Data
public class Configuration {
	
	public static final int DEFAULT_MASTER_PORT = 7877;
	public static final int DEFAULT_SLAVE_PORT = 7879;
	
	private String host = getDefaultHost();			// This machine's host name or IP that we use to bind this application against
	private int port = DEFAULT_MASTER_PORT;			// This machines port that we use to bind this application against
	
	private String masterHost = getDefaultHost();	// The host name or IP of the master; if this is a master, masterHost = host
	private int masterPort = DEFAULT_MASTER_PORT;	// The port of the master; if this is a master, masterPort = port
	
	private String actorSystemName = "octopus";		// The name of this application
	
	private int numWorkers = 4;						// The number of workers (indexers/validators) to start locally; should be at least one if the algorithm is started standalone (otherwise there are no workers to run the discovery)
	private int maxLhsSize = -1;					// The lhss can become numAttributes - 1 large, but often we are interested in only those FDs with lhs < some threshold (to be useful for normalization, key discovery etc.)
	private int inputRowLimit = -1;					// Maximum number of rows to be read from for analysis; values smaller or equal 0 will cause the algorithm to read all rows
	private boolean nullEqualsNull = true;			// The null semantic for comparing null values; null is always in-equal to any other value, but null==null might evaluate to true or false; true is used by most FD discovery algorithms
	private boolean enableMemoryGuardian = false;	// The memory guardian monitors the memory consumption and automatically lowers the maxLhsSize if memory is exhausted
	
	private boolean startPaused = false;			// Wait for some console input to start the discovery; useful, if we want to wait manually until all ActorSystems in the cluster are started (e.g. to avoid work stealing effects in performance evaluations)
	
	private int bufferSize = 100; 					// Buffer for input reading (the DatasetReader pre-fetches and buffers this many records)
	private int maxMessageSize = 1000;				// Maximum size of messages in bytes; larger messages will be broken into chunks of this size; needs to be the same value for all actor systems in the cluster; hence, its not a parameter of the algorithm
	
	private int maxCandidatesPerRequest = 30;		// Maximum number of FD candidates per candidate validation request; these batches of candidates are validated sequentially
	private double validationThreshold = 0.8;		// Proportion of true FD candidates in all FD candidates of one validation request; validationThreshold = true/all; if the actual validation efficiency is below that threshold, the dependency Steward switches its discovery strategy from candidate validation to sampling
	
	private int pliCachePrefixLength = 3;			// The maximum number of lhs prefix attributes for which the FD candidate validation should calculate and cache intermediate plis; e.g. for prefix 3 and candidate ABCD->E, we calculate the plis for A, AB, and ABC, cache them and use ABC for validation
	private int validationSmallClusterSize = 40;	// The maximum size of pli clusters that are validated (i.e. intersected) via nested-loops; larger clusters use hash-maps for validation
	
	private static String getDefaultHost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "localhost";
        }
    }

	public void update(CommandMaster commandMaster) {
		this.host = commandMaster.host;
		this.port = commandMaster.port;
		this.numWorkers = commandMaster.numWorkers;
		this.maxLhsSize = commandMaster.maxLhsSize;
		this.inputRowLimit = commandMaster.inputRowLimit;
		this.nullEqualsNull = commandMaster.nullEqualsNull;
		this.enableMemoryGuardian = commandMaster.enableMemoryGuardian;
		this.startPaused = commandMaster.startPaused;
		this.bufferSize = commandMaster.bufferSize;
		this.maxMessageSize = commandMaster.maxMessageSize;
		this.validationThreshold = commandMaster.validationThreshold;
		this.maxCandidatesPerRequest = commandMaster.maxCandidatesPerRequest;
		this.pliCachePrefixLength = commandMaster.pliCachePrefixLength;
		this.validationSmallClusterSize = commandMaster.validationSmallClusterSize;
	}

	public void update(CommandSlave commandSlave) {
		this.host = commandSlave.host;
		this.port = commandSlave.port;
		this.masterHost = commandSlave.masterhost;
		this.masterPort = commandSlave.masterport;
		this.numWorkers = commandSlave.numWorkers;
		this.maxMessageSize = commandSlave.maxMessageSize;
		this.pliCachePrefixLength = commandSlave.pliCachePrefixLength;
		this.validationSmallClusterSize = commandSlave.validationSmallClusterSize;
	}
}
