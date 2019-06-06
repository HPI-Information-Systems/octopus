package de.hpi.octopus.io;

import de.metanome.algorithm_integration.result_receiver.FunctionalDependencyResultReceiver;

public class FunctionalDependencyResultReceiverSingleton {

	private static FunctionalDependencyResultReceiver functionalDependencyResultReceiver = null;
	
	public static FunctionalDependencyResultReceiver get() {
		return functionalDependencyResultReceiver;
	}
	
	public static void set(FunctionalDependencyResultReceiver instance) {
		functionalDependencyResultReceiver = instance;
	}
}
