package de.hpi.octopus.io;

import de.metanome.algorithm_integration.input.RelationalInputGenerator;

public class RelationalInputGeneratorSingleton {

	private static RelationalInputGenerator relationalInputGenerator = null;
	
	public static RelationalInputGenerator get() {
		return relationalInputGenerator;
	}
	
	public static void set(RelationalInputGenerator instance) {
		relationalInputGenerator = instance;
	}
}
