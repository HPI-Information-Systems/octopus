package de.hpi.octopus.configuration;

public class DatasetDescriptorSingleton {

	private static DatasetDescriptor datasetDescriptor = new DatasetDescriptor();
	
	public static DatasetDescriptor get() {
		return datasetDescriptor;
	}
	
	public static void set(DatasetDescriptor instance) {
		datasetDescriptor = instance;
	}
}
