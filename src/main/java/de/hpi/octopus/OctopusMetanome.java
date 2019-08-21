package de.hpi.octopus;

import java.util.ArrayList;

import de.hpi.octopus.configuration.ConfigurationSingleton;
import de.hpi.octopus.io.FunctionalDependencyResultReceiverSingleton;
import de.hpi.octopus.io.RelationalInputGeneratorSingleton;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.BooleanParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.FunctionalDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.IntegerParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.RelationalInputParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.StringParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementBoolean;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementInteger;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementRelationalInput;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementString;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.FunctionalDependencyResultReceiver;

public class OctopusMetanome implements FunctionalDependencyAlgorithm, IntegerParameterAlgorithm, BooleanParameterAlgorithm, StringParameterAlgorithm, RelationalInputParameterAlgorithm {

	public enum Identifier {
		INPUT_GENERATOR, 
		NUM_WORKERS, 
		MAX_DETERMINANT_SIZE, 
		INPUT_ROW_LIMIT, 
		NULL_EQUALS_NULL, 
		ENABLE_MEMORY_GUARDIAN, 
		READ_BUFFER_SIZE, 
		MAX_MESSAGE_SIZE,
		MAX_CANDIDATES_PER_REQUEST,
		VALIDATION_THRESHOLD,
		PLI_CACHE_PREFIX_LENGTH,
		VALIDATION_SMALL_CLUSTER_SIZE
	};
	
	@Override
	public String getAuthors() {
		return "Thorsten Papenbrock";
	}

	@Override
	public String getDescription() {
		return "Distributed, hybrid Sampling- and Lattice-Traversal-based FD discovery";
	}
	
	@Override
	public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
		ArrayList<ConfigurationRequirement<?>> configs = new ArrayList<ConfigurationRequirement<?>>(5);
		configs.add(new ConfigurationRequirementRelationalInput(OctopusMetanome.Identifier.INPUT_GENERATOR.name()));
		
		configs.add(this.require(OctopusMetanome.Identifier.NUM_WORKERS.name(), ConfigurationSingleton.get().getNumWorkers(), false));
		configs.add(this.require(OctopusMetanome.Identifier.MAX_DETERMINANT_SIZE.name(), ConfigurationSingleton.get().getMaxLhsSize(), false));
		configs.add(this.require(OctopusMetanome.Identifier.INPUT_ROW_LIMIT.name(), ConfigurationSingleton.get().getInputRowLimit(), false));
		configs.add(this.require(OctopusMetanome.Identifier.NULL_EQUALS_NULL.name(), ConfigurationSingleton.get().isNullEqualsNull(), false));
		configs.add(this.require(OctopusMetanome.Identifier.ENABLE_MEMORY_GUARDIAN.name(), ConfigurationSingleton.get().isEnableMemoryGuardian(), false));
		configs.add(this.require(OctopusMetanome.Identifier.READ_BUFFER_SIZE.name(), ConfigurationSingleton.get().getBufferSize(), false));
		configs.add(this.require(OctopusMetanome.Identifier.MAX_MESSAGE_SIZE.name(), ConfigurationSingleton.get().getMaxMessageSize(), false));
		configs.add(this.require(OctopusMetanome.Identifier.MAX_CANDIDATES_PER_REQUEST.name(), ConfigurationSingleton.get().getMaxCandidatesPerRequest(), false));
		configs.add(this.require(OctopusMetanome.Identifier.VALIDATION_THRESHOLD.name(), ConfigurationSingleton.get().getValidationThreshold(), false));
		configs.add(this.require(OctopusMetanome.Identifier.PLI_CACHE_PREFIX_LENGTH.name(), ConfigurationSingleton.get().getPliCachePrefixLength(), false));
		configs.add(this.require(OctopusMetanome.Identifier.VALIDATION_SMALL_CLUSTER_SIZE.name(), ConfigurationSingleton.get().getValidationSmallClusterSize(), false));
		
		return configs;
	}

	private ConfigurationRequirementInteger require(String identifier, Integer defaultValue, boolean isRequired) {
		ConfigurationRequirementInteger requirement = new ConfigurationRequirementInteger(identifier);
		Integer[] defaultValues = { defaultValue };
		requirement.setDefaultValues(defaultValues);
		requirement.setRequired(isRequired);
		return requirement;
	}
	
	private ConfigurationRequirementBoolean require(String identifier, Boolean defaultValue, boolean isRequired) {
		ConfigurationRequirementBoolean requirement = new ConfigurationRequirementBoolean(identifier);
		Boolean[] defaultValues = { defaultValue };
		requirement.setDefaultValues(defaultValues);
		requirement.setRequired(isRequired);
		return requirement;
	}

	private ConfigurationRequirementString require(String identifier, Double defaultValue, boolean isRequired) {
		ConfigurationRequirementString requirement = new ConfigurationRequirementString(identifier);
		String[] defaultValues = { String.valueOf(defaultValue) };
		requirement.setDefaultValues(defaultValues);
		requirement.setRequired(isRequired);
		return requirement;
	}
	
	@Override
	public void setResultReceiver(FunctionalDependencyResultReceiver resultReceiver) {
		FunctionalDependencyResultReceiverSingleton.set(resultReceiver);
	}

	@Override
	public void setIntegerConfigurationValue(String identifier, Integer... values) throws AlgorithmConfigurationException {
		if (OctopusMetanome.Identifier.NUM_WORKERS.name().equals(identifier))
			ConfigurationSingleton.get().setNumWorkers(values[0]);
		else if (OctopusMetanome.Identifier.MAX_DETERMINANT_SIZE.name().equals(identifier))
			ConfigurationSingleton.get().setMaxLhsSize(values[0]);
		else if (OctopusMetanome.Identifier.INPUT_ROW_LIMIT.name().equals(identifier))
			ConfigurationSingleton.get().setInputRowLimit(values[0]);
		else if (OctopusMetanome.Identifier.READ_BUFFER_SIZE.name().equals(identifier))
			ConfigurationSingleton.get().setBufferSize(values[0]);
		else if (OctopusMetanome.Identifier.MAX_MESSAGE_SIZE.name().equals(identifier))
			ConfigurationSingleton.get().setMaxMessageSize(values[0]);
		else if (OctopusMetanome.Identifier.MAX_CANDIDATES_PER_REQUEST.name().equals(identifier))
			ConfigurationSingleton.get().setMaxCandidatesPerRequest(values[0]);
		else if (OctopusMetanome.Identifier.PLI_CACHE_PREFIX_LENGTH.name().equals(identifier))
			ConfigurationSingleton.get().setPliCachePrefixLength(values[0]);
		else if (OctopusMetanome.Identifier.VALIDATION_SMALL_CLUSTER_SIZE.name().equals(identifier))
			ConfigurationSingleton.get().setValidationSmallClusterSize(values[0]);
		else
			this.handleUnknownConfiguration(identifier, values[0].toString());
	}

	@Override
	public void setBooleanConfigurationValue(String identifier, Boolean... values) throws AlgorithmConfigurationException {
		if (OctopusMetanome.Identifier.NULL_EQUALS_NULL.name().equals(identifier))
			ConfigurationSingleton.get().setNullEqualsNull(values[0]);
		else if (OctopusMetanome.Identifier.ENABLE_MEMORY_GUARDIAN.name().equals(identifier))
			ConfigurationSingleton.get().setEnableMemoryGuardian(values[0]);
		else
			this.handleUnknownConfiguration(identifier, values[0].toString());
	}
	
	@Override
	public void setStringConfigurationValue(String identifier, String... values) throws AlgorithmConfigurationException {
		if (OctopusMetanome.Identifier.VALIDATION_THRESHOLD.name().equals(identifier))
			ConfigurationSingleton.get().setValidationThreshold(Double.valueOf(values[0]));
		else
			this.handleUnknownConfiguration(identifier, values[0].toString());
	}
	
	@Override
	public void setRelationalInputConfigurationValue(String identifier, RelationalInputGenerator... values) throws AlgorithmConfigurationException {
		if (OctopusMetanome.Identifier.INPUT_GENERATOR.name().equals(identifier))
			RelationalInputGeneratorSingleton.set(values[0]);
		else
			this.handleUnknownConfiguration(identifier, values[0].toString());
	}
	
	private void handleUnknownConfiguration(String identifier, String value) throws AlgorithmConfigurationException {
		throw new AlgorithmConfigurationException("Unknown configuration: " + identifier + " -> " + value);
	}

	@Override
	public String toString() {
		return "Octopus";
	}
	
	@Override
	public void execute() throws AlgorithmExecutionException {
		if (RelationalInputGeneratorSingleton.get() == null)
			throw new AlgorithmConfigurationException("No input generator set!");
		if (FunctionalDependencyResultReceiverSingleton.get() == null)
			throw new AlgorithmConfigurationException("No result receiver set!");
		
        OctopusMaster.start();
	}
}
