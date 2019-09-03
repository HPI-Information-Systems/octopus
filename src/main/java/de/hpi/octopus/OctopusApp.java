package de.hpi.octopus;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import de.hpi.octopus.configuration.CommandMaster;
import de.hpi.octopus.configuration.CommandSlave;
import de.hpi.octopus.configuration.ConfigurationSingleton;
import de.hpi.octopus.configuration.DatasetDescriptor;
import de.hpi.octopus.configuration.DatasetDescriptorSingleton;
import de.hpi.octopus.io.RelationalInputGeneratorSingleton;
import de.hpi.octopus.testing.Test;
import de.hpi.octopus.io.FunctionalDependencyResultReceiverSingleton;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.backend.input.file.DefaultFileInputGenerator;

public class OctopusApp {

	public static void main(String[] args) throws AlgorithmConfigurationException, Exception {
		
		CommandMaster commandMaster = new CommandMaster();
        CommandSlave commandSlave = new CommandSlave();
        JCommander jCommander = JCommander.newBuilder()
        	.addCommand(OctopusMaster.MASTER_ROLE, commandMaster)
            .addCommand(OctopusSlave.SLAVE_ROLE, commandSlave)
            .build();

		DatasetDescriptor dataset = new DatasetDescriptor();
        
        try {
        	jCommander.parse(args);

            if (jCommander.getParsedCommand() == null)
                throw new ParameterException("No command given.");

            switch (jCommander.getParsedCommand()) {
                case OctopusMaster.MASTER_ROLE:
                	ConfigurationSingleton.get().update(commandMaster);
                	DatasetDescriptorSingleton.get().update(commandMaster);
                	
                	RelationalInputGenerator input = DatasetDescriptorSingleton.get().createRelationalInputGenerator();
                	RelationalInputGeneratorSingleton.set(input);
                	
                //	FunctionalDependencyResultReceiver result = XXXX;			// TODO: define a result receiver and do not write the results directly
                //	FunctionalDependencyResultReceiverSingleton.set(result);
                	
                	OctopusMaster.start();
                    break;
                case OctopusSlave.SLAVE_ROLE:
                	ConfigurationSingleton.get().update(commandSlave);
                	DatasetDescriptorSingleton.set(null); // To make sure that slaves do not use their own datasets
                	
                	OctopusSlave.start();
                    break;
                default:
                    throw new AssertionError();
            }
        } catch (ParameterException e) {
            System.out.printf("Could not parse args: %s\n", e.getMessage());
            if (jCommander.getParsedCommand() == null) {
                jCommander.usage();
            } else {
                jCommander.usage(jCommander.getParsedCommand());
            }
            System.exit(1);
        }
	}
}
