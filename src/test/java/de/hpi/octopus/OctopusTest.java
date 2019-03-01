package de.hpi.octopus;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;

import de.hpi.octopus.fixtures.AbaloneFixture;
import de.hpi.octopus.fixtures.AbstractAlgorithmTestFixture;
import de.hpi.octopus.fixtures.AlgorithmTestFixture;
import de.hpi.octopus.fixtures.BridgesFixture;
import de.metanome.algorithm_integration.AlgorithmExecutionException;

public class OctopusTest extends FDAlgorithmTest {
	
	private String tempFolderPath = "io" + File.separator + "temp_junit" + File.separator;
	private boolean nullEqualsNull = true;
	
	@Before
	public void setUp() throws Exception {
		this.algo = new OctopusMetanome();
	}

	@After
	public void tearDown() throws Exception {
		// Clean temp if there are files from previous runs that may pollute this run
		FileUtils.cleanDirectory(new File(this.tempFolderPath));
	}
	
	protected void executeAndVerifyWithFixture(AbstractAlgorithmTestFixture fixture) throws AlgorithmExecutionException {
		OctopusMetanome algorithm = (OctopusMetanome) this.algo;
		algorithm.setRelationalInputConfigurationValue(OctopusMetanome.Identifier.INPUT_GENERATOR.name(), fixture.getInputGenerator());
		algorithm.setBooleanConfigurationValue(OctopusMetanome.Identifier.NULL_EQUALS_NULL.name(), Boolean.valueOf(this.nullEqualsNull));
		algorithm.setResultReceiver(fixture.getFunctionalDependencyResultReceiver());
        
		// Execute algorithm
		algorithm.execute();
        
        // Check results
        fixture.verifyFunctionalDependencyResultReceiver();
	}

	protected void executeAndVerifyWithFixture(AbaloneFixture fixture) throws AlgorithmExecutionException, UnsupportedEncodingException, FileNotFoundException {
		OctopusMetanome algorithm = (OctopusMetanome) this.algo;
		algorithm.setRelationalInputConfigurationValue(OctopusMetanome.Identifier.INPUT_GENERATOR.name(), fixture.getInputGenerator());
		algorithm.setBooleanConfigurationValue(OctopusMetanome.Identifier.NULL_EQUALS_NULL.name(), Boolean.valueOf(this.nullEqualsNull));
		algorithm.setResultReceiver(fixture.getFdResultReceiver());
		
		// Execute functionality
		algorithm.execute();

        // Check Results
        fixture.verifyFdResultReceiver();
	}

	protected void executeAndVerifyWithFixture(BridgesFixture fixture) throws AlgorithmExecutionException, UnsupportedEncodingException, FileNotFoundException {
		OctopusMetanome algorithm = (OctopusMetanome) this.algo;
		algorithm.setRelationalInputConfigurationValue(OctopusMetanome.Identifier.INPUT_GENERATOR.name(), fixture.getInputGenerator());
		algorithm.setBooleanConfigurationValue(OctopusMetanome.Identifier.NULL_EQUALS_NULL.name(), Boolean.valueOf(this.nullEqualsNull));
		algorithm.setResultReceiver(fixture.getFdResultReceiver());
		
		// Execute functionality
		algorithm.execute();

        // Check Results
        fixture.verifyFunctionalDependencyResultReceiver();
	}

	protected void executeAndVerifyWithFixture(AlgorithmTestFixture fixture) throws AlgorithmExecutionException {
		OctopusMetanome algorithm = (OctopusMetanome) this.algo;
		algorithm.setRelationalInputConfigurationValue(OctopusMetanome.Identifier.INPUT_GENERATOR.name(), fixture.getInputGenerator());
		algorithm.setBooleanConfigurationValue(OctopusMetanome.Identifier.NULL_EQUALS_NULL.name(), Boolean.valueOf(this.nullEqualsNull));
		algorithm.setResultReceiver(fixture.getFunctionalDependencyResultReceiver());
		
		// Execute functionality
		algorithm.execute();

        // Check Results
        fixture.verifyFunctionalDependencyResultReceiver();
	}
}
