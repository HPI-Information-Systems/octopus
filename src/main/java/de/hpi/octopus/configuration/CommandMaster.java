package de.hpi.octopus.configuration;

import java.nio.charset.Charset;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(commandDescription = "start a master actor system")
public class CommandMaster extends Command {

	@Override
	int getDefaultPort() {
		return Configuration.DEFAULT_MASTER_PORT;
	}

	@Parameter(names = { "-mls", "--maxLhsSize" }, required = false)
	int maxLhsSize = ConfigurationSingleton.get().getMaxLhsSize();

	@Parameter(names = { "-irl", "--inputRowLimit" }, required = false)
	int inputRowLimit = ConfigurationSingleton.get().getInputRowLimit();

	@Parameter(names = { "-nen", "--nullEqualsNull" }, required = false)
	boolean nullEqualsNull = ConfigurationSingleton.get().isNullEqualsNull();

	@Parameter(names = { "-emg", "--enableMemoryGuardian" }, required = false)
	boolean enableMemoryGuardian = ConfigurationSingleton.get().isEnableMemoryGuardian();
	
	@Parameter(names = { "-sp", "--startPaused" }, required = false)
	boolean startPaused = ConfigurationSingleton.get().isStartPaused();

	@Parameter(names = { "-bs", "--bufferSize" }, required = false)
	public int bufferSize = ConfigurationSingleton.get().getBufferSize();
	
	// DatasetDescriptor
	
	@Parameter(names = { "-dn", "--datasetName" }, required = false)
	String datasetName = DatasetDescriptorSingleton.get().getDatasetName();

	@Parameter(names = { "-dp", "--datasetPath" }, required = false)
	String datasetPath = DatasetDescriptorSingleton.get().getDatasetPath();

	@Parameter(names = { "-de", "--datasetEnding" }, required = false)
	String datasetEnding = DatasetDescriptorSingleton.get().getDatasetEnding();

	@Parameter(names = { "-fh", "--fileHasHeader" }, required = false)
	boolean fileHasHeader = DatasetDescriptorSingleton.get().isFileHasHeader();

	@Parameter(names = { "-cs", "--charset" }, required = false)
	Charset charset = DatasetDescriptorSingleton.get().getCharset();

	@Parameter(names = { "-as", "--attributeSeparator" }, required = false)
	char attributeSeparator = DatasetDescriptorSingleton.get().getAttributeSeparator();

	@Parameter(names = { "-aq", "--attributeQuote" }, required = false)
	char attributeQuote = DatasetDescriptorSingleton.get().getAttributeQuote();

	@Parameter(names = { "-ae", "--attributeEscape" }, required = false)
	char attributeEscape = DatasetDescriptorSingleton.get().getAttributeEscape();

	@Parameter(names = { "-an", "--attributeNullString" }, required = false)
	String attributeNullString = DatasetDescriptorSingleton.get().getAttributeNullString();

	@Parameter(names = { "-asq", "--attributeStrictQuotes" }, required = false)
	boolean attributeStrictQuotes = DatasetDescriptorSingleton.get().isAttributeStrictQuotes();

	@Parameter(names = { "-iw", "--attributeIgnoreLeadingWhitespace" }, required = false)
	boolean attributeIgnoreLeadingWhitespace = DatasetDescriptorSingleton.get().isAttributeIgnoreLeadingWhitespace();

	@Parameter(names = { "-rsl", "--readerSkipLines" }, required = false)
	int readerSkipLines = DatasetDescriptorSingleton.get().getReaderSkipLines();

	@Parameter(names = { "-rsdl", "--readerSkipDifferingLines" }, required = false)
	boolean readerSkipDifferingLines = DatasetDescriptorSingleton.get().isReaderSkipDifferingLines();
}
