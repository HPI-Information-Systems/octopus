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

	@Parameter(names = { "-mls", "--maxLhsSize" }, description = "The lhss can become numAttributes - 1 large, but often we are interested in only those FDs with lhs < some threshold (to be useful for normalization, key discovery etc.)", required = false)
	int maxLhsSize = ConfigurationSingleton.get().getMaxLhsSize();

	@Parameter(names = { "-irl", "--inputRowLimit" }, description = "Maximum number of rows to be read from for analysis; values smaller or equal 0 will cause the algorithm to read all rows", required = false)
	int inputRowLimit = ConfigurationSingleton.get().getInputRowLimit();

	@Parameter(names = { "-nen", "--nullEqualsNull" }, description = "The null semantic for comparing null values; null is always in-equal to any other value, but null==null might evaluate to true or false; true is used by most FD discovery algorithms", required = false)
	boolean nullEqualsNull = ConfigurationSingleton.get().isNullEqualsNull();

	@Parameter(names = { "-emg", "--enableMemoryGuardian" }, description = "The memory guardian monitors the memory consumption and automatically lowers the maxLhsSize if memory is exhausted", required = false)
	boolean enableMemoryGuardian = ConfigurationSingleton.get().isEnableMemoryGuardian();
	
	@Parameter(names = { "-sp", "--startPaused" }, description = "Wait for some console input to start the discovery; useful, if we want to wait manually until all ActorSystems in the cluster are started (e.g. to avoid work stealing effects in performance evaluations)", required = false)
	boolean startPaused = ConfigurationSingleton.get().isStartPaused();

	@Parameter(names = { "-mcpr", "--maxCandidatesPerRequest" }, description = "Maximum number of FD candidates per candidate validation request; these batches of candidates are validated sequentially", required = false)
	int maxCandidatesPerRequest = ConfigurationSingleton.get().getMaxCandidatesPerRequest();

	@Parameter(names = { "-bs", "--bufferSize" }, description = "Buffer for input reading (the DatasetReader pre-fetches and buffers this many records)", required = false)
	int bufferSize = ConfigurationSingleton.get().getBufferSize();
	
	@Parameter(names = { "-vt", "--validationThreshold" }, description = "Proportion of true FD candidates in all FD candidates of one validation request; validationThreshold = true/all; if the actual validation efficiency is below that threshold, the dependency Steward switches its discovery strategy from candidate validation to sampling", required = false)
	double validationThreshold = ConfigurationSingleton.get().getValidationThreshold();
	
	// DatasetDescriptor
	
	@Parameter(names = { "-dn", "--datasetName" }, description = "Dataset name", required = false)
	String datasetName = DatasetDescriptorSingleton.get().getDatasetName();

	@Parameter(names = { "-dp", "--datasetPath" }, description = "Dataset path", required = false)
	String datasetPath = DatasetDescriptorSingleton.get().getDatasetPath();

	@Parameter(names = { "-de", "--datasetEnding" }, description = "Dataset ending", required = false)
	String datasetEnding = DatasetDescriptorSingleton.get().getDatasetEnding();

	@Parameter(names = { "-fh", "--fileHasHeader" }, description = "File has header as defined by the input data", required = false)
	boolean fileHasHeader = DatasetDescriptorSingleton.get().isFileHasHeader();

	@Parameter(names = { "-cs", "--charset" }, description = "Charset as defined by the input data", required = false)
	Charset charset = DatasetDescriptorSingleton.get().getCharset();

	@Parameter(names = { "-vs", "--valueSeparator" }, description = "Value separator as defined by the input data", required = false)
	char attributeSeparator = DatasetDescriptorSingleton.get().getValueSeparator();

	@Parameter(names = { "-vq", "--valueQuote" }, description = "Value quote as defined by the input data", required = false)
	char attributeQuote = DatasetDescriptorSingleton.get().getValueQuote();

	@Parameter(names = { "-ve", "--valueEscape" }, description = "Value escape as defined by the input data", required = false)
	char attributeEscape = DatasetDescriptorSingleton.get().getValueEscape();

	@Parameter(names = { "-vn", "--valueNullString" }, description = "Value null string as defined by the input data", required = false)
	String attributeNullString = DatasetDescriptorSingleton.get().getValueNullString();

	@Parameter(names = { "-vsq", "--valueStrictQuotes" }, description = "Value strict quotes as defined by the input data", required = false)
	boolean attributeStrictQuotes = DatasetDescriptorSingleton.get().isValueStrictQuotes();

	@Parameter(names = { "-viw", "--valueIgnoreLeadingWhitespace" }, description = "Ignore i.e. delete all whitespaces preceding any read value ", required = false)
	boolean attributeIgnoreLeadingWhitespace = DatasetDescriptorSingleton.get().isValueIgnoreLeadingWhitespace();

	@Parameter(names = { "-rsl", "--readerSkipLines" }, description = "Number of lines that should be skipped at the beginning of the file", required = false)
	int readerSkipLines = DatasetDescriptorSingleton.get().getReaderSkipLines();

	@Parameter(names = { "-rsdl", "--readerSkipDifferingLines" }, description = "True if the reader should skip lines in the input that have a different length as the first line", required = false)
	boolean readerSkipDifferingLines = DatasetDescriptorSingleton.get().isReaderSkipDifferingLines();
}
