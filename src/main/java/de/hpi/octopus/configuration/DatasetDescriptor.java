package de.hpi.octopus.configuration;

import java.io.File;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor @AllArgsConstructor
public class DatasetDescriptor implements Serializable {
	
	private static final long serialVersionUID = 1985782678973727520L;
	
	private String datasetName = "ncvoter_Statewide_10001r_71c"; // "ncvoter_Statewide_1024001r_71c"
	private String datasetPath = "data/"; // "/home/thorsten/Data/Development/workspace/papenbrock/HyFDTestRunner/data/"
	private String datasetEnding = ".csv";

	private boolean fileHasHeader = true;
	private Charset charset = StandardCharsets.UTF_8;
	
	private char attributeSeparator = ','; // ';'
	private char attributeQuote = '"';
	private char attributeEscape = '\\';
	private String attributeNullString = "";
	private boolean attributeStrictQuotes = false;
	private boolean attributeIgnoreLeadingWhitespace = true;	// Ignore i.e. delete all whitespaces preceding any read value 
	
	private int readerSkipLines = 0;							// Number of lines that should be skipped at the beginning of the file
	private boolean readerSkipDifferingLines = true;			// True if the reader should skip lines in the input that have a different length as the first line
	
	public String getDatasetPathNameEnding() {
		String pathNameSeparator = this.datasetPath.endsWith(File.separator) ? "" : File.separator;
		String nameEndingSeparator = this.datasetEnding.startsWith(".") ? "" : ".";
		
		return this.datasetPath + pathNameSeparator + this.datasetName + nameEndingSeparator + this.datasetEnding;
	}

	public void update(CommandMaster commandMaster) {
		this.datasetName = commandMaster.datasetName;
		this.datasetPath = commandMaster.datasetPath;
		this.datasetEnding = commandMaster.datasetEnding;
		this.fileHasHeader = commandMaster.fileHasHeader;
		this.charset = commandMaster.charset;
		this.attributeSeparator = commandMaster.attributeSeparator;
		this.attributeQuote = commandMaster.attributeQuote;
		this.attributeEscape = commandMaster.attributeEscape;
		this.attributeNullString = commandMaster.attributeNullString;
		this.attributeStrictQuotes = commandMaster.attributeStrictQuotes;
		this.attributeIgnoreLeadingWhitespace = commandMaster.attributeIgnoreLeadingWhitespace;
		this.readerSkipLines = commandMaster.readerSkipLines;
		this.readerSkipDifferingLines = commandMaster.readerSkipDifferingLines;
	}
	
	public RelationalInputGenerator createRelationalInputGenerator() throws AlgorithmConfigurationException {
		return new DefaultFileInputGenerator(new ConfigurationSettingFileInput(
				this.getDatasetPathNameEnding(), true, this.attributeSeparator, this.attributeQuote, 
				this.attributeEscape, this.attributeStrictQuotes, this.attributeIgnoreLeadingWhitespace, 
				this.readerSkipLines, this.fileHasHeader, this.readerSkipDifferingLines, this.attributeNullString));
	}
}
