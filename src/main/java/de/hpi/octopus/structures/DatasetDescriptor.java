package de.hpi.octopus.structures;

import java.io.File;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data @AllArgsConstructor
public class DatasetDescriptor implements Serializable {
	
	private static final long serialVersionUID = 1985782678973727520L;
	
	private String datasetName;
	private String datasetPath;
	private String datasetEnding;

	private boolean fileHasHeader;
	private Charset charset = StandardCharsets.UTF_8;
	
	private char attributeSeparator;
	private char attributeQuote;
	private char attributeEscape;
	private String attributeNullString;
	private boolean attributeStrictQuotes;
	private boolean attributeIgnoreLeadingWhitespace;
	
	private int readerBufferSize;
	private int readerSkipLines;
	private boolean readerSkipDifferingLines;
	
	public String getDatasetPathNameEnding() {
		String pathNameSeparator = this.datasetPath.endsWith(File.separator) ? "" : File.separator;
		String nameEndingSeparator = this.datasetEnding.startsWith(".") ? "" : ".";
		
		return this.datasetPath + pathNameSeparator + this.datasetName + nameEndingSeparator + this.datasetEnding;
	}
}
