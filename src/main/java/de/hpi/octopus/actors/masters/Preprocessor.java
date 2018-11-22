package de.hpi.octopus.actors.masters;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import akka.actor.Props;
import de.hpi.octopus.actors.slaves.Indexer.IndexingMessage;
import de.hpi.octopus.actors.slaves.Slave.WorkMessage;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

public class Preprocessor extends Master {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "preprocessor";

	public static Props props() {
		return Props.create(Preprocessor.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @EqualsAndHashCode(callSuper=false) @AllArgsConstructor @SuppressWarnings("unused")
	public static class PreprocessingTaskMessage extends TaskMessage {
		private static final long serialVersionUID = -4788853430111845038L;
		private PreprocessingTaskMessage() {}
		private String input;
	}
	
	@Data @EqualsAndHashCode(callSuper=false) @AllArgsConstructor @SuppressWarnings("unused")
	public static class IndexingResultMessage extends ResultMessage {
		private static final long serialVersionUID = 5074517366545874380L;
		private IndexingResultMessage() {}
		private int[][] pli;
		private int inputLength;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	/////////////////////
	// Actor Lifecycle //
	/////////////////////
	
	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	protected List<WorkMessage> split(TaskMessage message) throws IOException {
		PreprocessingTaskMessage preprocessingTaskMessage = (PreprocessingTaskMessage) message;
		
		List<WorkMessage> workMessages = new ArrayList<>();
		int numAttributes = -1;
		
		try (
			Reader reader = new InputStreamReader(new FileInputStream(preprocessingTaskMessage.getInput()), StandardCharsets.UTF_8);
			CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
					.withSkipHeaderRecord(true)
					.withRecordSeparator(',')
					.withNullString("")
					.withEscape('\\')
					.withQuote('"')
					.withIgnoreEmptyLines())) {
			for (CSVRecord csvRecord : csvParser) {
				numAttributes = csvRecord.size();
			}
		}

		if (numAttributes <= 0)
			throw new RuntimeException("Input is empty!");
		
		for (int attribute = 0; attribute < numAttributes; attribute++)
			workMessages.add(new IndexingMessage(attribute, preprocessingTaskMessage.getInput()));
		return workMessages;
	}
	
	@Override
	protected void finish(WorkMessage work, ResultMessage result) {
		IndexingMessage indexingMessage = (IndexingMessage) work;
		IndexingResultMessage indexingResultMessage = (IndexingResultMessage) result;
		
		// TODO: Tell indexingResultMessage to profiler
	}
	
	@Override
	protected void finish() {
		
		// TODO Tell the Profiler that the preprocessing is done
	}
}
