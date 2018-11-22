package de.hpi.octopus.actors.slaves;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import akka.actor.Props;
import de.hpi.octopus.actors.masters.Preprocessor;
import de.hpi.octopus.actors.masters.Preprocessor.IndexingResultMessage;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

public class Indexer extends Slave {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "indexer";

	public static Props props() {
		return Props.create(Indexer.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @EqualsAndHashCode(callSuper=false) @AllArgsConstructor @SuppressWarnings("unused")
	public static class IndexingMessage extends WorkMessage {
		private static final long serialVersionUID = -3528385798844058013L;
		private IndexingMessage() {}
		private int attribute;
		private String input;
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
	protected String getMasterName() {
		return Preprocessor.DEFAULT_NAME;
	}

	@Override
	protected void handle(WorkMessage message) throws IOException {
		IndexingMessage indexingMessage = (IndexingMessage) message;
		
		Map<String, IntArrayList> value2position = new HashMap<>();
		int recordNumber = 0;
		
		try (
			Reader reader = new InputStreamReader(new FileInputStream(indexingMessage.getInput()), StandardCharsets.UTF_8);
			CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
					.withSkipHeaderRecord(true)
					.withRecordSeparator(',')
					.withNullString("")
					.withEscape('\\')
					.withQuote('"')
					.withIgnoreEmptyLines())) {
			for (CSVRecord csvRecord : csvParser) {
				String value = csvRecord.get(indexingMessage.getAttribute());
				if (!value2position.containsKey(value))
					value2position.put(value, new IntArrayList());
				value2position.get(value).add(recordNumber);
				recordNumber++;
			}
		}
		
		int[][] pli = new int[value2position.size()][];
		int groupNumber = 0;
		for (IntArrayList sameValueGroup : value2position.values()) {
			pli[groupNumber] = sameValueGroup.elements();
			groupNumber++;
		}
		
		this.sender().tell(new IndexingResultMessage(pli, recordNumber), this.self());
	}
}
