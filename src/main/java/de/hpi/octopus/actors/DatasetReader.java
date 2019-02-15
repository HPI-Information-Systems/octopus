package de.hpi.octopus.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import de.hpi.octopus.actors.masters.Preprocessor;
import de.hpi.octopus.structures.DatasetDescriptor;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import lombok.AllArgsConstructor;
import lombok.Data;

public class DatasetReader extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "inputReader";

	public static Props props(DatasetDescriptor dataset) {
		return Props.create(DatasetReader.class, () -> new DatasetReader(dataset));
	}

	public DatasetReader(DatasetDescriptor dataset) {
		this.dataset = dataset;
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class ReadMessage implements Serializable {
		private static final long serialVersionUID = -3254147511955012292L;
		private ReadMessage() {}
		private int watermark;
	}
	
	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class RestartMessage implements Serializable {
		private static final long serialVersionUID = -7999488286843626433L;
		private RestartMessage() {}
		private int watermark;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	private DatasetDescriptor dataset;

	private RelationalInputGenerator relationalInputGenerator;
	private RelationalInput relationalInputReader;
	
	private String[] columnNames;
	private List<List<String>> buffer;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////
	
	@Override
	public void preStart() throws Exception {
		this.relationalInputGenerator = new DefaultFileInputGenerator(new ConfigurationSettingFileInput(
				this.dataset.getDatasetPathNameEnding(), true, this.dataset.getAttributeSeparator(), this.dataset.getAttributeQuote(), 
				this.dataset.getAttributeEscape(), this.dataset.isAttributeStrictQuotes(), this.dataset.isAttributeIgnoreLeadingWhitespace(), 
				this.dataset.getReaderSkipLines(), this.dataset.isFileHasHeader(), this.dataset.isReaderSkipDifferingLines(), this.dataset.getAttributeNullString()));
		
		this.relationalInputReader = this.relationalInputGenerator.generateNewCopy();
		this.columnNames = this.relationalInputReader.columnNames().toArray(new String[0]);
		
		this.read();
	}

	@Override
	public void postStop() throws Exception {
		this.relationalInputReader.close();
		this.relationalInputGenerator.close();
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(ReadMessage.class, this::handle)
				.match(RestartMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(ReadMessage message) throws Exception {
		this.send(message.getWatermark());
		this.read();
	}
	
	private void handle(RestartMessage message) throws Exception {
		this.relationalInputReader.close();
		this.relationalInputReader = this.relationalInputGenerator.generateNewCopy();
		this.columnNames = this.relationalInputReader.columnNames().toArray(new String[0]);
		
		this.read();
		this.send(message.getWatermark());
		this.read();
	}

	private void send(int watermark) {
		if (this.buffer.isEmpty()) {
			this.sender().tell(new Preprocessor.BatchMessage(null, this.columnNames, watermark), this.self());
			return;
		}
		
		this.sender().tell(new Preprocessor.BatchMessage(this.buffer, null, watermark), this.self());
	}
	
	private void read() throws Exception {
		this.buffer = new ArrayList<>(this.dataset.getReaderBufferSize());
		
		while (this.relationalInputReader.hasNext() && this.buffer.size() < this.dataset.getReaderBufferSize()) {
			List<String> record = this.relationalInputReader.next();
			
			this.buffer.add(record);
		}
	}
}
