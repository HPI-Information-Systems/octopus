package de.hpi.octopus.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import de.hpi.octopus.actors.masters.Preprocessor;
import de.hpi.octopus.structures.Input;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import lombok.AllArgsConstructor;
import lombok.Data;

public class InputReader extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "inputReader";

	public static Props props(Input input) {
		return Props.create(InputReader.class, () -> new InputReader(input));
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
	
	private Input input;

	private RelationalInputGenerator relationalInputGenerator;
	private RelationalInput relationalInputReader;
	
	private List<List<String>> buffer;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////
	
	public InputReader(Input input) {
		this.input = input;
	}

	@Override
	public void preStart() throws Exception {
		this.relationalInputGenerator = new DefaultFileInputGenerator(new ConfigurationSettingFileInput(
				this.input.getDatasetPathNameEnding(), true, this.input.getAttributeSeparator(), this.input.getAttributeQuote(), 
				this.input.getAttributeEscape(), this.input.isAttributeStrictQuotes(), this.input.isAttributeIgnoreLeadingWhitespace(), 
				this.input.getReaderSkipLines(), this.input.isFileHasHeader(), this.input.isReaderSkipDifferingLines(), this.input.getAttributeNullString()));
		
		this.relationalInputReader = this.relationalInputGenerator.generateNewCopy();
		
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
		List<List<String>> batch = this.buffer;
		
		this.sender().tell(new Preprocessor.BatchMessage(batch, message.getWatermark()), this.self());
		
		this.read();
	}
	
	private void handle(RestartMessage message) throws Exception {
		this.relationalInputReader.close();
		this.relationalInputReader = this.relationalInputGenerator.generateNewCopy();
		
		this.read();
		
		List<List<String>> batch = this.buffer;
		
		this.sender().tell(new Preprocessor.BatchMessage(batch, message.getWatermark()), this.self());
		
		this.read();
	}

	private void read() throws Exception {
		this.buffer = new ArrayList<>(this.input.getReaderBufferSize());
		
		while (this.relationalInputReader.hasNext() && this.buffer.size() < this.input.getReaderBufferSize()) {
			List<String> record = this.relationalInputReader.next();
			
			this.buffer.add(record);
		}
	}
	
}
