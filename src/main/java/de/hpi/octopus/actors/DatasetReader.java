package de.hpi.octopus.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import de.hpi.octopus.actors.masters.Preprocessor;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class DatasetReader extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "datasetReader";

	public static Props props(RelationalInputGenerator relationalInputGenerator, int bufferSize) {
		return Props.create(DatasetReader.class, () -> new DatasetReader(relationalInputGenerator, bufferSize));
	}

	public DatasetReader(RelationalInputGenerator relationalInputGenerator, int bufferSize) {
		this.relationalInputGenerator = relationalInputGenerator;
		this.bufferSize = bufferSize;
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class ReadMessage implements Serializable {
		private static final long serialVersionUID = -3254147511955012292L;
		private int watermark;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class RestartMessage implements Serializable {
		private static final long serialVersionUID = -7999488286843626433L;
		private int watermark;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	private RelationalInputGenerator relationalInputGenerator;
	private RelationalInput relationalInputReader;
	
	private int bufferSize;
	
	private String relationName;
	private String[] columnNames;
	private List<List<String>> buffer;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() throws Exception {
		// Open the input and start reading first lines into local cache
		this.relationalInputReader = this.relationalInputGenerator.generateNewCopy();
		this.relationName = this.relationalInputReader.relationName();
		this.columnNames = this.relationalInputReader.columnNames().toArray(new String[0]);
		
		this.read();
		
		// Register at this actor system's reaper
		Reaper.watchWithDefaultReaper(this);
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
		
		this.relationName = this.relationalInputReader.relationName();
		this.columnNames = this.relationalInputReader.columnNames().toArray(new String[0]);
		
		this.read();
		this.send(message.getWatermark());
		this.read();
	}

	private void send(int watermark) {
		if (this.buffer.isEmpty())
			this.sender().tell(new Preprocessor.BatchMessage(null, this.relationName, this.columnNames, watermark), this.self());
		else
			this.sender().tell(new Preprocessor.BatchMessage(this.buffer, null, null, watermark), this.self());
	}
	
	private void read() throws Exception {
		this.buffer = new ArrayList<>(this.bufferSize);
		
		while (this.relationalInputReader.hasNext() && this.buffer.size() < this.bufferSize) {
			List<String> record = this.relationalInputReader.next();
			
			this.buffer.add(record);
		}
	}
}
