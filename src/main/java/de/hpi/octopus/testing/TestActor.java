package de.hpi.octopus.testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.octopus.actors.DatasetReader.ReadMessage;
import de.hpi.octopus.actors.DatasetReader.RestartMessage;
import de.hpi.octopus.actors.masters.Preprocessor;
import de.hpi.octopus.structures.DatasetDescriptor;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import lombok.AllArgsConstructor;
import lombok.Data;

public class TestActor extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "testActor";

	public static Props props(ActorRef other) {
		return Props.create(TestActor.class, () -> new TestActor(other));
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class TestMessage implements Serializable {
		private static final long serialVersionUID = -3254147511955012292L;
		private TestMessage() {}
		private int[] data;
	}

	/////////////////
	// Actor State //
	/////////////////
	
	private ActorRef other;
	private int[] data;

	/////////////////////
	// Actor Lifecycle //
	/////////////////////
	
	public TestActor(ActorRef other) {
		this.other = other;
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(TestMessage.class, this::handle)
				//.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(TestMessage message) throws Exception {
		if (this.data != null) {
			for (int i : this.data)
				this.log().info("data: " + i);
			
			for (int i : message.getData())
				this.log().info("message: " + i);
			
			return;
		}
		
		this.data = message.getData();
		
		for (int i : this.data)
			this.log().info("" + i);
		
		message.getData()[0] = 42;
		
		if (this.other != null) {
			this.other.tell(message, this.self());
		}
		else {
			this.data[0] = 88;
			this.sender().tell(message, this.self());
		}
		
		
	}
	
}
