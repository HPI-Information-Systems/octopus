package de.hpi.octopus.testing;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;

import akka.Done;
import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.SourceRef;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamRefs;
import akka.util.ByteString;
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
				.match(String.class, this::handle)
				.match(SourceRef.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
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
	
	private Materializer materializer = ActorMaterializer.create(this.context());
	
//	private Flow<String, Byte, NotUsed> serialize(String object) {
//		return Flow.of(String.class).mapConcat(object -> object.getBytes());
//	}
	
	private void handle(String message) {
		ByteString serializedMessage = ByteString.fromByteBuffer(ByteBuffer.wrap(message.getBytes()));
		Source<ByteString, NotUsed> source = Source.single(serializedMessage);
		
		CompletionStage<SourceRef<ByteString>> completionStage = source.runWith(StreamRefs.sourceRef(), this.materializer);

		Patterns.pipe(completionStage, context().dispatcher()).to(this.other);
	}
	
	private void handle(SourceRef<ByteString> sourceRef) {
		CompletionStage<Done> completionStage = sourceRef.getSource().runWith(Sink.foreach(log -> System.out.println(log)), this.materializer);
		
		completionStage.whenComplete((done, throwable) -> {
			// ...
		});
	}
}
