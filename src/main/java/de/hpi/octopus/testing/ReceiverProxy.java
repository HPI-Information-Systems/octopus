package de.hpi.octopus.testing;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class ReceiverProxy extends AbstractLoggingActor {

	private ActorRef receiver;
	
	private int sequenceNumber = -1;
	private Map<Integer, String> waitingMessages = new HashMap<>();
	
	public ReceiverProxy(ActorRef receiver) {
		this.receiver = receiver;
	}

	public static Props props(ActorRef receiver) {
		return Props.create(ReceiverProxy.class, () -> new ReceiverProxy(receiver));
	}

	public static class ReliableMessage implements Serializable {
		private static final long serialVersionUID = -3254147511955012292L;
		public ReliableMessage() {}
		public ReliableMessage(int sequenceNumber, String message, ActorRef sender) {
			this.sequenceNumber = sequenceNumber;
			this.message = message;
			this.sender = sender;
		}
		public int sequenceNumber;
		public String message;
		public ActorRef sender;
	}
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(ReliableMessage.class, this::handle)
				.matchAny(object -> this.log().info("Unknown message: \"{}\"", object.toString()))
				.build();
	}
	
	private void handle(ReliableMessage message) {
		// Send acknowledgement
		this.sender().tell(message.sequenceNumber, this.self());
		
		// Ignore message, if it is a duplicate
		if (message.sequenceNumber <= this.sequenceNumber || this.waitingMessages.containsKey(message.sequenceNumber)) {
			return;
		}
		
		// Hold message, if a previous message is still pending
		if (this.sequenceNumber + 1 > message.sequenceNumber) {
			this.waitingMessages.put(message.sequenceNumber, message.message);
			return;
		}
		
		// Forward message
		this.sequenceNumber++;
		this.receiver.tell(message.message, message.sender);
		
		// Forward all messages that were waiting for this message
		while (this.waitingMessages.containsKey(this.sequenceNumber + 1)) {
			this.sequenceNumber++;
			String waitingMessage = this.waitingMessages.remove(this.sequenceNumber);
			this.receiver.tell(waitingMessage, message.sender);
		}
	}
}
