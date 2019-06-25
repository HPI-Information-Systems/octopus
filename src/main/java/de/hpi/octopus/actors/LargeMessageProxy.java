package de.hpi.octopus.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import de.hpi.octopus.configuration.ConfigurationSingleton;
import de.hpi.octopus.structures.KryoPoolSingleton;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import lombok.AllArgsConstructor;
import lombok.Data;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	
	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private LargeMessage() {}
		public LargeMessage(T message, ActorRef receiver) {
			this.message = message;
			this.receiver = receiver;
		}
		private T message;
		private ActorRef receiver;
		private boolean serializeWhenLocal = false; // For testing ...
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class StartLargeMessage implements Serializable {
		private static final long serialVersionUID = 309151527831192292L;
		private StartLargeMessage() {}
		private int senderCounter;
		private ActorRef sender;
		private ActorRef receiver;
	}
	
	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class AckStartLargeMessage implements Serializable {
		private static final long serialVersionUID = -2649789746787658450L;
		private AckStartLargeMessage() {}
		private int senderCounter;
		private int receiverCounter;
	}
	
	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class BytesMessage implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private BytesMessage() {}
		private byte[] bytes;
		private String transferKey;
	}
	
	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class AckBytesMessage implements Serializable {
		private static final long serialVersionUID = -8119751796961718320L;
		private AckBytesMessage() {}
		private String transferKey;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	private int counter = 0;
	
	private Int2ObjectMap<LargeMessage<?>> pendingLargeMessages = new Int2ObjectOpenHashMap<>();
	
	private Map<String, SendState> largeMessagesBeingSend = new HashMap<String, SendState>();
	private Map<String, ReceiveState> largeMessagesBeingReceived = new HashMap<String, ReceiveState>();
	
	private final int maxMessageSize = ConfigurationSingleton.get().getMaxMessageSize();
	
	@Data @AllArgsConstructor
	private class SendState {
		private byte[] bytes;
		private int offset;
	}
	
	@Data @AllArgsConstructor
	private class ReceiveState {
		private List<byte[]> bytes;
		private ActorRef sender;
		private ActorRef receiver;
	}
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(StartLargeMessage.class, this::handle)
				.match(AckStartLargeMessage.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.match(AckBytesMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.proxyFor(receiver);
		
		// Send the large message directly via reference, if the receiver is on the same host
		if (this.self().path().address().equals(receiver.path().address()) && !message.isSerializeWhenLocal()) {
			receiver.tell(message.getMessage(), this.sender());
			return;
		}
		
		// Send start message
		receiverProxy.tell(new StartLargeMessage(this.counter, this.sender(), receiver), this.self());
		
		// Put large message as a pending message
		this.pendingLargeMessages.put(this.counter, message);
		
		this.counter++;
	}
	
	private void handle(StartLargeMessage message) {
		int senderCounter = message.getSenderCounter();
		ActorRef sender = message.getSender();
		ActorRef receiver = message.getReceiver();
		
		// Initialize a receiver state for this message
		String transferKey = senderCounter + "#" + this.counter;
		this.largeMessagesBeingReceived.put(transferKey, new ReceiveState(new ArrayList<byte[]>(), sender, receiver));
		
		// Reply with an acknowledgement
		this.sender().tell(new AckStartLargeMessage(senderCounter, this.counter), this.self());
		
		this.counter++;
	}
	
	private void handle(AckStartLargeMessage message) {
		int senderCounter = message.getSenderCounter();
		int receiverCounter = message.getReceiverCounter();
		LargeMessage<?> largeMessage = this.pendingLargeMessages.remove(senderCounter);

		// Serialize the message into bytes
		byte[] bytes = KryoPoolSingleton.get().toBytesWithClass(largeMessage.getMessage()); // Serialization without class (i.e. toBytesWithoutClass()) is faster, but we need to send class information anyway, so we can simply (and equally efficiently) do it with the serialization itself
		
//		this.log().info(bytes.length + " bytes serialized; " + Arrays.hashCode(bytes) + " is their hash code");
		
		// Initialize a sender state for this message
		String transferKey = senderCounter + "#" + receiverCounter;
		this.largeMessagesBeingSend.put(transferKey, new SendState(bytes, 0));
		
		// Send the first chunk
		BytesMessage bytesMessage = this.getNextBytesMessage(transferKey);
		this.sender().tell(bytesMessage, this.self());
	}
	
	private void handle(AckBytesMessage message) {
		BytesMessage bytesMessage = this.getNextBytesMessage(message.getTransferKey());
		this.sender().tell(bytesMessage, this.self());
	}
	
	private BytesMessage getNextBytesMessage(String transferKey) {
		SendState state = this.largeMessagesBeingSend.get(transferKey);
		
		byte[] bytes = state.getBytes();
		int offset = state.getOffset();
		
		// Get next chunk
		byte[] chunk = Arrays.copyOfRange(bytes, offset, Math.min(offset + this.maxMessageSize, bytes.length));
		state.setOffset(offset + this.maxMessageSize);
		
		// Delete the large message from the map of tracked messages, if this is the last chunk
		if (chunk.length < this.maxMessageSize)
			this.largeMessagesBeingSend.remove(transferKey);
		
		// Return the chunk wrapped in a BytesMessage
		return new BytesMessage(chunk, transferKey);
	}

	private ActorSelection proxyFor(ActorRef actor) {
	//	System.out.println(actor.path());
	//	System.out.println(actor.path().name());
	//	System.out.println(actor.path().address());
	//	System.out.println(actor.path().getElements());
	//	System.out.println(actor.path().child(DEFAULT_NAME));
		
		return this.context().actorSelection(actor.path().child(DEFAULT_NAME));
	}
	
	private void handle(BytesMessage message) {
		String transferKey = message.getTransferKey();
		byte[] chunk = message.getBytes();
		
		// Store the chunk, if it is not complete
		if (chunk.length == this.maxMessageSize) {
			this.largeMessagesBeingReceived.get(transferKey).getBytes().add(chunk);
			
			this.sender().tell(new AckBytesMessage(transferKey), this.self());
			return;
		}
		
		// Re-assemble the byte chunks into one array
		ReceiveState state = this.largeMessagesBeingReceived.remove(transferKey);
		List<byte[]> chunks = state.getBytes();
		byte[] bytes = new byte[chunks.size() * this.maxMessageSize + chunk.length];
		int i = 0;
		for (byte[] c : chunks) {
			for (int j = 0; j < c.length; j++) {
				bytes[i] = c[j];
				i++;
			}
		}
		for (int j = 0; j < chunk.length; j++) {
			bytes[i] = chunk[j];
			i++;
		}
		
		// De-serialize the large message
		Object deserializedMessage = KryoPoolSingleton.get().fromBytes(bytes); // Possible alternative fromBytes(bytes, clazz)

//		this.log().info(bytes.length + " = " + i + " bytes de-serialized; " + Arrays.hashCode(bytes) + " is their hash code");
		
		// Forward the large message to the parent
		state.getReceiver().tell(deserializedMessage, state.getSender());
	}
}
