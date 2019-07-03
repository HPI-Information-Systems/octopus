package de.hpi.octopus.actors.slaves;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import de.hpi.octopus.actors.LargeMessageProxy.LargeMessage;
import de.hpi.octopus.actors.Storekeeper.SendDataMessage;
import de.hpi.octopus.actors.masters.Profiler;
import de.hpi.octopus.actors.masters.Profiler.SamplingResultMessage;
import de.hpi.octopus.actors.masters.Profiler.ValidationResultMessage;
import de.hpi.octopus.logic.SamplingLogic;
import de.hpi.octopus.logic.ValidationLogic;
import de.hpi.octopus.structures.BitSet;
import de.hpi.octopus.structures.BloomFilter;
import lombok.AllArgsConstructor;
import lombok.Data;

public class Validator extends AbstractSlave {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "validator";

	public static Props props(ActorRef storekeeper) {
		return Props.create(Validator.class, () -> new Validator(storekeeper));
	}

	public Validator(ActorRef storekeeper) {
		this.storekeeper = storekeeper;
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class ValidationMessage implements Serializable {// OctopusMessage {//
		private static final long serialVersionUID = -7643194361868862395L;
		private ValidationMessage() {}
		private BitSet[] lhss;
		private int rhs;
	}
	
	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class SamplingMessage implements Serializable {
		private static final long serialVersionUID = -8572954221161108586L;
		private SamplingMessage() {}
		private int attribute;
		private int distance;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class DataMessage implements Serializable {
		private static final long serialVersionUID = -850201357295330326L;
		private DataMessage() {}
		private int[][][] plis;
		private int[][] records;
		private BloomFilter filter;
		private boolean[] finishedRhsAttributes;
	}
	
	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class AttributeFinishedMessage implements Serializable {
		private static final long serialVersionUID = -5409993307300630128L;
		private AttributeFinishedMessage() {}
		private int attribute;
	}

	/////////////////
	// Actor State //
	/////////////////

	private ActorRef storekeeper;
	
	private List<AttributeFinishedMessage> finishedRhsAttributesCache = new ArrayList<>();
	private boolean[] finishedRhsAttributes;
	
	private ValidationLogic validationLogic;
	private SamplingLogic samplingLogic;
	
	private Object waitingMessage;
	private ActorRef waitingMessageSender;

	/////////////////////
	// Actor Lifecycle //
	/////////////////////
	
	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(ValidationMessage.class, message -> this.time(this::handle, message))
				.match(SamplingMessage.class, message -> this.time(this::handle, message))
				.match(DataMessage.class, message -> this.time(this::handle, message))
				.match(AttributeFinishedMessage.class, message -> this.time(this::handle, message))
				.build()
				.orElse(super.createReceive());
	}

	@Override
	protected String getName() {
		return Validator.DEFAULT_NAME;
	}
	
	@Override
	protected String getMasterName() {
		return Profiler.DEFAULT_NAME;
	}

//	private <T> void time(Function<T, Integer> handle, T message) {
//		long t = System.currentTimeMillis();
//		int numNonFDs = handle.apply(message);
//		//this.log().info("Processed {} in {} ms yielding {} non-FDs.", message.getClass().getSimpleName(), System.currentTimeMillis() - t, numNonFDs);
//	}
	
	private <T> void time(Consumer<T> handle, T message) {
		//long t = System.currentTimeMillis();
		handle.accept(message);
		//this.log().info("Processed {} in {} ms.", message.getClass().getSimpleName(), System.currentTimeMillis() - t);
	}
	
	@Override
	protected void handle(TerminateMessage message) {
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.storekeeper.tell(PoisonPill.getInstance(), ActorRef.noSender());
	}
	
	private void handle(DataMessage message) {
		// Store the data
		this.finishedRhsAttributes = message.getFinishedRhsAttributes().clone();
		this.finishedRhsAttributesCache.forEach(m -> this.finishedRhsAttributes[m.getAttribute()] = true);
		this.finishedRhsAttributesCache = null;
		
		this.validationLogic = new ValidationLogic(message.getRecords(), message.getPlis(), message.getFilter(), this.finishedRhsAttributes, this.log());
		this.samplingLogic = new SamplingLogic(message.getRecords(), message.getPlis(), message.getFilter(), this.finishedRhsAttributes, this.log());
		
		// Remove waiting message and sender
		Object waitingMessage = this.waitingMessage;
		ActorRef waitingMessageSender = this.waitingMessageSender;
		this.waitingMessage = null;
		this.waitingMessageSender = null;
		
		// Process the waiting message
		if (waitingMessage instanceof ValidationMessage)
			this.process((ValidationMessage) waitingMessage, waitingMessageSender);
		else
			this.process((SamplingMessage) waitingMessage, waitingMessageSender);
	}
	
	private void handle(SamplingMessage message) {
		// Request the validation data if it is not present
		if (this.samplingLogic == null) {
			this.waitingMessage = message;
			this.waitingMessageSender = this.sender();
			this.storekeeper.tell(new SendDataMessage(), this.self());
			return;
		}
		
		// Process the sampling message
		this.process(message, this.sender());
	}
	
	private void process(SamplingMessage message, ActorRef sender) {
		SamplingResultMessage samplingResult = this.samplingLogic.process(message);
		this.largeMessageProxy.tell(new LargeMessage<>(samplingResult, sender), this.self());
	}
	
	private void handle(ValidationMessage message) {
		// Request the validation data if it is not present
		if (this.validationLogic == null) {
			this.waitingMessage = message;
			this.waitingMessageSender = this.sender();
			this.storekeeper.tell(new SendDataMessage(), this.self());
			return;
		}
		
		// Process the validation message
		this.process(message, this.sender());
	}
	
	private void process(ValidationMessage message, ActorRef sender) {
		ValidationResultMessage validationResult = this.validationLogic.process(message);
		this.largeMessageProxy.tell(new LargeMessage<>(validationResult, sender), this.self());
	}
	
	private void handle(AttributeFinishedMessage message) {
		// Put the message aside if we did not receive any data yet
		if (this.finishedRhsAttributes == null) {
			this.finishedRhsAttributesCache.add(message);
			return;
		}
		
		// Mark rhs attribute as finished to not report any further results for it
		this.finishedRhsAttributes[message.getAttribute()] = true;
	}
}