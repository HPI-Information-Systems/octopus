package de.hpi.octopus.actors.slaves;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import de.hpi.octopus.actors.LargeMessageProxy.LargeMessage;
import de.hpi.octopus.actors.Sampler;
import de.hpi.octopus.actors.Storekeeper.SendDataMessage;
import de.hpi.octopus.actors.Validator;
import de.hpi.octopus.actors.masters.Profiler;
import de.hpi.octopus.actors.masters.Profiler.SamplingResultMessage;
import de.hpi.octopus.actors.masters.Profiler.ValidationResultMessage;
import de.hpi.octopus.structures.BitSet;
import de.hpi.octopus.structures.BloomFilter;
import de.hpi.octopus.structures.FunctionalDependency;
import de.hpi.octopus.structures.PliCache;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import lombok.AllArgsConstructor;
import lombok.Data;

public class Worker extends AbstractSlave {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props(ActorRef storekeeper) {
		return Props.create(Worker.class, () -> new Worker(storekeeper));
	}

	public Worker(ActorRef storekeeper) {
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
	public static class DetailedValidationResultMessage implements Serializable {
		private static final long serialVersionUID = 5350065098247367868L;
		private DetailedValidationResultMessage() {}
		private List<FunctionalDependency> invalidFDs;
		private int numCandidates;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class DetailedSamplingResultMessage implements Serializable {
		private static final long serialVersionUID = -1157340528394252282L;
		private DetailedSamplingResultMessage() {}
		private List<FunctionalDependency> invalidFDs;
		private int numComparisons;
		private int numMatches;
	}
	
	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class DataMessage implements Serializable {
		private static final long serialVersionUID = -850201357295330326L;
		private DataMessage() {}
		private int[][][] plis;
		private int[][] records;
		private PliCache pliCache;
		private ActorRef pliCacheManipulator;
		private BloomFilter filter;
		private ActorRef filterManipulator;
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
	
	private ActorRef validator;
	private ActorRef sampler;
	
	private ActorRef profiler; // Note that although we override the profiler ref with every validation/sampling request, a worker is still tied to only one profiler, because it has the data for only one profiler
	
	private List<AttributeFinishedMessage> finishedRhsAttributesCache = new ArrayList<>();
	private boolean[] finishedRhsAttributes;
	
	private Object waitingMessage;

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
				.match(DetailedValidationResultMessage.class, message -> this.time(this::handle, message))
				.match(DetailedSamplingResultMessage.class, message -> this.time(this::handle, message))
				.match(DataMessage.class, message -> this.time(this::handle, message))
				.match(AttributeFinishedMessage.class, message -> this.time(this::handle, message))
				.build()
				.orElse(super.createReceive());
	}

	@Override
	protected String getName() {
		return Worker.DEFAULT_NAME;
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
		this.finishedRhsAttributes = new boolean[message.getPlis().length];
		Arrays.fill(this.finishedRhsAttributes, false);
		this.finishedRhsAttributesCache.forEach(m -> this.finishedRhsAttributes[m.getAttribute()] = true);
		this.finishedRhsAttributesCache = null;
		
		this.validator = this.context().actorOf(Validator.props(message.getRecords(), message.getPlis(), message.getPliCache(), message.getPliCacheManipulator(), message.getFilterManipulator()), Validator.DEFAULT_NAME);
		this.sampler = this.context().actorOf(Sampler.props(message.getRecords(), message.getPlis(), message.getFilter(), message.getFilterManipulator()), Sampler.DEFAULT_NAME);
		
		// Remove waiting message and sender
		Object waitingMessage = this.waitingMessage;
		this.waitingMessage = null;
		
		// Process the waiting message
		if (waitingMessage instanceof ValidationMessage)
			this.validator.tell(new Validator.DetailedValidationMessage((ValidationMessage) waitingMessage, this.finishedRhsAttributes), this.self());
		else
			this.sampler.tell(new Sampler.DetailedSamplingMessage((SamplingMessage) waitingMessage, this.finishedRhsAttributes), this.self());
	}

	private void handle(ValidationMessage message) {
		this.profiler = this.sender();
		
		// Request the validation data if it is not present
		if (this.validator == null) {
			this.waitingMessage = message;
			this.storekeeper.tell(new SendDataMessage(), this.self());
			return;
		}
		
		// Process the validation message
		this.validator.tell(new Validator.DetailedValidationMessage(message, this.finishedRhsAttributes), this.self());
	}
	
	private void handle(SamplingMessage message) {
		this.profiler = this.sender();
		
		// Request the validation data if it is not present
		if (this.sampler == null) {
			this.waitingMessage = message;
			this.storekeeper.tell(new SendDataMessage(), this.self());
			return;
		}
		
		// Process the sampling message
		this.sampler.tell(new Sampler.DetailedSamplingMessage(message, this.finishedRhsAttributes), this.self());
	}
	
	private void handle(DetailedValidationResultMessage message) {
		ValidationResultMessage validationResult = this.aggregate(message);
		this.largeMessageProxy.tell(new LargeMessage<>(validationResult, this.profiler), this.self());
	}

	private void handle(DetailedSamplingResultMessage message) {
		SamplingResultMessage samplingResult = this.aggregate(message);
		this.largeMessageProxy.tell(new LargeMessage<>(samplingResult, this.profiler), this.self());
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

	private ValidationResultMessage aggregate(DetailedValidationResultMessage message) {
		List<FunctionalDependency> invalidFDs = message.getInvalidFDs();
		int numCandidates = message.getNumCandidates();
		
		Collections.sort(invalidFDs);
		
		List<BitSet[]> invalidLhss = new ArrayList<>();
		IntList invalidRhss = new IntArrayList();
		int i = 0;
		while (i < invalidFDs.size()) {
			int currentRhs = invalidFDs.get(i).getRhs();
			
			int j = i + 1;
			while ((j < invalidFDs.size()) && (invalidFDs.get(j).getRhs() == invalidFDs.get(i).getRhs()))
				j++;
			
			BitSet[] currentLhss = new BitSet[j - i];
			for (int k = 0, l = i; l < j; k++, l++)
				currentLhss[k] = invalidFDs.get(l).getLhs();
			
			invalidLhss.add(currentLhss);
			invalidRhss.add(currentRhs);
			
			i = j;
		}
		
		return new ValidationResultMessage(
				invalidLhss.toArray(new BitSet[invalidRhss.size()][]), 
				invalidRhss.toArray(new int[invalidRhss.size()]),
				numCandidates);
	}
	
	private SamplingResultMessage aggregate(DetailedSamplingResultMessage message) {
		List<FunctionalDependency> invalidFDs = message.getInvalidFDs();
		int numComparisons = message.getNumComparisons();
		int numMatches = message.getNumMatches();
		
		Collections.sort(invalidFDs);
		
		List<BitSet[]> invalidLhss = new ArrayList<>();
		IntList invalidRhss = new IntArrayList();
		int i = 0;
		while (i < invalidFDs.size()) {
			int currentRhs = invalidFDs.get(i).getRhs();
			
			int j = i + 1;
			while ((j < invalidFDs.size()) && (invalidFDs.get(j).getRhs() == invalidFDs.get(i).getRhs()))
				j++;
			
			BitSet[] currentLhss = new BitSet[j - i];
			for (int k = 0, l = i; l < j; k++, l++)
				currentLhss[k] = invalidFDs.get(l).getLhs();
			
			invalidLhss.add(currentLhss);
			invalidRhss.add(currentRhs);
			
			i = j;
		}
		
		return new SamplingResultMessage(
				invalidLhss.toArray(new BitSet[invalidRhss.size()][]), 
				invalidRhss.toArray(new int[invalidRhss.size()]),
				numComparisons,
				numMatches);
	}
}