package de.hpi.octopus.actors.slaves;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import de.hpi.octopus.actors.LargeMessageProxy.LargeMessage;
import de.hpi.octopus.actors.Storekeeper.SendDataMessage;
import de.hpi.octopus.actors.Storekeeper.SendFilterMessage;
import de.hpi.octopus.actors.masters.Profiler;
import de.hpi.octopus.actors.masters.Profiler.SamplingResultMessage;
import de.hpi.octopus.actors.masters.Profiler.ValidationResultMessage;
import de.hpi.octopus.structures.BitSet;
import de.hpi.octopus.structures.BloomFilter;
import de.hpi.octopus.structures.FunctionalDependency;
import de.hpi.octopus.structures.ValueCombination;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
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
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class FilterMessage implements Serializable {
		private static final long serialVersionUID = -1933265769147934970L;
		private FilterMessage() {}
		private BloomFilter filter;
	}

	@Data @AllArgsConstructor
	public static class TerminateMessage implements Serializable {
		private static final long serialVersionUID = 4184578526050265353L;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private int[][][] plis;
	private int[][] records;
	private BloomFilter filter;
	//private HashSet<BitSet> filter2 = new HashSet<>();
	
	private ActorRef storekeeper;
	
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
				.match(FilterMessage.class, message -> this.time(this::handle, message))
				.match(TerminateMessage.class, this::handle)
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

	private <T> void time(Function<T, Integer> handle, T message) {
		long t = System.currentTimeMillis();
		int numNonFDs = handle.apply(message);
//		this.log().info("Processed {} in {} ms yielding {} non-FDs.", message.getClass().getSimpleName(), System.currentTimeMillis() - t, numNonFDs);
	}
	
	private void handle(TerminateMessage message) {
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.storekeeper.tell(PoisonPill.getInstance(), ActorRef.noSender());
	}
	
	private int handle(DataMessage message) {
		// Store the data
		this.plis = message.getPlis();
		this.records = message.getRecords();
		
		// Remove waiting message and sender
		Object waitingMessage = this.waitingMessage;
		ActorRef waitingMessageSender = this.waitingMessageSender;
		this.waitingMessage = null;
		this.waitingMessageSender = null;
		
		// Process the waiting message
		if (waitingMessage instanceof ValidationMessage)
			return this.process((ValidationMessage) waitingMessage, waitingMessageSender);
		return this.process((SamplingMessage) waitingMessage, waitingMessageSender);
	}
	
	private int handle(FilterMessage message) {
		// Store the filter
		this.filter = message.getFilter();
		
		// Remove waiting message and sender
		Object waitingMessage = this.waitingMessage;
		ActorRef waitingMessageSender = this.waitingMessageSender;
		this.waitingMessage = null;
		this.waitingMessageSender = null;
		
		// Process the waiting message
		return this.process((SamplingMessage) waitingMessage, waitingMessageSender);
	}
	
	private int handle(SamplingMessage message) {
		// Request the validation data if it is not present
		if (this.plis == null) {
			this.waitingMessage = message;
			this.waitingMessageSender = this.sender();
			this.storekeeper.tell(new SendDataMessage(), this.self());
			return 0;
		}
		
		// Process the sampling message
		return this.process(message, this.sender());
	}
	
	private int process(SamplingMessage message, ActorRef sender) {
		// Request the filter data if it is not present
		if (this.filter == null) {
			this.waitingMessage = message;
			this.waitingMessageSender = sender;
			this.storekeeper.tell(new SendFilterMessage(), this.self());
			return 0;
		}
		
		List<BitSet> matches = new ArrayList<>();
		
		// Match all records with their "distance" neighbor w.r.t. the pli of the given "attribute"
		BitSet match = new BitSet(this.plis.length);
		int numComparisons = 0;
		int numMatches = 0;
/*		if (message.getDistance() == 1) { // For distance 1, we count all unique matches of this attribute but only report completely new matches
			Set<BitSet> matchesSet = new HashSet<>();
			for (int[] cluster : this.plis[message.getAttribute()]) {
				for (int index = 0; index < cluster.length - message.getDistance(); index++) {
					for (int attribute = 0; attribute < this.plis.length; attribute++)
						if (isMatch(cluster[index], cluster[index + message.getDistance()], attribute))
							match.set(attribute);
					numComparisons++;
					
					if (!matchesSet.contains(match)) {
						BitSet clone = match.clone();
						matchesSet.add(clone);
						if (this.filter.add(clone))
							matches.add(clone);
					}
					match.clear();
				}
			}
			numMatches = matchesSet.size();
		}
		else { // For distances >1, we count and report only those matches that are completely new over all attributes
*/			for (int[] cluster : this.plis[message.getAttribute()]) {
				for (int index = 0; index < cluster.length - message.getDistance(); index++) {
					for (int attribute = 0; attribute < this.plis.length; attribute++)
						if (isMatch(cluster[index], cluster[index + message.getDistance()], attribute))
							match.set(attribute);
					numComparisons++;
					
					if (this.filter.add(match))
						matches.add(match.clone());
					match.clear();
				}
			}
			numMatches = matches.size();
//		}
			
		// Convert matches into invalid FDs
		List<FunctionalDependency> invalidFDs = new ArrayList<>(numMatches * this.plis.length / 2);
		for (BitSet bitsetLhs : matches) {
			for (int rhs = 0; rhs < this.plis.length; rhs++) {
				if (!bitsetLhs.get(rhs)) {
					invalidFDs.add(new FunctionalDependency(bitsetLhs, rhs));
				}
			}
		}
		matches = null;
		
		// Send the result to the sender of the sampling message
		ValidationResultMessage validationResult = toValidationResultMessage(invalidFDs);
		SamplingResultMessage samplingResult = new SamplingResultMessage(validationResult.getInvalidLhss(), validationResult.getInvalidRhss(), numComparisons, numMatches);
		this.largeMessageProxy.tell(new LargeMessage<>(samplingResult, sender), this.self());
		
		return invalidFDs.size();
	}
	
	private int handle(ValidationMessage message) {
		// Request the validation data if it is not present
		if (this.plis == null) {
			this.waitingMessage = message;
			this.waitingMessageSender = this.sender();
			this.storekeeper.tell(new SendDataMessage(), this.self());
			return 0;
		}
		
		// Process the validation message
		return this.process(message, this.sender());
	}
	
	private int process(ValidationMessage message, ActorRef sender) {
		// Initialize a container for the invalid FDs
		List<FunctionalDependency> invalidFDs = new ArrayList<>(message.getLhss().length);
		
		// Process the validation message		
		int rhs = message.getRhs();
		for (BitSet lhsBitSet : message.getLhss()) { // The lhs should have at least one attribute, because we do not discover {}->A FDs
			int[] lhs = toArray(lhsBitSet);
			
			int[] violation = this.findViolation(lhs, rhs);
			if (violation == null)
				continue;
			
			// Add the violated FD to the container of invalid FDs
		//	invalidFDs.add(new FunctionalDependency(lhsBitSet, rhs)); // Not necessary, because we compare the two records and find and add this non-FD again 
			
			// Compare the two records that caused the violation to find violations for other FDs (= execute comparison suggestion)
			BitSet invalidLhs = new BitSet(this.plis.length);
			IntList invalidRhss = new IntArrayList();
			for (int attribute = 0; attribute < this.plis.length; attribute++) {
				if (this.isMatch(violation[0], violation[1], attribute))
					invalidLhs.set(attribute);
				else
					invalidRhss.add(attribute);
			}
			
			// Add the comparison result to the filter so that we do not report the same result again during sampling
			if (this.filter != null)
				this.filter.add(invalidLhs);
			
			// Derive the fds from the match
			for (int invalidRhs : invalidRhss)
				invalidFDs.add(new FunctionalDependency(invalidLhs, invalidRhs));
		}
		
		// Send the result to the sender of the validation message
		ValidationResultMessage validationMessage = toValidationResultMessage(invalidFDs);
		this.largeMessageProxy.tell(new LargeMessage<>(validationMessage, sender), this.self());
		
		return invalidFDs.size();
	}
	
	// For ncvoter_Statewide_10001r_71c:
	// Found 169316 FDs in 35985 ms	with <10
	// Found 169316 FDs in 34399 ms	with <20
	// Found 169316 FDs in 34732 ms	with <20
	// Found 169316 FDs in 33652 ms	with <30
	// Found 169316 FDs in 33413 ms	with <40
	// Found 169316 FDs in 33729 ms	with <50
	// Found 169316 FDs in 34854 ms	with <60
	private int[] findViolation(int[] lhs, int rhs) {
		for (int[] cluster : this.plis[lhs[0]]) {
			if (cluster.length < 40) { // For small clusters, compare all records directly without hashing
				for (int i = 0; i < cluster.length - 1; i++) {
					final int recordID1 = cluster[i];
					
					for (int j = i + 1; j < cluster.length; j++) {
						final int recordID2 = cluster[j];
						
						if (this.isMatch(recordID1, recordID2, lhs))
							if (!isMatch(recordID1, recordID2, rhs)) {
								final int[] violation = {recordID1, recordID2};
								return violation;
							}
					}
				}
			}
			else { // For large clusters, compare the records via hashing
				final Map<ValueCombination, int[]> lhsValue2rhsValue = new HashMap<>();
				for (int i = 0; i < cluster.length; i++) {
					final int recordID = cluster[i];
					
					ValueCombination lhsValue = new ValueCombination(this.records[recordID], lhs);
					final int rhsValue = this.records[recordID][rhs];

					if (lhsValue.isUnique())
						continue;
					
					// If the lhs value is new, add a new mapping to the rhs value
					if (!lhsValue2rhsValue.containsKey(lhsValue)) {
						final int[] rhsValueAndRecord = {rhsValue, recordID};
						lhsValue2rhsValue.put(lhsValue, rhsValueAndRecord);
						continue;
					}
					
					// If the lhs value hash is known, test if the rhs value is the same and return a violation if not
					final int[] rhsValueAndRecord = lhsValue2rhsValue.get(lhsValue);
					if (!isEqual(rhsValueAndRecord[0], rhsValue)) {
						final int[] violation = {recordID, rhsValueAndRecord[1]};
						return violation;
					}
				}
			}
		}
		return null;
	}
	
	private boolean isMatch(final int recordID1, final int recordID2, final int[] attributes) {
		for (int i = 0; i < attributes.length; i++)
			if (!isEqual(this.records[recordID1][attributes[i]], this.records[recordID2][attributes[i]]))
				return false;
		return true;
	}

	private boolean isMatch(final int recordID1, final int recordID2, final int attribute) {
		return isEqual(this.records[recordID1][attribute], this.records[recordID2][attribute]);
	}

	private static boolean isEqual(final int value1, final int value2) {
		return (value1 == value2) && (value1 != -1);
	}
	
	private static int[] toArray(BitSet bitSet) {
		int[] array = new int[bitSet.cardinality()];
		for (int trueBit = bitSet.nextSetBit(0), index = 0; trueBit >= 0; trueBit = bitSet.nextSetBit(trueBit + 1), index++)
			array[index] = trueBit;
		return array;
	}
	
	private static ValidationResultMessage toValidationResultMessage(List<FunctionalDependency> invalidFDs) {
		Collections.sort(invalidFDs);
		List<BitSet[]> invalidLhss = new ArrayList<>();
		IntList invalidRhss = new IntArrayList();
		int i = 0;
		while (i < invalidFDs.size()) {
			int j = i + 1;
			while ((j < invalidFDs.size()) && (invalidFDs.get(j).getRhs() == invalidFDs.get(i).getRhs()))
				j++;
			
			BitSet[] currentLhss = new BitSet[j - i];
			for (int k = 0, l = i; l < j; k++, l++)
				currentLhss[k] = invalidFDs.get(l).getLhs();
			
			invalidLhss.add(currentLhss);
			invalidRhss.add(invalidFDs.get(i).getRhs());
			
			i = j;
		}
		
		return new ValidationResultMessage(
				invalidLhss.toArray(new BitSet[invalidRhss.size()][]), 
				invalidRhss.toArray(new int[invalidRhss.size()]));
	}
}