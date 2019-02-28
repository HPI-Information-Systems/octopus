package de.hpi.octopus.actors.slaves;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import de.hpi.octopus.actors.Storekeeper.SendDataMessage;
import de.hpi.octopus.actors.Storekeeper.SendFilterMessage;
import de.hpi.octopus.actors.masters.Profiler;
import de.hpi.octopus.actors.masters.Profiler.SamplingResultMessage;
import de.hpi.octopus.actors.masters.Profiler.ValidationResultMessage;
import de.hpi.octopus.structures.BloomFilter;
import de.hpi.octopus.structures.FunctionalDependency;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
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
	public static class ValidationMessage implements Serializable {
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
		for (int[] cluster : this.plis[message.getAttribute()]) {
			for (int record = 0; record < cluster.length - message.getDistance(); record++) {
				for (int attribute = 0; attribute < this.plis.length; attribute++)
					if (isMatch(record, record + message.getDistance(), attribute))
						match.set(attribute);
				numComparisons++;
				
				if (this.filter.add(match))
					matches.add((BitSet) match.clone());
				match.clear();
			}
		}
		int numMatches = matches.size();
		
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
		sender.tell(samplingResult, this.self());
		
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
			for (int invalidRhs : invalidRhss)
				invalidFDs.add(new FunctionalDependency(invalidLhs, invalidRhs));
		}
		
		// Send the result to the sender of the validation message
		sender.tell(toValidationResultMessage(invalidFDs), this.self());
		
		return invalidFDs.size();
	}
	
	private int[] findViolation(int[] lhs, int rhs) {
		for (int[] cluster : this.plis[lhs[0]]) {
			Int2ObjectOpenHashMap<int[]> lhsHash2rhsValue = new Int2ObjectOpenHashMap<>();
			for (int recordID : cluster) {
				int lhsHash = this.hash(lhs, recordID);
				int rhsValue = this.records[recordID][rhs];
				
				// If the lhs value hash is new, add a new mapping to the rhs value.
				if (!lhsHash2rhsValue.containsKey(lhsHash)) {
					int[] rhsValueAndRecord = new int[2];
					rhsValueAndRecord[0] = rhsValue;
					rhsValueAndRecord[1] = recordID;
					lhsHash2rhsValue.put(lhsHash, rhsValueAndRecord);
					continue;
				}
				
				// If the lhs value hash is known, test if the rhs value is the same.
				if (lhsHash2rhsValue.get(lhsHash)[0] == rhsValue) {
					continue;
				}
				
				// If the lhs value is the same, the FD is violated, because the rhs value differs here.
				int recordIDOld = lhsHash2rhsValue.get(lhsHash)[1];
				if (this.isMatch(recordID, recordIDOld, lhs)) {
					int[] violation = {recordID, recordIDOld};
					return violation;
				}
			}
		}
		return null;
	}
	
	private int hash(int[] lhs, int recordID) {
		int hash = 1;
		int index = lhs.length;
		while (index-- != 1)
			hash = 31 * hash + this.records[recordID][lhs[index]];
		return hash;
	}

	private boolean isMatch(final int recordID1, final int recordID2, final int[] attributes) {
		for (int attribute : attributes)
			if (isDifferent(this.records[recordID1][attribute], this.records[recordID2][attribute]))
				return false;
		return true;
	}

	private boolean isMatch(final int recordID1, final int recordID2, final int attribute) {
		return isEqual(this.records[recordID1][attribute], this.records[recordID2][attribute]);
	}

	private static boolean isEqual(final int value1, final int value2) {
		return (value1 == value2) && (value1 != -1);
	}
	
	private static boolean isDifferent(final int value1, final int value2) {
		return (value1 == -1) || (value1 != value2);
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