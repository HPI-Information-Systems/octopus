package de.hpi.octopus.actors.slaves;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.sun.xml.xsom.impl.Ref.Term;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import de.hpi.octopus.actors.DependencySteward;
import de.hpi.octopus.actors.Storekeeper;
import de.hpi.octopus.actors.masters.Profiler;
import de.hpi.octopus.actors.masters.Profiler.SamplingResultMessage;
import de.hpi.octopus.actors.masters.Profiler.ValidationResultMessage;
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

	@Data @AllArgsConstructor
	public static class TerminateMessage implements Serializable {
		private static final long serialVersionUID = 4184578526050265353L;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private int[][][] plis;
	private int[][] records;
	
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
				.match(ValidationMessage.class, this::handle)
				.match(SamplingMessage.class, this::handle)
				.match(DataMessage.class, this::handle)
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

	private void handle(TerminateMessage message) {
		this.self().tell(PoisonPill.getInstance(), this.self());
		this.storekeeper.tell(PoisonPill.getInstance(), ActorRef.noSender());
	}
	
	private void handle(DataMessage message) {
		// Store the data
		this.plis = message.getPlis();
		this.records = message.getRecords();
		
		// Process the waiting message
		if (this.waitingMessage instanceof ValidationMessage)
			this.process((ValidationMessage) this.waitingMessage, this.waitingMessageSender);
		else
			this.process((SamplingMessage) this.waitingMessage, this.waitingMessageSender);
		this.waitingMessage = null;
		this.waitingMessageSender = null;
	}
	
	private void handle(SamplingMessage message) {
		// If the validation data is not present, put the sampling message to waiting and request the data
		if (this.plis == null) {
			this.waitingMessage = message;
			this.waitingMessageSender = this.sender();
			this.storekeeper.tell(new Storekeeper.SendDataMessage(), this.self());
			return;
		}
		
		// Process the sampling message
		this.process(message, this.sender());
	}
	
	private void process(SamplingMessage message, ActorRef sender) {
		Set<BitSet> matches = new HashSet<>();
		
		// Match all records with their "distance" neighbor w.r.t. the pli of the given "attribute"
		BitSet match = new BitSet(this.plis.length);
		int numComparisons = 0;
		for (int[] cluster : this.plis[message.getAttribute()]) {
			for (int record = 0; record < cluster.length - message.getDistance(); record++) {
				for (int attribute = 0; attribute < this.plis.length; attribute++)
					if (isEqual(this.records[record][attribute], this.records[record + message.getDistance()][attribute]))
						match.set(attribute);
				numComparisons++;
				
				if (!matches.contains(match))
					matches.add((BitSet) match.clone());
				matches.clear();
			}
		}
		int numMatches = matches.size();
		
		// Convert matches into invalid FDs
		List<FunctionalDependency> invalidFDs = new ArrayList<>(matches.size() * this.plis.length / 2);
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
	}
	
	private void handle(ValidationMessage message) {
		// If the validation data is not present, put the validation message to waiting and request the data
		if (this.plis == null) {
			this.waitingMessage = message;
			this.waitingMessageSender = this.sender();
			this.storekeeper.tell(new Storekeeper.SendDataMessage(), this.self());
			return;
		}
		
		// Process the validation message
		this.process(message, this.sender());
	}
	
	private void process(ValidationMessage message, ActorRef sender) {
		// Initialize a container for the invalid FDs
		List<FunctionalDependency> invalidFDs = new ArrayList<>(message.getLhss().length);
		
		// Process the validation message		
		int rhs = message.getRhs();
		for (BitSet lhsBitSet : message.getLhss()) { // The lhs should have at least one attribute, because we do not discover {}->A FDs
			int[] lhs = toArray(lhsBitSet);
			
			int[] violation = this.findViolation(lhs, rhs);
			if (violation != null) {
				// Add the violated FD to the container of invalid FDs
			//	invalidFDs.add(new FunctionalDependency(lhsBitSet, rhs)); // Not necessary, because we compare the two records and find and add this non-FD again 
				
				// Compare the two records that caused the violation to find violations for other FDs (= execute comparison suggestion)
				BitSet invalidLhs = new BitSet(this.plis.length);
				IntList invalidRhss = new IntArrayList();
				for (int attribute = 0; attribute < this.plis.length; attribute++) {
					if ((this.records[violation[0]][attribute] == -1) || (this.records[violation[0]][attribute] != this.records[violation[1]][attribute]))
						invalidRhss.add(attribute);
					else
						invalidLhs.set(attribute);
				}
				for (int i = 0; i < invalidRhss.size(); i++)
					invalidFDs.add(new FunctionalDependency(invalidLhs, rhs));
			}
		}
		
		// Send the result to the sender of the validation message
		sender.tell(toValidationResultMessage(invalidFDs), this.self());
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
				
				// If the lhs value hash is known, test if the rhs is the same.
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
	
	private boolean isMatch(int recordID1, int recordID2, int[] attributes) {
		for (int attribute : attributes)
			if (isDifferent(this.records[recordID1][attribute], this.records[recordID2][attribute]))
				return false;
		return true;
	}
	
	private static boolean isEqual(int value1, int value2) {
		return !isDifferent(value1, value2);
	}
	
	private static boolean isDifferent(int value1, int value2) {
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
				invalidLhss.toArray(new BitSet[invalidFDs.size()][]), 
				invalidRhss.toArray(new int[invalidFDs.size()]));
	}
}