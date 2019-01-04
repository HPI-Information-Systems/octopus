package de.hpi.octopus.actors.slaves;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.octopus.actors.Storekeeper;
import de.hpi.octopus.actors.masters.Profiler;
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
	public static class DataMessage implements Serializable {
		private static final long serialVersionUID = -850201357295330326L;
		private DataMessage() {}
		private int[][][] plis;
		private int[][] records;
	}

	/////////////////
	// Actor State //
	/////////////////

	private int[][][] plis;
	private int[][] records;
	
	private ActorRef storekeeper;
	
	private ValidationMessage waitingValidationMessage;
	private ActorRef waitingValidationSender;

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
				.match(DataMessage.class, this::handle)
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
	
	private void handle(DataMessage message) {
		// Store the data
		this.plis = message.getPlis();
		this.records = message.getRecords();
		
		// Process the waiting validation message
		this.process(this.waitingValidationMessage, this.waitingValidationSender);
		this.waitingValidationMessage = null;
		this.waitingValidationSender = null;
	}
	
	private void handle(ValidationMessage message) {
		// If the validation data is not present, put the validation message to waiting and request the data
		if (this.plis == null) {
			this.waitingValidationMessage = message;
			this.waitingValidationSender = this.sender();
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
			int[] lhs = new int[lhsBitSet.cardinality()];
			for (int attribute = lhsBitSet.nextSetBit(0), i = 0; attribute >= 0; attribute = lhsBitSet.nextSetBit(attribute + 1), i++)
				lhs[i] = attribute;
			
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
		
		// Send result to the sender of the validation message
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
		sender.tell(new ValidationResultMessage(
				invalidLhss.toArray(new BitSet[invalidFDs.size()][]), 
				invalidRhss.toArray(new int[invalidFDs.size()])), this.self());
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
			if ((this.records[recordID1][attribute] == -1) || (this.records[recordID1][attribute] != this.records[recordID2][attribute]))
				return false;
		return true;
	}
}