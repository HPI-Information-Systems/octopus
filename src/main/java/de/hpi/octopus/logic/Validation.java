package de.hpi.octopus.logic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import akka.event.LoggingAdapter;
import de.hpi.octopus.actors.masters.Profiler.ValidationResultMessage;
import de.hpi.octopus.actors.slaves.Validator.ValidationMessage;
import de.hpi.octopus.structures.BitSet;
import de.hpi.octopus.structures.BloomFilter;
import de.hpi.octopus.structures.FunctionalDependency;
import de.hpi.octopus.structures.ValueCombination;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class Validation {

	private final int[][] records;
	private final int[][][] plis;
	private final BloomFilter filter;
	
	@SuppressWarnings("unused")
	private final LoggingAdapter log;
	
	public Validation(final int[][] records, final int[][][] plis, final BloomFilter filter, final LoggingAdapter log) {
		this.records = records;
		this.plis = plis;
		this.filter = filter;
		this.log = log;
	}
	
	public ValidationResultMessage process(ValidationMessage message) {
		// Initialize a container for the invalid FDs
		List<FunctionalDependency> invalidFDs = new ArrayList<>(message.getLhss().length);
		
		// Process the validation message		
		int rhs = message.getRhs();
		for (BitSet lhsBitSet : message.getLhss()) { // The lhs should have at least one attribute
			int[] lhs = Conversion.bitset2Array(lhsBitSet);
			
			int[] violation = this.findViolation(lhs, rhs);
			if (violation == null)
				continue;
			
			// Add the violated FD to the container of invalid FDs
		//	invalidFDs.add(new FunctionalDependency(lhsBitSet, rhs)); // Not necessary, because we compare the two records and find and add this non-FD again 
			
			// Compare the two records that caused the violation to find violations for other FDs (= execute comparison suggestion)
			BitSet invalidLhs = new BitSet(this.plis.length);
			IntList invalidRhss = new IntArrayList();
			for (int attribute = 0; attribute < this.plis.length; attribute++) {
				if (Matching.isMatch(this.records[violation[0]], this.records[violation[1]], attribute))
					invalidLhs.set(attribute);
				else
					invalidRhss.add(attribute);
			}
			
			// Add the comparison result to the filter so that we do not report the same result again during sampling
			this.filter.add(invalidLhs);
			
			// Derive the fds from the match
			for (int invalidRhs : invalidRhss)
				invalidFDs.add(new FunctionalDependency(invalidLhs, invalidRhs));
		}
		
		// Send the result to the sender of the validation message
		final ValidationResultMessage validationMessage = Conversion.fds2ValidationResultMessage(invalidFDs, rhs, message.getLhss().length);
		
		return validationMessage;
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
						
						final int[] record1 = this.records[recordID1];
						final int[] record2 = this.records[recordID2];
						if (Matching.isMatch(record1, record2, lhs) && !Matching.isMatch(record1, record2, rhs)) {
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
					
					// If the lhs value is unique, continue
					if (ValueCombination.isUnique(this.records[recordID], lhs))
						continue;
					
					ValueCombination lhsValue = new ValueCombination(this.records[recordID], lhs);
					final int rhsValue = this.records[recordID][rhs];
					
					// If the lhs value is new, add a new mapping to the rhs value
					if (!lhsValue2rhsValue.containsKey(lhsValue)) {
						final int[] rhsValueAndRecord = {rhsValue, recordID};
						lhsValue2rhsValue.put(lhsValue, rhsValueAndRecord);
						continue;
					}
					
					// If the lhs value hash is known, test if the rhs value is the same and return a violation if not
					final int[] rhsValueAndRecord = lhsValue2rhsValue.get(lhsValue);
					if (!Matching.isEqual(rhsValueAndRecord[0], rhsValue)) {
						final int[] violation = {recordID, rhsValueAndRecord[1]};
						return violation;
					}
				}
			}
		}
		return null;
	}
}
