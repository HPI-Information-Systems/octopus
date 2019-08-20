package de.hpi.octopus.logic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import akka.event.LoggingAdapter;
import de.hpi.octopus.actors.masters.Profiler.ValidationResultMessage;
import de.hpi.octopus.actors.slaves.Validator.ValidationMessage;
import de.hpi.octopus.structures.BitSet;
import de.hpi.octopus.structures.BloomFilter;
import de.hpi.octopus.structures.FunctionalDependency;
import de.hpi.octopus.structures.PliCache;
import de.hpi.octopus.structures.ValueCombination;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class ValidationLogic {

	private final int[][] records;
	private final int[][][] plis;
	private final PliCache pliCache;
	private final BloomFilter filter;
	private final boolean[] finishedRhsAttributes;
	
	@SuppressWarnings("unused")
	private final LoggingAdapter log;
	
	public ValidationLogic(final int[][] records, final int[][][] plis, final PliCache pliCache, final BloomFilter filter, final boolean[] finishedRhsAttributes, final LoggingAdapter log) {
		this.records = records;
		this.plis = plis;
		this.pliCache = pliCache;
		this.filter = filter;
		this.finishedRhsAttributes = finishedRhsAttributes;
		this.log = log;
		
	}

	public ValidationResultMessage process(ValidationMessage message) {
		// Initialize a container for the invalid FDs
		List<FunctionalDependency> invalidFDs = new ArrayList<>(message.getLhss().length);
		
		// Process the validation message		
		int rhs = message.getRhs();
		for (BitSet lhsBitSet : message.getLhss()) { // The lhs should have at least one attribute
			int[] lhs = ConversionLogic.bitset2Array(lhsBitSet);
			
//			int[] violation = this.findViolation(lhs, rhs);
			int[] violation = this.findViolationCached(lhs, rhs);
			if (violation == null)
				continue;
			
			// Add the violated FD to the container of invalid FDs
		//	invalidFDs.add(new FunctionalDependency(lhsBitSet, rhs)); // Not necessary, because we compare the two records and find and add this non-FD again 
			
			// Compare the two records that caused the violation to find violations for other FDs (= execute comparison suggestion)
			BitSet invalidLhs = new BitSet(this.plis.length);
			for (int attribute = 0; attribute < this.plis.length; attribute++)
				if (MatchingLogic.isMatch(this.records[violation[0]], this.records[violation[1]], attribute))
					invalidLhs.set(attribute);
			
			// Add the comparison result to the filter so that we do not report the same result again during sampling
			this.filter.add(invalidLhs);
			
			// Derive the fds from the match
			for (int invalidRhs = 0; invalidRhs < this.plis.length; invalidRhs++)
				if (!invalidLhs.get(invalidRhs) && !this.finishedRhsAttributes[invalidRhs])
					invalidFDs.add(new FunctionalDependency(invalidLhs, invalidRhs));
		}
		
		// Send the result to the sender of the validation message
		final ValidationResultMessage validationMessage = ConversionLogic.fds2ValidationResultMessage(invalidFDs, rhs, message.getLhss().length);
		
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
						if (MatchingLogic.isMatch(record1, record2, lhs) && !MatchingLogic.isMatch(record1, record2, rhs)) {
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
					if (!MatchingLogic.isEqual(rhsValueAndRecord[0], rhsValue)) {
						final int[] violation = {recordID, rhsValueAndRecord[1]};
						return violation;
					}
				}
			}
		}
		return null;
	}

	private int[] findViolationCached(int[] lhs, int rhs) {
		// Find a small pli to start with in the cache; create it if necessary
		int[][] pivotPli = this.plis[lhs[0]];
		if (lhs.length > 1) {
			// Take the pivotPli from the cache
			pivotPli = this.pliCache.get(lhs[0], lhs[1]);
			
			// If the cache does not contain the intersection of the first two attributes, create it and add it
			if (pivotPli == null) {
				pivotPli = this.intersect(this.plis[lhs[0]], lhs[1]);
				int[] attributes = {lhs[0], lhs[1]};
				this.pliCache.add(attributes, pivotPli);
			}
		}
		
		for (int[] cluster : pivotPli) {
			if (cluster.length < 40) { // For small clusters, compare all records directly without hashing
				for (int i = 0; i < cluster.length - 1; i++) {
					final int recordID1 = cluster[i];
					
					for (int j = i + 1; j < cluster.length; j++) {
						final int recordID2 = cluster[j];
						
						final int[] record1 = this.records[recordID1];
						final int[] record2 = this.records[recordID2];
						if (MatchingLogic.isMatch(record1, record2, lhs) && !MatchingLogic.isMatch(record1, record2, rhs)) {
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
					if (!MatchingLogic.isEqual(rhsValueAndRecord[0], rhsValue)) {
						final int[] violation = {recordID, rhsValueAndRecord[1]};
						return violation;
					}
				}
			}
		}
		return null;
	}
	
	private int[][] intersect(int[][] pli, int attribute) {
		List<IntList> clusters = new ArrayList<>(pli.length);
		
		// Intersect the given pli with the pli of the specified attribute
		for (int[] cluster : pli) {
			final Int2ObjectOpenHashMap<IntList> clusterMap = new Int2ObjectOpenHashMap<>();
			for (int i = 0; i < cluster.length; i++) {
				final int recordId1 = cluster[i];
				final int clusterId2 = this.records[recordId1][attribute];
				
				if (!clusterMap.containsKey(clusterId2)) {
					IntArrayList newCluster = new IntArrayList();
					newCluster.add(recordId1);
					clusterMap.put(clusterId2, newCluster);
				}
				else {
					clusterMap.get(clusterId2).add(recordId1);
				}
			}
			clusters.addAll(clusterMap.values());
		}
		
		int[][] intersectedPli = new int[clusters.size()][];
		for (int i = 0; i < intersectedPli.length; i++)
			intersectedPli[i] = clusters.get(i).toIntArray();
		
		return intersectedPli;
	}
}
