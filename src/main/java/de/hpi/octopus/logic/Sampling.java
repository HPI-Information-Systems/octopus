package de.hpi.octopus.logic;

import java.util.ArrayList;
import java.util.List;

import akka.event.LoggingAdapter;
import de.hpi.octopus.actors.masters.Profiler.SamplingResultMessage;
import de.hpi.octopus.actors.masters.Profiler.ValidationResultMessage;
import de.hpi.octopus.actors.slaves.Validator.SamplingMessage;
import de.hpi.octopus.structures.BitSet;
import de.hpi.octopus.structures.BloomFilter;
import de.hpi.octopus.structures.FunctionalDependency;

public class Sampling {

	private final int[][] records;
	private final int[][][] plis;
	private final BloomFilter filter;
	
	@SuppressWarnings("unused")
	private final LoggingAdapter log;
	
	public Sampling(final int[][] records, final int[][][] plis, final BloomFilter filter, final LoggingAdapter log) {
		this.records = records;
		this.plis = plis;
		this.filter = filter;
		this.log = log;
	}
	
	public SamplingResultMessage process(SamplingMessage message) {
		List<BitSet> matches = new ArrayList<>();
		
		// Match all records with their "distance" neighbor w.r.t. the pli of the given "attribute"
		BitSet match = new BitSet(this.plis.length);
		int numComparisons = 0;
		for (int[] cluster : this.plis[message.getAttribute()]) {
			for (int index = 0; index < cluster.length - message.getDistance(); index++) {
				for (int attribute = 0; attribute < this.plis.length; attribute++)
					if (Matching.isMatch(this.records[cluster[index]], this.records[cluster[index + message.getDistance()]], attribute))
						match.set(attribute);
				numComparisons++;
				
				if (this.filter.add(match))
					matches.add(match.clone());
				match.clear();
			}
		}
		final int numMatches = matches.size();
		
		// Convert matches into invalid FDs
//		matches = this.filterSmallMatches(matches);
		matches = this.pruneSubsets(matches);
		final List<FunctionalDependency> invalidFDs = Conversion.matches2FDs(matches, this.plis.length);
		matches = null;
		
		// Send the result to the sender of the sampling message
		final ValidationResultMessage validationResult = Conversion.fds2ValidationResultMessage(invalidFDs);
		final SamplingResultMessage samplingResult = new SamplingResultMessage(validationResult.getInvalidLhss(), validationResult.getInvalidRhss(), numComparisons, numMatches);
		
		return samplingResult;
	}
	
	private List<BitSet> pruneSubsets(List<BitSet> matches) {
		final BitSet[] transposedMatches = new BitSet[this.plis.length];
		for (int attribute = 0; attribute < this.plis.length; attribute++) {
			final BitSet b = new BitSet(matches.size());
			for (int match = 0; match < matches.size(); match++)
				if (matches.get(match).get(attribute))
					b.set(match);
			transposedMatches[attribute] = b;
		}
		
		final List<BitSet> prunedMatches = new ArrayList<>(matches.size());
		final BitSet allSet = new BitSet(matches.size());
		allSet.set(0, matches.size());
		for (int i = 0; i < matches.size(); i++) {
			final BitSet intersection = allSet.clone();
			final BitSet match = matches.get(i);
			for (int trueBit = match.nextSetBit(0); trueBit >= 0; trueBit = match.nextSetBit(trueBit + 1))
				intersection.and(transposedMatches[trueBit]);
			if (intersection.cardinality() == 1)
				prunedMatches.add(match);
		}
//		this.log.info("{} -> {}     {}", matches.size(), prunedMatches.size(), matches.size() - prunedMatches.size());
		return prunedMatches;
	}
	
	/////// Experimental Stuff /////////////
/*	
	private float averageMatchSize = 0;
	private int numMatches = 1;
	private int sumMatchSizes = 1;
	private boolean averageMatchSizeStable = false; 
	
	private List<BitSet> filterSmallMatches(List<BitSet> matches) {
		// Update the average calculation
		if (this.sumMatchSizes < Integer.MAX_VALUE * 0.8f) {
			for (BitSet match : matches) {
				this.numMatches++;
				this.sumMatchSizes += match.cardinality();
			}
			this.averageMatchSize = (float) Math.ceil(this.sumMatchSizes / (float) this.numMatches);
		}
		
		// Filter the matches
		ArrayList<BitSet> filteredMatches = new ArrayList<>(matches.size() / 2);
		for (BitSet match : matches)
			if (match.cardinality() > this.averageMatchSize)
				filteredMatches.add(match);
		return filteredMatches;
	}
	
	private boolean isRelevant(BitSet match) {
		// Returns true if the match is above average large and was (probably) not seen before
		
		// Return false if the match size is below average
		if (match.cardinality() <= this.averageMatchSize)
			return false;
		
		// Return false if the match was seen before
		if (!this.filter.add(match))
			return false;
		
		// Update average if not stable
		if (!this.averageMatchSizeStable) {
			this.numMatches++;
			this.sumMatchSizes += match.cardinality();
			
			// Occasionally recalculate and check for stability
			//if (this.numMatches % 100 == 0)
				this.averageMatchSize = (float) Math.ceil(this.sumMatchSizes / (float) this.numMatches);
				this.log.info("{}", this.averageMatchSize);
		}
		
		return true;
	}
	
	public SamplingResultMessage processTopK(SamplingMessage message) {
		final int k = 100;
		PriorityQueue<BitSet> matchSelection = new PriorityQueue<>(k, new Comparator<BitSet>() {
			@Override
			public int compare(BitSet o1, BitSet o2) {
				return o2.cardinality() - o1.cardinality();
			}
		});
		
		// Find the top k longest (and hence most relevant) matches
		BitSet match = new BitSet(this.plis.length);
		int numComparisons = 0;
		for (int[] cluster : this.plis[message.getAttribute()]) {
			for (int index = 0; index < cluster.length - message.getDistance(); index++) {
				for (int attribute = 0; attribute < this.plis.length; attribute++)
					if (Matching.isMatch(this.records[cluster[index]], this.records[cluster[index + message.getDistance()]], attribute))
						match.set(attribute);
				numComparisons++;
				
				if (matchSelection.size() < k) {
					if (!this.filter.contains(match))
						matchSelection.add(match.clone());
				}
				else {
					if ((matchSelection.peek().cardinality() < match.cardinality()) && !this.filter.contains(match)) {
						matchSelection.poll();
						matchSelection.add(match.clone());
					}
				}
				
				match.clear();
			}
		}
		
		BitSet[] matches = matchSelection.toArray(new BitSet[0]);
		matchSelection = null;
		
		this.filter.addAll(matches);
		final int numMatches = matches.length;
		
		// Convert matches into invalid FDs
		final List<FunctionalDependency> invalidFDs = Conversion.matches2FDs(matches, this.plis.length);
		matches = null;
		
		// Send the result to the sender of the sampling message
		final ValidationResultMessage validationResult = Conversion.fds2ValidationResultMessage(invalidFDs);
		final SamplingResultMessage samplingResult = new SamplingResultMessage(validationResult.getInvalidLhss(), validationResult.getInvalidRhss(), numComparisons, numMatches);
		
		return samplingResult;
	}
*/	
}
