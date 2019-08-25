package de.hpi.octopus.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.octopus.actors.FilterManipulator.AddAllMessage;
import de.hpi.octopus.actors.slaves.Worker.DetailedSamplingResultMessage;
import de.hpi.octopus.actors.slaves.Worker.SamplingMessage;
import de.hpi.octopus.logic.ConversionLogic;
import de.hpi.octopus.logic.MatchingLogic;
import de.hpi.octopus.structures.BitSet;
import de.hpi.octopus.structures.BloomFilter;
import de.hpi.octopus.structures.FunctionalDependency;
import lombok.AllArgsConstructor;
import lombok.Data;

public class Sampler extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "sampler";

	public static Props props(final int[][] records, final int[][][] plis, final BloomFilter filter, final ActorRef filterManipulator) {
		return Props.create(Sampler.class, () -> new Sampler(records, plis, filter, filterManipulator));
	}

	public Sampler(final int[][] records, final int[][][] plis, final BloomFilter filter, final ActorRef filterManipulator) {
		this.records = records;
		this.plis = plis;
		this.filter = filter;
		this.filterManipulator = filterManipulator;
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class DetailedSamplingMessage implements Serializable {
		private static final long serialVersionUID = 6609781303993431757L;
		private DetailedSamplingMessage() {}
		public DetailedSamplingMessage(final SamplingMessage message, final boolean[] finishedRhsAttributes) {
			this.attribute = message.getAttribute();
			this.distance = message.getDistance();
			this.finishedRhsAttributes = finishedRhsAttributes;
		}
		private int attribute;
		private int distance;
		private boolean[] finishedRhsAttributes;
	}

	/////////////////
	// Actor State //
	/////////////////

	private final int[][] records;
	private final int[][][] plis;
	private volatile BloomFilter filter;
	private final ActorRef filterManipulator;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(DetailedSamplingMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(DetailedSamplingMessage message) {
		// Reset the filter reference to make its elements effectively volatile (to see changes possibly made in other cache hierarchies)
		this.filter = this.filter;
		
		//List<BitSet> matches = new ArrayList<>(); // With a list, we need to deduplicate after collecting the matches and before removing subsets
		Set<BitSet> matches = new HashSet<>();
		
		// Match all records with their "distance" neighbor w.r.t. the pli of the given "attribute"
		BitSet match = new BitSet(this.plis.length);
		int numComparisons = 0;
		for (int[] cluster : this.plis[message.getAttribute()]) {
			for (int index = 0; index < cluster.length - message.getDistance(); index++) {
				for (int attribute = 0; attribute < this.plis.length; attribute++)
					if (MatchingLogic.isMatch(this.records[cluster[index]], this.records[cluster[index + message.getDistance()]], attribute))
						match.set(attribute);
				numComparisons++;
				
				if (!this.filter.contains(match)) // A direct "add()" here would require a synchronize method that breaks the actor model, but its faster...
					matches.add(match.clone());
				match.clear();
			}
		}
		
		List<BitSet> prunedMatches = new ArrayList<BitSet>(matches);
		
		this.filterManipulator.tell(new AddAllMessage(prunedMatches), this.self());
		
//		prunedMatches = this.filterSmallMatches(matches);
		prunedMatches = this.pruneSubsets(prunedMatches);
		
		// Convert matches into invalid FDs
		final List<FunctionalDependency> invalidFDs = ConversionLogic.matches2FDs(prunedMatches, this.plis.length, message.getFinishedRhsAttributes());
		
		int numMatches = matches.size();
		matches = null;
		
		// Send the result to the sender of the sampling message
		this.sender().tell(new DetailedSamplingResultMessage(invalidFDs, numComparisons, numMatches), this.self());
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

	/////// Experimental Stuff; still from SamplingLogic /////////////
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
