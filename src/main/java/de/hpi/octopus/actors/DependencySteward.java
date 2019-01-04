package de.hpi.octopus.actors;

import java.io.Serializable;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import de.hpi.octopus.actors.masters.Profiler;
import de.hpi.octopus.structures.FDTree;
import lombok.AllArgsConstructor;
import lombok.Data;

public class DependencySteward extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "dependencySteward";

	public static final int MAX_CANDIDATES_PER_REQUEST = 100;
	
	public static Props props(int rhs, int numAttributes, int maxDepth) {
		return Props.create(DependencySteward.class, () -> new DependencySteward(rhs, numAttributes, maxDepth));
	}

	public DependencySteward(int rhs, int numAttributes, int maxDepth) {
		this.rhs = rhs;
		this.fds = new FDTree(numAttributes);
		
		this.maxDepth = maxDepth;
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class InvalidFDsMessage implements Serializable {
		private static final long serialVersionUID = -102767440935270949L;
		private InvalidFDsMessage() {}
		private BitSet[] invalidLhss;
	}
	
	@Data @AllArgsConstructor
	public static class CandidateRequestMessage implements Serializable {
		private static final long serialVersionUID = -102767540935370948L;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private final int rhs;
	private final FDTree fds;
	
	private int maxDepth;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(InvalidFDsMessage.class, this::handle)
				.match(CandidateRequestMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(CandidateRequestMessage message) {
		BitSet[] lhss = this.fds.announceLhss(MAX_CANDIDATES_PER_REQUEST);
		this.sender().tell(new Profiler.CandidateMessage(lhss, this.rhs), this.self());
	}
	
	protected void handle(InvalidFDsMessage message) {
		int numAttributes = this.fds.getNumAttributes();
		int changes = 0;
		
		// Prune the candidate from the FDTree and infer new candidates
		for (BitSet invalidLhs : message.getInvalidLhss()) {
			for (BitSet specLhs : this.fds.getLhsAndGeneralizations(invalidLhs)) {
				this.fds.removeLhs(specLhs);
				
				if ((this.maxDepth > 0) && (specLhs.cardinality() >= this.maxDepth))
					continue;
				
				for (int attribute = numAttributes - 1; attribute >= 0; attribute--) { // TODO: Is iterating backwards a good or bad idea?
					if (invalidLhs.get(attribute) || (attribute == this.rhs))
						continue;
					
					specLhs.set(attribute);
					if (!this.fds.containsLhsOrGeneralization(specLhs)) {
						this.fds.addLhs(specLhs);
						changes++;
						
						// If dynamic memory management is enabled, frequently check the memory consumption and trim the positive cover if it does not fit anymore
						this.memoryGuardian.memoryChanged(1);
						this.memoryGuardian.match(this.negCover, this.posCover, invalidLhs);
					}
					specLhs.clear(attribute);
				}
			}
		}
		
		return changes;
	}
}
