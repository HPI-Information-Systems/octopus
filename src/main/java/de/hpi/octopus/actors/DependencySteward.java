package de.hpi.octopus.actors;

import java.io.Serializable;
import java.util.List;
import java.util.function.Consumer;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import de.hpi.octopus.actors.listeners.ProgressListener;
import de.hpi.octopus.actors.masters.Profiler.CandidateMessage;
import de.hpi.octopus.actors.masters.Profiler.FDsUpdatedMessage;
import de.hpi.octopus.configuration.ConfigurationSingleton;
import de.hpi.octopus.io.FileSink;
import de.hpi.octopus.structures.BitSet;
import de.hpi.octopus.structures.Dataset;
import de.hpi.octopus.structures.FDStore;
import de.hpi.octopus.structures.FDTree;
import de.hpi.octopus.structures.ValidationEfficiency;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class DependencySteward extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "dependencySteward";

	public static Props props(int rhs, int numAttributes) {
		return Props.create(DependencySteward.class, () -> new DependencySteward(rhs, numAttributes));
	}

	public DependencySteward(final int rhs, final int numAttributes) {
		this.rhs = rhs;
		this.fds = new FDTree(numAttributes, rhs);
		
		this.maxDepth = ConfigurationSingleton.get().getMaxLhsSize();
		this.maxCandidatesPerRequest = ConfigurationSingleton.get().getMaxCandidatesPerRequest();
		this.validationThreshold = ConfigurationSingleton.get().getValidationThreshold();
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class InvalidFDsMessage implements Serializable {
		private static final long serialVersionUID = -102767440935270949L;
		private BitSet[] invalidLhss; // The invalid lhss for this rhs
		private int numCandidates; // The number of candidates that have been tested for this rhs; -1 if the invalid FDs were found with candidates from other rhss or if sampling was used, because sampling is for everyone
	}
	
	@Data @NoArgsConstructor
	public static class CandidateRequestMessage implements Serializable {
		private static final long serialVersionUID = -102767540935370948L;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class FinalizeMessage implements Serializable {
		private static final long serialVersionUID = 1958938788143620773L;
		private Dataset dataset;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	private int rhs;
	private FDStore fds;
	
	private final int maxDepth; // TODO: Control with memory guardian
	private final int maxCandidatesPerRequest;
	private final double validationThreshold;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		// Register at this actor system's reaper
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(InvalidFDsMessage.class, message -> this.time(this::handle, message))
				.match(CandidateRequestMessage.class, message -> this.time(this::handle, message))
				.match(FinalizeMessage.class, message -> this.time(this::handle, message))
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private <T> void time(Consumer<T> handle, T message) {
//		long t = System.currentTimeMillis();
		handle.accept(message);
//		this.log().info("Processed {} in {} ms.", message.getClass().getSimpleName(), System.currentTimeMillis() - t);
	}
	
	protected void handle(CandidateRequestMessage message) {
		BitSet[] lhss = this.fds.announceLhss(this.maxCandidatesPerRequest);
		this.sender().tell(new CandidateMessage(lhss, this.rhs), this.self());
	}
	
	protected void handle(InvalidFDsMessage message) {
		final int numAttributes = this.fds.getNumAttributes();
		
		// Prune the candidates from the FDTree and infer new candidates
		for (BitSet invalidLhs : message.getInvalidLhss()) {
			for (BitSet generalLhs : this.fds.getLhsAndGeneralizations(invalidLhs)) {
				BitSet specLhs = generalLhs.clone();
				this.fds.removeLhs(specLhs);
				
				if ((this.maxDepth > 0) && (specLhs.cardinality() >= this.maxDepth))
					continue;
				
				for (int attribute = 0; attribute < numAttributes; attribute++) {
					if (invalidLhs.get(attribute) || (attribute == this.rhs))
						continue;
					
					specLhs.set(attribute);
					if (!this.fds.containsLhsOrGeneralization(specLhs)) {
						this.fds.addLhs(specLhs);
						
						// If dynamic memory management is enabled, frequently check the memory consumption and trim the positive cover if it does not fit anymore
					//	this.memoryGuardian.memoryChanged(1);
					//	this.memoryGuardian.match(this.negCover, this.posCover, invalidLhs); // TODO: Someone needs to supervise the overall memory consumption
					}
					specLhs.clear(attribute);
				}
			}
		}
		
		// Calculate the efficiency
		final double efficiency = ValidationEfficiency.calculateEfficiency(message.getNumCandidates(), message.getInvalidLhss().length);
		
		// Set update preference if the message is a response to your own request
		final boolean updatePreference = message.getNumCandidates() > 0;
		
		// Derive the validation preference from the efficiency
		final boolean validation = efficiency > this.validationThreshold;
		
		// Report the update
		this.sender().tell(new FDsUpdatedMessage(this.rhs, updatePreference, validation, this.fds.hasUnannounceLhss()), this.self());
	}
	
	protected void handle(FinalizeMessage message) {
		// Collect all valid lhss
		BitSet allAttributes = new BitSet(this.fds.getNumAttributes());
		allAttributes.set(0, this.fds.getNumAttributes());
		List<BitSet> allLhss = this.fds.getLhsAndGeneralizations(allAttributes);
		
		// Free the FDTree
		this.fds = null;
		
		// Write all FDs to disk
		FileSink.write(allLhss, this.rhs, message.dataset, this.log());
		
		// Tell the progress listener that this dependency steward is done
		this.context().actorSelection("/user/" + ProgressListener.DEFAULT_NAME).tell(new ProgressListener.FinishedMessage(allLhss.toArray(new BitSet[allLhss.size()]), this.rhs, message.getDataset()), ActorRef.noSender());
		
		// Terminate
		this.self().tell(PoisonPill.getInstance(), this.self());
		
//		// Testing: Is the reported result correct?
//		this.context().actorSelection("/user/" + Validator.DEFAULT_NAME + "1").tell(new Validator.ValidationMessage(allLhss.toArray(new BitSet[0]), this.rhs), this.self());
	}
	
}
