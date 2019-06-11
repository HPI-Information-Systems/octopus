package de.hpi.octopus.actors;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
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
import de.hpi.octopus.structures.BitSet;
import de.hpi.octopus.structures.Dataset;
import de.hpi.octopus.structures.FDStore;
import de.hpi.octopus.structures.FDTree;
import de.hpi.octopus.structures.FunctionalDependency;
import lombok.AllArgsConstructor;
import lombok.Data;

public class DependencySteward extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "dependencySteward";

	public static final int MAX_CANDIDATES_PER_REQUEST = 50;
	
	public static Props props(int rhs, int numAttributes) {
		return Props.create(DependencySteward.class, () -> new DependencySteward(rhs, numAttributes));
	}

	public DependencySteward(final int rhs, final int numAttributes) {
		this.rhs = rhs;
		this.fds = new FDTree(numAttributes, rhs);
		
		this.maxDepth = ConfigurationSingleton.get().getMaxLhsSize();
		this.validationThreshold = ConfigurationSingleton.get().getValidationThreshold();
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class InvalidFDsMessage implements Serializable {
		private static final long serialVersionUID = -102767440935270949L;
		private InvalidFDsMessage() {}
		private BitSet[] invalidLhss;
		private int rhs; // The rhs of the dependency steward that requested the validation that led to this result message; -1 for sampling results, because sampling is for everyone
		private double efficiency;
	}
	
	@Data @AllArgsConstructor
	public static class CandidateRequestMessage implements Serializable {
		private static final long serialVersionUID = -102767540935370948L;
	}
	
	@Data @AllArgsConstructor
	public static class FinalizeMessage implements Serializable {
		private static final long serialVersionUID = 1958938788143620773L;
		private Dataset dataset;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	private int rhs;
	private FDStore fds;
	
	private int maxDepth;
	
	private double validationThreshold;
	
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
				.match(InvalidFDsMessage.class, message -> this.time(this::handle, message, message.getInvalidLhss().length))
				.match(CandidateRequestMessage.class, message -> this.time(this::handle, message, MAX_CANDIDATES_PER_REQUEST))
				.match(FinalizeMessage.class, message -> this.time(this::handle, message, 1))
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private <T> void time(Consumer<T> handle, T message, int size) {
//		long t = System.currentTimeMillis();
		handle.accept(message);
//		this.log().info("Processed {} in {} ms given a message size of {}.", message.getClass().getSimpleName(), System.currentTimeMillis() - t, size);
	}
	
	protected void handle(CandidateRequestMessage message) {
		BitSet[] lhss = this.fds.announceLhss(MAX_CANDIDATES_PER_REQUEST);
		this.sender().tell(new CandidateMessage(lhss, this.rhs), this.self());
	}
	
	protected void handle(InvalidFDsMessage message) {
		final int numAttributes = this.fds.getNumAttributes();
		
		// Prune the candidates from the FDTree and infer new candidates
		for (BitSet invalidLhs : message.getInvalidLhss()) {
			for (BitSet specLhs : this.fds.getLhsAndGeneralizations(invalidLhs)) {
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
		
		// Set update preference if the message is a response to your own request; only validation tasks carry a reference to their sender (and are, therefore, used for updates) - sampling tasks are for everyone
		final boolean updatePreference = message.getRhs() == this.rhs;
		
		// Calculate the validation preference from the validation efficiency if preference needs to be updated
		final boolean validation = (updatePreference) ? message.getEfficiency() > this.validationThreshold : false;
		
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
		Path path = message.getDataset().createOutputPathFor(this.rhs);
		try (BufferedWriter writer = Files.newBufferedWriter(path, Charset.forName("UTF8"))) {
		    for (BitSet lhs : allLhss) {
				writer.write(FunctionalDependency.toString(lhs, this.rhs, message.getDataset()));
			}
		} catch (IOException x) {
		    this.log().error("Failed storing results for rhs attribute " + this.rhs, x.getMessage());
		}
		
		// Tell the progress listener that this dependency steward is done
		this.context().actorSelection("/user/" + ProgressListener.DEFAULT_NAME).tell(new ProgressListener.FinishedMessage(allLhss.toArray(new BitSet[allLhss.size()]), this.rhs, message.getDataset()), ActorRef.noSender());
		
		// Terminate
		this.self().tell(PoisonPill.getInstance(), this.self());
		
//		// Testing: Is the reported result correct?
//		this.context().actorSelection("/user/" + Validator.DEFAULT_NAME + "1").tell(new Validator.ValidationMessage(allLhss.toArray(new BitSet[0]), this.rhs), this.self());
	}
	
}
