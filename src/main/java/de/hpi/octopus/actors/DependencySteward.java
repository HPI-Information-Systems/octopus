package de.hpi.octopus.actors;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.BitSet;
import java.util.List;
import java.util.function.Consumer;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import de.hpi.octopus.actors.listeners.ProgressListener;
import de.hpi.octopus.actors.masters.Profiler.CandidateMessage;
import de.hpi.octopus.actors.masters.Profiler.FDsUpdatedMessage;
import de.hpi.octopus.structures.Dataset;
import de.hpi.octopus.structures.FDStore;
import de.hpi.octopus.structures.FDTree;
import lombok.AllArgsConstructor;
import lombok.Data;

public class DependencySteward extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "dependencySteward";

	public static final int MAX_CANDIDATES_PER_REQUEST = 50;
	
	public static Props props(int rhs, int numAttributes, int maxDepth) {
		return Props.create(DependencySteward.class, () -> new DependencySteward(rhs, numAttributes, maxDepth));
	}

	public DependencySteward(int rhs, int numAttributes, int maxDepth) {
		this.rhs = rhs;
		this.fds = new FDTree(numAttributes, rhs);
		
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
		private boolean validation;
		private boolean own;
		private double efficiency;
	}
	
	@Data @AllArgsConstructor
	public static class CandidateRequestMessage implements Serializable {
		private static final long serialVersionUID = -102767540935370948L;
	}
	
	@Data @AllArgsConstructor
	public static class FinalizeMessage implements Serializable {
		private static final long serialVersionUID = 1958938788143620773L;
		private String outputPath;
		private Dataset dataset;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	private int rhs;
	private FDStore fds;
	
	private int maxDepth;
	
	private double validationCalculationEfficiency = 0;
	private double validationUpdateEfficiency = 0;
	private double samplingCalculationEfficiency = 0;
	private double samplingUpdateEfficiency = 0;
	
	private double validationThreshold = 0.8;
	private double samplingThreshold = 0.01;
	
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
		long t = System.currentTimeMillis();
		handle.accept(message);
//		this.log().info("Processed {} in {} ms given a message size of {}.", message.getClass().getSimpleName(), System.currentTimeMillis() - t, size);
	}
	
	protected void handle(CandidateRequestMessage message) {
		BitSet[] lhss = this.fds.announceLhss(MAX_CANDIDATES_PER_REQUEST);
		this.sender().tell(new CandidateMessage(lhss, this.rhs), this.self());
	}
	
	protected void handle(InvalidFDsMessage message) {
		int numAttributes = this.fds.getNumAttributes();
		int numUpdates = 0;
		
		// Prune the candidates from the FDTree and infer new candidates
		for (BitSet invalidLhs : message.getInvalidLhss()) {
			for (BitSet specLhs : this.fds.getLhsAndGeneralizations(invalidLhs)) {
				this.fds.removeLhs(specLhs);
				numUpdates++;
				
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
		
		// Update efficiencies and preference if necessary
		if (message.isOwn()) {
			if (message.isValidation()) { // TODO: Nochmal überdenken wie die Effizienz bewertet wird...
				this.validationCalculationEfficiency = message.getEfficiency();
				this.validationUpdateEfficiency = (double) numUpdates / (double) message.getInvalidLhss().length;
			}
			else {
				this.samplingCalculationEfficiency = message.getEfficiency();
				this.samplingUpdateEfficiency = (double) numUpdates / (double) message.getInvalidLhss().length;
			}
			boolean validation = this.calculatePreference();
			this.sender().tell(new FDsUpdatedMessage(this.rhs, true, validation), this.self());
		}
		else {
			this.sender().tell(new FDsUpdatedMessage(this.rhs, false, false), this.self());
		}
	}
	
	protected boolean calculatePreference() {
		//this.validationCalculationEfficiency	// validFDs / validations
		//this.validationUpdateEfficiency		// update / invalidFDs 			Usually 1 if no intermediate updates happened
		//this.samplingCalculationEfficiency	// matches / comparisons
		//this.samplingUpdateEfficiency			// updates / matches
		
		double validationEfficiency = this.validationCalculationEfficiency; // We ignore the update efficiency, because we cannot know the causes of the extra updates
		double samplingEfficiency = this.samplingCalculationEfficiency * this.samplingUpdateEfficiency;
		
		if (validationEfficiency > this.validationThreshold)
			return true;
		
	//	if (samplingEfficiency > this.samplingThreshold) // Easy sampling idea: If more than 10% candidates are invalid, ask for a sampling round; the profiler will switch the preference back to validation after that preference was served anyway; we then see if we are back into <10% non-FDs
			return false;
	}

	protected void handle(FinalizeMessage message) {
		// Collect all valid lhss
		BitSet allAttributes = new BitSet(this.fds.getNumAttributes());
		allAttributes.set(0, this.fds.getNumAttributes());
		List<BitSet> allLhss = this.fds.getLhsAndGeneralizations(allAttributes);
		
		// Free the FDTree
		this.fds = null;
		
		// Write all FDs to disk
		String pathString = message.getOutputPath() + File.separatorChar + message.getDataset().getRelationName();
		String fileString = pathString + File.separatorChar + message.getDataset().getColumnNames()[this.rhs] + ".txt";
		
		File path = new File(pathString);
		if (!path.exists())
			path.mkdirs();
		
		File file = new File(fileString);
		if (file.exists())
			file.delete();
		
		try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileString), Charset.forName("UTF8"))) {
		    for (BitSet lhs : allLhss) {
				StringBuffer buffer = new StringBuffer("[");
				for (int attribute = lhs.nextSetBit(0); attribute >= 0; attribute = lhs.nextSetBit(attribute + 1)) {
					buffer.append(message.getDataset().getColumnNames()[attribute]);
					buffer.append(", ");
				}
				buffer.delete(buffer.length() - 2 , buffer.length());
				buffer.append("] --> ");
				buffer.append(message.getDataset().getColumnNames()[this.rhs]);
				buffer.append("\r\n");

				writer.write(buffer.toString());
			}
		} catch (IOException x) {
		    this.log().error(x, x.getMessage());
		}
		
		// Tell the progress listener that this dependency steward is done
		this.context().actorSelection("/user/" + ProgressListener.DEFAULT_NAME).tell(new ProgressListener.FinishedMessage(allLhss.size(), allLhss.toArray(new BitSet[allLhss.size()]), this.rhs, message.getDataset()), ActorRef.noSender());
		
		// Terminate
		this.self().tell(PoisonPill.getInstance(), this.self());
		
//		// Testing: Is the reported result correct?
//		this.context().actorSelection("/user/" + Validator.DEFAULT_NAME + "1").tell(new Validator.ValidationMessage(allLhss.toArray(new BitSet[0]), this.rhs), this.self());
	}
	
}
