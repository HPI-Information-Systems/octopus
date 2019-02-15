package de.hpi.octopus.actors.masters;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.octopus.actors.DependencySteward;
import de.hpi.octopus.actors.DependencySteward.CandidateRequestMessage;
import de.hpi.octopus.actors.DependencySteward.InvalidFDsMessage;
import de.hpi.octopus.actors.slaves.Validator;
import de.hpi.octopus.actors.slaves.Validator.SamplingMessage;
import de.hpi.octopus.structures.Dataset;
import de.hpi.octopus.structures.DependencyStewardRing;
import de.hpi.octopus.structures.SamplingEfficiency;
import lombok.AllArgsConstructor;
import lombok.Data;

public class Profiler extends AbstractMaster {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "profiler";

	public static Props props() {
		return Props.create(Profiler.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class DiscoveryTaskMessage implements Serializable {
		private static final long serialVersionUID = -8330958742629706627L;
		private DiscoveryTaskMessage() {}
		private int[][][] plis;
		private int numRecords;
		private String[] schema;
	}

	@Data @AllArgsConstructor
	public static class SendPlisMessage implements Serializable {
		private static final long serialVersionUID = -8456522795571418518L;
	}
	
	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class CandidateMessage implements Serializable {
		private static final long serialVersionUID = 8558551115259674228L;
		private CandidateMessage() {}
		private BitSet[] lhss;
		private int rhs;
	}
	
	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class ValidationResultMessage implements Serializable {
		private static final long serialVersionUID = -6823011111281387872L;
		private ValidationResultMessage() {}
		private BitSet[][] invalidLhss;
		private int[] invalidRhss;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class SamplingResultMessage implements Serializable {
		private static final long serialVersionUID = -6823011111281387872L;
		private SamplingResultMessage() {}
		private BitSet[][] invalidLhss;
		private int[] invalidRhss;
		private int comparisons;
		private int matches;
	}
	
	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class FDsUpdatedMessage implements Serializable {
		private static final long serialVersionUID = 1182835629941183917L;
		private FDsUpdatedMessage() {}
		private int rhs;
		private boolean updatePreference;
		private boolean validation;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private Dataset dataset;

	private List<ActorRef> validators = new ArrayList<>();
	private Queue<ActorRef> idleValidators = new LinkedList<>();	// Idle: This validation branch is stopped here and pauses until a dependency steward becomes idle again.
	private Queue<ActorRef> waitingValidators = new LinkedList<>();	// Waiting: This validation branch is collecting candidates and waits for them to come.
	private Map<ActorRef, Object> busyValidators = new HashMap<>();	// Busy: This validation branch is working on a message.
	
	private ActorRef[] dependencyStewards;
	private DependencyStewardRing dependencyStewardRing;
	
	private SamplingEfficiency[] samplingEfficiencies;
	private PriorityQueue<SamplingEfficiency> prioritizedSamplingEfficiencies;

	private Queue<Object> unassignedWork = new LinkedList<>();

	/////////////////////
	// Actor Lifecycle //
	/////////////////////
	
	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(DiscoveryTaskMessage.class, this::handle)
				.match(SendPlisMessage.class, this::handle)
				.match(CandidateMessage.class, this::handle)
				.match(ValidationResultMessage.class, this::handle)
				.match(SamplingResultMessage.class, this::handle)
				.match(FDsUpdatedMessage.class, this::handle)
				.build()
				.orElse(super.createReceive());
	}
	
	@Override
	protected void handle(RegistrationMessage message) {
		super.handle(message);
		
		this.validators.add(this.sender());
		
		this.assign(this.sender());
	}
	
	@Override
	protected void handle(Terminated message) {
		super.handle(message);
		
		this.validators.remove(message.getActor());
		
		this.idleValidators.remove(message.getActor());
		this.waitingValidators.remove(message.getActor());
		Object work = this.busyValidators.remove(message.getActor());
		if (work != null) {
			if (this.idleValidators.isEmpty()) {
				this.unassignedWork.add(work);
				return;
			}
			
			ActorRef validator = this.idleValidators.poll();
			this.busyValidators.put(validator, work);
			validator.tell(work, this.self());
		}
	}

	protected void handle(DiscoveryTaskMessage message) throws Exception {
		// Handle edge cases
		if (this.dataset != null) {
			this.log().error("Can process only one task! Dropping profiling request for {} plis.", message.getPlis().length);
			return;
		}
		if (message.getPlis().length <= 1) {
			this.log().error("Found {} attributes in the input and stopped processing, because at least 2 attributes are needed to make FDs possible.", message.getPlis().length);
			return;
		}
		
		// TODO: we could check the special cases of {}->A for all attributes A here by testing if this.plis[A][0].length == this.numRecords or we keep ignoring all {}->A and consider B->A, C->A, D->A etc. as smallest FDs
		
		// Initialize local dataset with the given task; this also sorts the attributes by the number of their pli clusters
		this.dataset = new Dataset(message, this.log());
		
		// Start one dependency steward for each attribute
		int numAttributes = this.dataset.getNumAtrributes();
		this.dependencyStewards = new ActorRef[numAttributes];
		this.samplingEfficiencies = new SamplingEfficiency[numAttributes];
		this.prioritizedSamplingEfficiencies = new PriorityQueue<SamplingEfficiency>(numAttributes);
		for (int i = 0; i < numAttributes; i++) {
			this.dependencyStewards[i] = this.context().actorOf(
					DependencySteward.props(i, numAttributes, -1), 
					DependencySteward.DEFAULT_NAME + i); // TODO: the maxDepth i.e. max FD size should be a parameter
			
			SamplingEfficiency samplingEfficiency = new SamplingEfficiency(i);
			this.samplingEfficiencies[i] = samplingEfficiency;
			this.prioritizedSamplingEfficiencies.add(samplingEfficiency);
		}
		this.dependencyStewardRing = new DependencyStewardRing(this.dependencyStewards);
		
		// Assign initial work to all validators
		for (ActorRef validator : this.validators)
			this.assign(validator);
	}
	
	protected void handle(ValidationResultMessage validationResultMessage) {
		ActorRef validator = this.sender();
		Validator.ValidationMessage validationMessage = (Validator.ValidationMessage) this.busyValidators.remove(validator);
		
		int validationRequester = validationMessage.getRhs(); // Who asked for this validation
		
		// Forward the validation results to the different dependency stewards
		for (int i = 0; i < validationResultMessage.getInvalidRhss().length; i++) {
			int rhs = validationResultMessage.getInvalidRhss()[i];
			BitSet[] lhss = validationResultMessage.getInvalidLhss()[i];
			
			this.dependencyStewardRing.setBusy(rhs, true);
			if (rhs == validationRequester) {
				double validationEfficiency = (double) (validationMessage.getLhss().length - lhss.length) / (double) validationMessage.getLhss().length;
				this.dependencyStewards[rhs].tell(new InvalidFDsMessage(lhss, true, true, validationEfficiency), this.self());
			}
			else {
				this.dependencyStewards[rhs].tell(new InvalidFDsMessage(lhss, true, false, 0), this.self());
			}
		}
		
		// Assign new work to the validator
		this.assign(validator);
	}
	
	protected void handle(SamplingResultMessage samplingResultMessage) {
		ActorRef validator = this.sender();
		SamplingMessage samplingMessage = (SamplingMessage) this.busyValidators.remove(validator);
		
		// Update the sampling efficiency for the target attribute
		int attribute = samplingMessage.getAttribute();
		int distance = samplingMessage.getDistance();
		int comparisons = samplingResultMessage.getComparisons();
		int matches = samplingResultMessage.getMatches();
		
		SamplingEfficiency samplingEfficiency = this.samplingEfficiencies[attribute];
		this.prioritizedSamplingEfficiencies.remove(samplingEfficiency);
		samplingEfficiency.update(comparisons, matches, distance);
		this.prioritizedSamplingEfficiencies.add(samplingEfficiency);

		// Forward the sampling results to the different dependency stewards
		for (int i = 0; i < samplingResultMessage.getInvalidRhss().length; i++) {
			int rhs = samplingResultMessage.getInvalidRhss()[i];
			BitSet[] lhss = samplingResultMessage.getInvalidLhss()[i];
			this.dependencyStewardRing.setBusy(rhs, true);
			this.dependencyStewards[rhs].tell(new InvalidFDsMessage(lhss, false, true, samplingEfficiency.getEfficiency()), this.self());
		}
		
		// Assign new work to the validator
		this.assign(validator);
	}
	
	protected void handle(FDsUpdatedMessage message) {
		// The dependency steward is now idle
		this.dependencyStewardRing.setBusy(message.getRhs(), false);
		
		// If the dependency steward changed its preferences, we have to update them
		if (message.isUpdatePreference())
			this.dependencyStewardRing.setValidation(message.getRhs(), message.isValidation());
		
		// If idle validators exist, we can now assign work from this idle dependency steward to one of them
		if (!this.idleValidators.isEmpty()) {
			ActorRef validator = this.idleValidators.poll();
			this.assign(validator);
		}
	}
	
	private void handle(SendPlisMessage mesage) {
		this.sender().tell(this.dataset.toPlisMessage(), this.self());
	}
	
	private void handle(CandidateMessage message) {
		// The dependency steward is now idle
		this.dependencyStewardRing.setBusy(message.getRhs(), false);
		
		// Construct the validation message
		Validator.ValidationMessage validationMessage = new Validator.ValidationMessage(message.getLhss(), message.getRhs());
		if (this.waitingValidators.isEmpty()) { // The validator terminated while the dependency steward was collecting candidates
			this.unassignedWork.add(validationMessage);
			return;
		}
		
		// Assign the candidates to some a waiting validator
		ActorRef validator = this.waitingValidators.poll();
		this.busyValidators.put(validator, validationMessage);
		validator.tell(validationMessage, this.self());
		
		// TODO: Implement finalize: An rhs attribute has been fully processed if its dependencySteward returned no new candidates AND no validator is working on candidates from that attribute
	}

	private void assign(ActorRef validator) {
		// Let the validator idle if no discovery task is present yet
		if (this.dataset == null) {
			this.idleValidators.add(validator);
			return;
		}
		
		// Assign work that is waiting for a worker
		Object work = this.unassignedWork.poll();
		if (work != null) {
			validator.tell(work, this.self());
			this.busyValidators.put(validator, work);
			return;
		}
		
		// Assign sampling tasks for attributes that have not yet been used for sampling
		if (!this.prioritizedSamplingEfficiencies.peek().hasStepped()) {
			SamplingEfficiency samplingEfficiency = this.prioritizedSamplingEfficiencies.poll();
			int distance = samplingEfficiency.step();
			this.prioritizedSamplingEfficiencies.add(samplingEfficiency);
			
			SamplingMessage samplingMessage = new SamplingMessage(samplingEfficiency.getAttribute(), distance);
			validator.tell(samplingMessage, this.self());
			this.busyValidators.put(validator, samplingMessage);
			return;
		}
		
		// Try to assign a validation task
		int attribute = this.dependencyStewardRing.nextIdleWithValidationPreference();
		if (attribute >= 0) {
			this.dependencyStewardRing.setBusy(attribute, true);
			this.dependencyStewards[attribute].tell(new CandidateRequestMessage(), this.self());
			
			this.waitingValidators.add(validator);
			return;
		}
		
		// Try to assign a sampling task
		attribute = this.dependencyStewardRing.nextIdleWithSamplingPreference();
		if (attribute >= 0) {
			SamplingEfficiency samplingEfficiency = this.prioritizedSamplingEfficiencies.poll();
			int distance = samplingEfficiency.step();
			this.prioritizedSamplingEfficiencies.add(samplingEfficiency);
			
			this.dependencyStewardRing.setValidation(attribute, true); // We fulfilled the sampling wish and set the preference to validation; otherwise we would sample forever, if the sampling does not discover any further nonFDs (which were needed to change the preference)
			
			SamplingMessage samplingMessage = new SamplingMessage(samplingEfficiency.getAttribute(), distance);
			validator.tell(samplingMessage, this.self());
			this.busyValidators.put(validator, samplingMessage);
			return;
		}
		
		// Let the validator idle if all dependency stewards are busy (i.e., we apply backpressure to not exhaust the profiler and its dependency stewards)
		this.idleValidators.add(validator);
	}
	
	protected void finish() {
	//	DiscoveryTaskMessage discoveryDiscoveryTaskMessage = this.task;
		
		// TODO Re-substitute attribute indexes: for(fd : fds) for(attribute : fd) attribute = sortedIndex2schemaIndex[attribute];
		// TODO: Unsort the attributes into their original order for all discovered FDs
	//	for (fd : fds)
	//		for (attribute : fd)
	//			attribute = this.sortedIndex2schemaIndex[attribute]
		
		// TODO Report FDs
		
		
		this.log().info("Finished discovery task.");
				
		// TODO Tell the Application that the discovery is done
	}
}