package de.hpi.octopus.actors.masters;

import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.octopus.actors.DependencySteward;
import de.hpi.octopus.actors.DependencySteward.CandidateRequestMessage;
import de.hpi.octopus.actors.DependencySteward.FinalizeMessage;
import de.hpi.octopus.actors.DependencySteward.InvalidFDsMessage;
import de.hpi.octopus.actors.listeners.ProgressListener;
import de.hpi.octopus.actors.slaves.Validator;
import de.hpi.octopus.actors.slaves.Validator.SamplingMessage;
import de.hpi.octopus.actors.slaves.Validator.TerminateMessage;
import de.hpi.octopus.actors.slaves.Validator.ValidationMessage;
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
		private String relationName;
		private String[] columnNames;
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

	private Queue<ActorRef> idleValidators = new LinkedList<>();	// Idle: This validation branch is stopped here and pauses until a dependency steward becomes idle again.
	private Queue<ActorRef> waitingValidators = new LinkedList<>();	// Waiting: This validation branch is collecting candidates and waits for them to come.
	private Map<ActorRef, Object> busyValidators = new HashMap<>();	// Busy: This validation branch is working on a message.
	
	private ActorRef[] dependencyStewards;							// List of non-finished dependency stewards
	private DependencyStewardRing dependencyStewardRing;			// Ring of dependency stewards to find the next steward serving new validation tasks
	
	private int children = 0;
	
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
		
		this.assign(this.sender());
	}
	
	@Override
	protected void handle(Terminated message) {
		super.handle(message);
		
		String actorName = message.getActor().path().name();
		
		// If a dependency steward terminates, it is done and we have one child less
		if (actorName.contains(DependencySteward.DEFAULT_NAME)) {
			this.children--;
			
			// Terminate the profiling if all children, i.e., all dependency stewards are done
			if (this.children == 0) {
				this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
				for (ActorRef validator : this.busyValidators.keySet())
					validator.tell(new TerminateMessage(), ActorRef.noSender());
				for (ActorRef validator : this.idleValidators)
					validator.tell(new TerminateMessage(), ActorRef.noSender());
				for (ActorRef validator : this.waitingValidators)
					validator.tell(new TerminateMessage(), ActorRef.noSender());
				
				this.log().info("Finished discovery task.");
			}
		}
		
		// If a validator terminates, we forget him and might need to reschedule his work
		if (actorName.contains(Validator.DEFAULT_NAME)) {
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
		this.children = numAttributes;
		this.dependencyStewards = new ActorRef[numAttributes];
		this.samplingEfficiencies = new SamplingEfficiency[numAttributes];
		this.prioritizedSamplingEfficiencies = new PriorityQueue<SamplingEfficiency>(numAttributes);
		for (int i = 0; i < numAttributes; i++) {
			this.dependencyStewards[i] = this.context().actorOf(
					DependencySteward.props(i, numAttributes, -1), 
					DependencySteward.DEFAULT_NAME + i); // TODO: the maxDepth i.e. max FD size should be a parameter
			this.context().watch(this.dependencyStewards[i]);
			
			SamplingEfficiency samplingEfficiency = new SamplingEfficiency(i);
			this.samplingEfficiencies[i] = samplingEfficiency;
			this.prioritizedSamplingEfficiencies.add(samplingEfficiency);
		}
		this.dependencyStewardRing = new DependencyStewardRing(this.dependencyStewards.length);
		
		// Tell the progress listener to wait for the dependency stewards
		this.context().actorSelection("/user/" + ProgressListener.DEFAULT_NAME).tell(new ProgressListener.StewardsMessage(numAttributes), ActorRef.noSender());
		
		// Assign initial work to all validators
		while (!this.idleValidators.isEmpty())
			this.assign(this.idleValidators.poll());
	}

	private void handle(SendPlisMessage mesage) {
		this.sender().tell(this.dataset.toPlisMessage(), this.self());
	}
	
	protected void handle(ValidationResultMessage validationResultMessage) {
		ActorRef validator = this.sender();
		ValidationMessage validationMessage = (ValidationMessage) this.busyValidators.remove(validator);
		
		// Consider the current validator as idle; this is important, because if the profiling is done, we need to notify all validators that they can terminate
		this.idleValidators.add(validator);
		
		int validationRequester = validationMessage.getRhs(); // Who asked for this validation
		
		// Forward the validation results to the different dependency stewards
		for (int i = 0; i < validationResultMessage.getInvalidRhss().length; i++) {
			int rhs = validationResultMessage.getInvalidRhss()[i];
			BitSet[] lhss = validationResultMessage.getInvalidLhss()[i];

			if (this.dependencyStewards[rhs] == null) // If the steward is already done
				continue;
			
			this.dependencyStewardRing.increaseBusy(rhs);
			
			// Update the validation efficiency only for the dependency steward that actually requested this validation
			if (rhs == validationRequester) {
				double validationEfficiency = (double) (validationMessage.getLhss().length - lhss.length) / (double) validationMessage.getLhss().length;
				this.dependencyStewards[rhs].tell(new InvalidFDsMessage(lhss, true, true, validationEfficiency), this.self());
			}
			else {
				this.dependencyStewards[rhs].tell(new InvalidFDsMessage(lhss, true, false, 0), this.self());
			}
		}
		
		// Check if the validation requester is done and, if true, finish that dependency steward; finish the discovery if done entirely
		this.finishStewardIfDone(validationRequester);
		
		// Assign new work to the validator
		this.assign(this.idleValidators.poll());
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
			
			if (this.dependencyStewards[rhs] == null) // If the steward is already done
				continue;
			
			this.dependencyStewardRing.increaseBusy(rhs);
			this.dependencyStewards[rhs].tell(new InvalidFDsMessage(lhss, false, true, samplingEfficiency.getEfficiency()), this.self()); // Any SamplingResultMessage is marked to trigger an efficiency update (not only the own sampling requests), because the optimal sampling attribute is chosen independently of the dependency steward's attribute anyway
		}
		
		// The sampling fulfills all sampling preferences for all dependency stewards whether or not they actually issued this sampling request or got an update from it
		this.dependencyStewardRing.setValidation(true);
		
		// Assign new work to the validator
		this.assign(validator);
	}
	
	protected void handle(FDsUpdatedMessage message) {
		// The dependency steward is now one message less busy
		this.dependencyStewardRing.decreaseBusy(message.getRhs());
		
		// The dependency steward may have new candidates now
		this.dependencyStewardRing.setCandidates(message.getRhs(), true);
		
		// If the dependency steward updated its preference, the profiler has to update it, too
		if (message.isUpdatePreference())
			this.dependencyStewardRing.setValidation(message.getRhs(), message.isValidation());
		
		// If the dependency steward is idle now and idle validators exist, we can assign work from this idle dependency steward to one of them; because sending sampling tasks does not make this steward busy, we assign tasks until it gets idle
		while (this.dependencyStewardRing.isIdle(message.getRhs()) && !this.idleValidators.isEmpty())
			this.assign(this.idleValidators.poll());
	}
	
	private void handle(CandidateMessage message) {
		// The dependency steward is now one message less busy
		this.dependencyStewardRing.decreaseBusy(message.getRhs());
		
		// The dependency steward may have no more candidates now
		if (message.getLhss().length == 0)
			this.dependencyStewardRing.setCandidates(message.getRhs(), false);
		
		// Check if the validation requester is done and, if true, finish that dependency steward; finish the discovery if done entirely
		this.finishStewardIfDone(message.getRhs());
		
		// Get the validator that is waiting for this candidate message
		ActorRef validator = this.waitingValidators.poll();
		
		// Assign the validator to somthing else if the candidate message did not deliver candidates
		if ((message.getLhss().length == 0) && (validator != null)) {
			this.assign(validator);
			return;
		}
		
		// Create the validation message
		ValidationMessage validationMessage = new ValidationMessage(message.getLhss(), message.getRhs());
		
		// If the validator terminated while the dependency steward was collecting candidates
		if (validator == null) {
			// Hold the validation message
			this.unassignedWork.add(validationMessage);
		}
		else {
			// Assign the candidates to the waiting validator
			this.busyValidators.put(validator, validationMessage);
			validator.tell(validationMessage, this.self());
		}
		
		// If the dependency steward is idle now and idle validators exist, we can assign work from this idle dependency steward to one of them; because sending sampling tasks does not make this steward busy, we assign tasks until it gets idle
		while (this.dependencyStewardRing.isIdle(message.getRhs()) && !this.idleValidators.isEmpty())
			this.assign(this.idleValidators.poll());
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
		int attribute = this.dependencyStewardRing.nextIdleWithValidationPreferenceAndCandidates();
		if (attribute >= 0) {
			this.dependencyStewardRing.increaseBusy(attribute);
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

	private boolean finishStewardIfDone(int stewardAttribute) {
		// Check if the dependency steward is actually done
		if (!this.isDone(stewardAttribute))
			return false;
		
		// Tell the dependency steward to finalize, i.e., write results and terminate	
		this.dependencyStewards[stewardAttribute].tell(new FinalizeMessage("results", this.dataset), this.self());

		// Make the dependency steward permanently busy so that it is never asked for more candidates
		this.dependencyStewardRing.increaseBusy(stewardAttribute);
		
		// Remove the dependency steward from the local list so that we do not forward any further results to it
		this.dependencyStewards[stewardAttribute] = null;
		
/*		System.out.println("Done " + stewardAttribute);
		for (int i = 0; i < this.dataset.getNumAtrributes(); i++)
			if (this.dependencyStewards[i] == null)
				System.out.print(1);
			else
				System.out.print(0);
		System.out.println(" done");
		for (int i = 0; i < this.dataset.getNumAtrributes(); i++)
			if (this.dependencyStewardRing.isCandidates(i))
				System.out.print(1);
			else
				System.out.print(0);
		System.out.println(" candidates");
		for (int i = 0; i < this.dataset.getNumAtrributes(); i++)
			if (this.dependencyStewardRing.isBusy(i))
				System.out.print(1);
			else
				System.out.print(0);
		System.out.println(" busy");
		for (int i = 0; i < this.dataset.getNumAtrributes(); i++)
			if (this.dependencyStewardRing.isValidation(i))
				System.out.print(1);
			else
				System.out.print(0);
		System.out.println(" validation");
*/		
		return true;
	}
	
	private boolean isDone(int attribute) {
		// A dependency steward that is done will eventually send an empty candidate message, because ...
		// - it will be idle at some point (= all messages are processed)
		//   - the profiler will at some point ask for candidates, because the sampling-validation strategy always comes back to validation once a sampling wish has been fulfilled
		
		// A dependency steward is done if the following three conditions are fulfilled:
		// (1) it has no more candidates, i.e., it has sent an empty candidate message (= no candidates need validation)
		// (2) it has no work in progress, i.e., is idle (= no new results are on their way and may create new candidates)
		// (3) it has no validation in progress (= no candidates are being validated and could potentially be non-FDs)
		
		// Check if the dependency steward has no more candidates
		if (this.dependencyStewardRing.isCandidates(attribute))
			return false;
		
		// Check if the dependency steward is idle
		if (this.dependencyStewardRing.isBusy(attribute))
			return false;
		
		// Check if any validator is validating candidates from the dependency steward
		for (Object task : this.busyValidators.values())
			if ((task instanceof ValidationMessage) && ((ValidationMessage) task).getRhs() == attribute)
				return false;
		
		return true;
	}
}