package de.hpi.octopus.actors.masters;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.octopus.actors.DependencySteward;
import de.hpi.octopus.actors.DependencySteward.CandidateRequestMessage;
import de.hpi.octopus.actors.DependencySteward.FinalizeMessage;
import de.hpi.octopus.actors.DependencySteward.InvalidFDsMessage;
import de.hpi.octopus.actors.LargeMessageProxy.LargeMessage;
import de.hpi.octopus.actors.Storekeeper.PlisMessage;
import de.hpi.octopus.actors.listeners.ProgressListener;
import de.hpi.octopus.actors.listeners.ProgressListener.FinishedMessage;
import de.hpi.octopus.actors.slaves.Validator;
import de.hpi.octopus.actors.slaves.Validator.AttributeFinishedMessage;
import de.hpi.octopus.actors.slaves.Validator.SamplingMessage;
import de.hpi.octopus.actors.slaves.AbstractSlave.TerminateMessage;
import de.hpi.octopus.actors.slaves.Validator.ValidationMessage;
import de.hpi.octopus.io.FileSink;
import de.hpi.octopus.structures.BitSet;
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
		private int numCandidates;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class SamplingResultMessage implements Serializable {
		private static final long serialVersionUID = -7967800935003781138L;
		private SamplingResultMessage() {}
		private BitSet[][] invalidLhss;
		private int[] invalidRhss;
		private int numComparisons;
		private int numMatches;
	}
	
	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class FDsUpdatedMessage implements Serializable {
		private static final long serialVersionUID = 1182835629941183917L;
		private FDsUpdatedMessage() {}
		private int rhs;
		private boolean updatePreference;
		private boolean validation;
		private boolean candidatesAvailable;
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
			if (this.children == 0)
				this.terminate();
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
	
	protected void terminate() {
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.tellAllValidators(new TerminateMessage());
		this.log().info("Finished discovery task.");
	}
	
	protected void tellAllValidators(final Object message) {
		for (ActorRef validator : this.busyValidators.keySet())
			validator.tell(message, ActorRef.noSender());
		for (ActorRef validator : this.idleValidators)
			validator.tell(message, ActorRef.noSender());
		for (ActorRef validator : this.waitingValidators)
			validator.tell(message, ActorRef.noSender());
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
		
		// Initialize local dataset with the given task; this also sorts the attributes by the number of their pli clusters
		this.dataset = new Dataset(message, this.log());
		int numAttributes = this.dataset.getNumAtrributes();
		
		// Tell the progress listener to wait for all attributes being finished
		ActorSelection progressListener = this.context().actorSelection("/user/" + ProgressListener.DEFAULT_NAME);
		progressListener.tell(new ProgressListener.StewardsMessage(numAttributes), ActorRef.noSender());
		
		// Start one dependency steward for each attribute (or write the result directly if the rhs attribute is constant)
		this.children = 0;
		this.dependencyStewards = new ActorRef[numAttributes];
		this.dependencyStewardRing = new DependencyStewardRing(numAttributes);
		this.samplingEfficiencies = new SamplingEfficiency[numAttributes];
		this.prioritizedSamplingEfficiencies = new PriorityQueue<SamplingEfficiency>(numAttributes);
		for (int i = 0; i < numAttributes; i++) {
			// Output []->Ai directly and ignore Ai (for candidate generation and sampling), if the attribute is constant
			if ((this.dataset.getPlis()[i].length == 1) && (this.dataset.getPlis()[i][0].length == this.dataset.getNumRecords())) {
				// Output []->Ai
				FileSink.write(new BitSet(0), i, this.dataset, this.log());
				
				// Make the dependency steward permanently busy so that it is never asked for more candidates
				this.dependencyStewardRing.increaseBusy(i);
				
				// Tell the progress listener that this attribute is finished
				BitSet[] lhss = new BitSet[1];
				lhss[0] = new BitSet(0);
				progressListener.tell(new FinishedMessage(lhss, i, this.dataset), this.self());
				
				continue;
			}
			
			// Create the dependency steward
			this.dependencyStewards[i] = this.context().actorOf(
					DependencySteward.props(i, numAttributes), 
					DependencySteward.DEFAULT_NAME + i);
			this.context().watch(this.dependencyStewards[i]);
			this.children++;
			
			// Create the sampling efficiency object
			SamplingEfficiency samplingEfficiency = new SamplingEfficiency(i);
			this.samplingEfficiencies[i] = samplingEfficiency;
			this.prioritizedSamplingEfficiencies.add(samplingEfficiency);
		}
		
		// End here, if there are no dependency stewards that require validation
		if (this.children == 0)
			this.terminate();
		
		// Assign initial work to all validators (if possible); because this.assign adds validators back to the idle list if no work is present, we poll each validator only once.
		int numValidators = this.idleValidators.size();
		for (int i = 0; i < numValidators; i++)
			this.assign(this.idleValidators.poll());
	}

	private void handle(SendPlisMessage mesage) {
		PlisMessage message = this.dataset.toPlisMessage();
		this.largeMessageProxy.tell(new LargeMessage<>(message, this.sender()), this.self());
	}
	
	protected void handle(ValidationResultMessage validationResultMessage) {
		final ActorRef validator = this.sender();
		final ValidationMessage validationMessage = (ValidationMessage) this.busyValidators.remove(validator);

		final int validationRequester = validationMessage.getRhs(); // Who asked for this validation
		final BitSet[][] invalidLhss = validationResultMessage.getInvalidLhss();
		final int[] invalidRhss = validationResultMessage.getInvalidRhss();
		final int numCandidates = validationResultMessage.getNumCandidates();
		
		// Consider the current validator as idle; this is important, because if the profiling is done, we need to notify all validators that they can terminate
		this.idleValidators.add(validator);
		
		// Forward the validation results to the different dependency stewards
		this.forwardInvalidFDs(validationRequester, true, invalidLhss, invalidRhss, numCandidates);
		
		// Check if the validation requester is done and, if true, finish that dependency steward; finish the discovery if done entirely
		this.finishStewardIfDone(validationRequester);
		
		// Assign new work to the validator
		this.assign(this.idleValidators.poll());
	}
	
	protected void handle(SamplingResultMessage samplingResultMessage) {
		final ActorRef validator = this.sender();
		final SamplingMessage samplingMessage = (SamplingMessage) this.busyValidators.remove(validator);

		final int attribute = samplingMessage.getAttribute();
		final int distance = samplingMessage.getDistance();
		final BitSet[][] invalidLhss = samplingResultMessage.getInvalidLhss();
		final int[] invalidRhss = samplingResultMessage.getInvalidRhss();
		final int numComparisons = samplingResultMessage.getNumComparisons();
		final int numMatches = samplingResultMessage.getNumMatches();
		
		// Calculate sampling efficiency
		final double efficiency = SamplingEfficiency.calculateEfficiency(numComparisons, numMatches);
		System.out.println("SE: " + (float) efficiency);
		
		// Update the sampling efficiency for the target attribute
		SamplingEfficiency samplingEfficiency = this.samplingEfficiencies[attribute];
		this.prioritizedSamplingEfficiencies.remove(samplingEfficiency);
		samplingEfficiency.update(efficiency, distance);
		this.prioritizedSamplingEfficiencies.add(samplingEfficiency);
		
		// Forward the sampling results to the different dependency stewards
		this.forwardInvalidFDs(-1, false, invalidLhss, invalidRhss, -1); // Every SamplingResultMessage is marked to trigger an efficiency update (not only the own sampling requests), because the optimal sampling attribute is chosen independently of the dependency steward's attribute anyway
		
		// Update all preferences to validation, because the sampling fulfills all sampling preferences for all dependency stewards whether or not they actually issued this sampling request or got an update from it; it is important to do so, because otherwise attributes might prefer sampling forever if they never get a sampling result
		this.dependencyStewardRing.setValidation(true);
		
		// Assign new work to the validator
		this.assign(validator);
	}
	
	protected void forwardInvalidFDs(final int validationRequester, final boolean validation, final BitSet[][] invalidLhss, final int[] invalidRhss, final int numCandidates) {
		for (int i = 0; i < invalidRhss.length; i++) {
			final int rhs = invalidRhss[i];
			final BitSet[] lhss = invalidLhss[i];

			if (this.dependencyStewards[rhs] == null) // If the steward is already done
				continue;
			
			this.dependencyStewardRing.increaseBusy(rhs);
			
			this.dependencyStewards[rhs].tell(new InvalidFDsMessage(lhss, (validationRequester == rhs) ? numCandidates : -1), this.self());
		}
	}
	
	protected void handle(FDsUpdatedMessage message) {
		// The dependency steward is now one message less busy
		this.dependencyStewardRing.decreaseBusy(message.getRhs());
		
		// The dependency steward may have new candidates now
		this.dependencyStewardRing.setCandidates(message.getRhs(), message.isCandidatesAvailable());
		
		// Check if the dependency steward is done and, if true, finish that dependency steward; finish the discovery if done entirely
		if (!message.isCandidatesAvailable())
			this.finishStewardIfDone(message.getRhs());
		
		// If the dependency steward updated its preference, the profiler has to update it, too
		if (message.isUpdatePreference())
			this.dependencyStewardRing.setValidation(message.getRhs(), message.isValidation());
		
		// If the dependency steward is idle now, we can assign work from this idle dependency steward to idle validators; because sending sampling tasks does not make this steward busy, we assign tasks until no more tasks could be assigned
		if (this.dependencyStewardRing.isIdle(message.getRhs()))
			this.assignIdleValidators();
	}
	
	private void handle(CandidateMessage message) {
		// The dependency steward is now one message less busy
		this.dependencyStewardRing.decreaseBusy(message.getRhs());
		
		// The dependency steward may have no more candidates now
		if (message.getLhss().length == 0) {
			this.dependencyStewardRing.setCandidates(message.getRhs(), false);
		
			// Check if the validation requester is done and, if true, finish that dependency steward; finish the discovery if done entirely
			this.finishStewardIfDone(message.getRhs());
		}
		
		// Get the validator that is waiting for this candidate message
		ActorRef validator = this.waitingValidators.poll();
		
		// Assign the validator to something else if the candidate message did not deliver candidates
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
		
		// If the dependency steward is idle now, we can assign work from this idle dependency steward to idle validators; because sending sampling tasks does not make this steward busy, we assign tasks until no more tasks could be assigned
		if (this.dependencyStewardRing.isIdle(message.getRhs()))
			this.assignIdleValidators();
	}

	private void assignIdleValidators() {
		// Assign idle validators until there are either no more idle validators or we could not assign any further work
		while (!this.idleValidators.isEmpty() && this.assign(this.idleValidators.poll())) {}
	}
	
	private boolean assign(ActorRef validator) {
		// Let the validator idle if no discovery task is present yet
		if (this.dataset == null) {
			this.idleValidators.add(validator);
			return false;
		}
		
		// Assign work that is waiting for a worker
		Object work = this.unassignedWork.poll();
		if (work != null) {
			validator.tell(work, this.self());
			this.busyValidators.put(validator, work);
			return true;
		}
		
		// Assign sampling tasks for attributes that have not yet been used for sampling
		if (!this.prioritizedSamplingEfficiencies.peek().hasStepped()) {
			SamplingEfficiency samplingEfficiency = this.prioritizedSamplingEfficiencies.poll();
			int distance = samplingEfficiency.step();
			this.prioritizedSamplingEfficiencies.add(samplingEfficiency);
			
			SamplingMessage samplingMessage = new SamplingMessage(samplingEfficiency.getAttribute(), distance);
			validator.tell(samplingMessage, this.self());
			this.busyValidators.put(validator, samplingMessage);
			return true;
		}
		
		// Try to assign a validation task
		int attribute = this.dependencyStewardRing.nextIdleWithValidationPreferenceAndCandidates();
		if (attribute >= 0) {
			this.dependencyStewardRing.increaseBusy(attribute);
			this.dependencyStewards[attribute].tell(new CandidateRequestMessage(), this.self());
			
			this.waitingValidators.add(validator);
			return true;
		}
		
		// Try to assign a sampling task
		attribute = this.dependencyStewardRing.nextIdleWithSamplingPreference();
		if (attribute >= 0) {
			SamplingEfficiency samplingEfficiency = this.prioritizedSamplingEfficiencies.poll();
			
			// Do not sample on the same attribute that has asked for the sampling, because the sampling would not find any violation for that rhs attribute (all values in that attribute would be the same during sampling; hence, no violation possible for that rhs)
			if (attribute == samplingEfficiency.getAttribute()) {
				SamplingEfficiency uselessSamplingEfficiency = samplingEfficiency;   // The selected attribute is the most efficient attribute but it is useless for serving this request
				samplingEfficiency = this.prioritizedSamplingEfficiencies.poll();    // The next most efficient sampling attribute is what is needed
				this.prioritizedSamplingEfficiencies.add(uselessSamplingEfficiency); // The useless attribute can be added back to the top of the priority queue
			}
			
			// Set the sampling efficiency of this attribute the 0 (or basically unknown), because we do not know its sampling performance any more and want to take the next attribute for the next, concurrent sampling request; the actual efficiency of this attribute is updates with the sampling result
			int distance = samplingEfficiency.step();
			this.prioritizedSamplingEfficiencies.add(samplingEfficiency);
			
			SamplingMessage samplingMessage = new SamplingMessage(samplingEfficiency.getAttribute(), distance);
			validator.tell(samplingMessage, this.self());
			this.busyValidators.put(validator, samplingMessage);
			return true;
		}
		
		// Let the validator idle if all dependency stewards are busy (i.e., we apply backpressure to not exhaust the profiler and its dependency stewards)
		this.idleValidators.add(validator);
		return false;
	}

	private boolean finishStewardIfDone(int stewardAttribute) {
		// Check if the dependency steward is actually done
		if (!this.isDone(stewardAttribute))
			return false;
		
		// Tell the dependency steward to finalize, i.e., write results and terminate	
		this.dependencyStewards[stewardAttribute].tell(new FinalizeMessage(this.dataset), this.self());

		// Make the dependency steward permanently busy so that it is never asked for more candidates
		this.dependencyStewardRing.increaseBusy(stewardAttribute);
		
		// Remove the dependency steward from the local list so that we do not forward any further results to it
		this.dependencyStewards[stewardAttribute] = null;
		
		// Tell all workers that we can ignore this rhs attribute now, i.e., they do not need to send non-FDs with this rhs
		final AttributeFinishedMessage attributeFinishedMessage = new AttributeFinishedMessage(stewardAttribute);
		this.tellAllValidators(attributeFinishedMessage);
		
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