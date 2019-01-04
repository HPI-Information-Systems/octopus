package de.hpi.octopus.actors.masters;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.octopus.actors.DependencySteward;
import de.hpi.octopus.actors.Storekeeper;
import de.hpi.octopus.actors.slaves.Validator.ValidationMessage;
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

	/////////////////
	// Actor State //
	/////////////////

	private int[][][] plis;
	private int numRecords;
	private String[] schema;
	
	private Queue<ValidationMessage> unassignedWork = new LinkedList<>();
	private Queue<ActorRef> idleValidators = new LinkedList<>();
	private Map<ActorRef, ValidationMessage> busyValidators = new HashMap<>();
	
	private List<ActorRef> validators = new ArrayList<>();

	private ActorRef[] dependencyStewards;
	
	private DiscoveryTaskMessage task;

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
		
		if (!this.idleValidators.remove(message.getActor())) {
			ValidationMessage work = this.busyValidators.remove(message.getActor());
			if (work != null) {
				this.assign(work);
			}
		}
	}

	protected void handle(DiscoveryTaskMessage message) throws Exception {
		if (this.plis != null) {
			this.log().error("Can process only one task! Dropping profiling request for {} plis.", message.getPlis().length);
			return;
		}
		
		// Initialize local fields with the given task
		this.plis = message.getPlis();
		this.numRecords = message.getNumRecords();
		this.schema = message.getSchema();
		
		int numAttributes = this.plis.length;
		
		// Sort the plis (i.e. attributes) by their number of clusters: For searching in the covers and for validation, it is good to have attributes with few non-unique values and many clusters left in the prefix tree
		@Data @AllArgsConstructor
		final class Attribute implements Comparable<Attribute> {
			private int schemaIndex;
			private int numClusters;
			@Override
			public int compareTo(Attribute other) {
				return other.getNumClusters() - this.getNumClusters();
			}
		}
		Attribute[] attributes = new Attribute[numAttributes];
		for (int i = 0; i < numAttributes; i++) {
			int numNonUniqueValues = Arrays.stream(this.plis[i]).map(cluster -> cluster.length).reduce((a,b) -> a + b).get().intValue();
			int numStrippedClusters = this.plis[i].length;
			attributes[i] = new Attribute(i, this.numRecords - numNonUniqueValues + numStrippedClusters);
		}
		Arrays.sort(attributes);
		int[][][] sortedPlis = new int[numAttributes][][];
		for (int i = 0; i < numAttributes; i++)
			sortedPlis[i] = this.plis[attributes[i].getSchemaIndex()];
		this.plis = sortedPlis;
		
		for (Attribute attribute : attributes)
			System.out.println(attribute.getSchemaIndex() + "  " + attribute.getNumClusters() + "   " + this.schema[attribute.getSchemaIndex()]);
		
		// Start one dependency steward for each attribute
		this.dependencyStewards = new ActorRef[numAttributes];
		for (int i = 0; i < numAttributes; i++)
			this.dependencyStewards[i] = this.context().actorOf(
					DependencySteward.props(i, numAttributes, -1), 
					DependencySteward.DEFAULT_NAME); // TODO: the maxDepth i.e. max FD size should be a parameter
		
		
		
		
		
		
		
		// Unsort the plis (i.e. attributes) into their original order
		int[] sortedIndex2schemaIndex = new int[this.plis.length];
		for (int i = 0; i < this.plis.length; i++)
			sortedIndex2schemaIndex[i] = attributes[i].getSchemaIndex();
		
		// TODO Re-substitute attribute indexes: for(fd : fds) for(attribute : fd) attribute = sortedIndex2schemaIndex[attribute];
		// TODO Report FDs
	}
	
	protected void handle(ValidationResultMessage message) {
		// Forward the validation results to the different dependency stewards
		for (int i = 0; i < message.getInvalidRhss().length; i++) {
			int rhs = message.getInvalidRhss()[i];
			BitSet[] lhss = message.getInvalidLhss()[i];
			this.dependencyStewards[rhs].tell(new DependencySteward.InvalidFDsMessage(lhss), this.self());
		}
				
		// Find new work for the worker
		ActorRef worker = this.sender();
		ValidationMessage work = this.busyValidators.remove(worker);
		this.idleValidators.add(worker);
		
		
		// TODO
		
		
		
		if (this.unassignedWork.isEmpty() && this.busyValidators.isEmpty()) {
			this.finish();
			this.task = null;
		} else {
			this.assign(worker);
		}
	}
	
	private void handle(SendPlisMessage mesage) {
		this.sender().tell(new Storekeeper.PlisMessage(this.plis, this.numRecords), this.self());
	}
	
	private void handle(CandidateMessage message) {
		// Assign candidates to some free validator
		// Track candidates and re-assign them, if the validator dies
		
		// An rhs attribute has been fully processed if its dependencySteward returned no new candidates AND no validator is working on candidates from that attribute
		
		// TODO	
	}
	
	
	protected void assign(ValidationMessage work) {
		ActorRef worker = this.idleValidators.poll();
		
		if (worker == null) {
			this.unassignedWork.add(work);
			return;
		}
		
		this.busyValidators.put(worker, work);
		worker.tell(work, this.self());
	}
	
	protected void assign(ActorRef worker) {
		ValidationMessage work = this.unassignedWork.poll();
		
		if (work == null) {
			this.idleValidators.add(worker);
			return;
		}
		
		this.busyValidators.put(worker, work);
		worker.tell(work, this.self());
	}
	
	protected List<ValidationMessage> split(DiscoveryTaskMessage message) throws Exception {
		return Arrays.asList(new ValidationMessage(new int[0], new int[0]));
	}

	protected void finish() {
		DiscoveryTaskMessage discoveryDiscoveryTaskMessage = this.task;
		
		this.log().info("Finished discovery task.");
				
		// TODO Tell the Application that the discovery is done
	}
	
	private void report(ValidationMessage work) {
		this.log().info("UCC: {}", Arrays.toString(work.getX()));
		
		// TODO Write somewhere else or tell someone
	}

}