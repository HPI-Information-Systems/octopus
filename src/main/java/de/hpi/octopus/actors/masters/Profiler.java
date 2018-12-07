package de.hpi.octopus.actors.masters;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.octopus.actors.slaves.Validator.ValidationMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

public class Profiler extends Master {

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
	}
	
	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class ValidationResultMessage implements Serializable {
		private static final long serialVersionUID = -6823011111281387872L;
		public enum status {MINIMAL, EXTENDABLE, FALSE}
		private ValidationResultMessage() {}
		private status result;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	protected final Queue<ValidationMessage> unassignedWork = new LinkedList<>();
	protected final Queue<ActorRef> idleWorkers = new LinkedList<>();
	protected final Map<ActorRef, ValidationMessage> busyWorkers = new HashMap<>();
	
	protected final List<ActorRef> workers = new ArrayList<>();
	
	protected DiscoveryTaskMessage task;

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
				.match(ValidationResultMessage.class, this::handle)
				.build()
				.orElse(super.createReceive());
	}
	
	@Override
	protected void handle(RegistrationMessage message) {
		super.handle(message);
		
		this.workers.add(this.sender());
		
		this.assign(this.sender());
	}
	
	@Override
	protected void handle(Terminated message) {
		super.handle(message);
		
		this.workers.remove(message.getActor());
		
		if (!this.idleWorkers.remove(message.getActor())) {
			ValidationMessage work = this.busyWorkers.remove(message.getActor());
			if (work != null) {
				this.assign(work);
			}
		}
	}

	protected void handle(DiscoveryTaskMessage message) throws Exception {
		if (this.task != null)
			this.log().error("Can process only one task! Dropping profiling request for {} plis.", message.getPlis().length);
		
		this.task = message;
		
		int i = 0;
		for (int[][] pli : message.plis) {
			this.log().info("Pli {} if of size {}", i++, pli.length);
			
		}
		
		// Transform plis into pliRecords
		int[][] records = new int[message.getNumRecords()][];
		for (int r = 0; r < message.getNumRecords(); r++) {
			records[r] = new int[message.getPlis().length];
			for (int a = 0; a < message.getPlis().length; a++) {
				records[r][a] = -1;
			}
		}
		for (int attr = 0; attr < message.getPlis().length; attr++) {
			int[][] pli = message.getPlis()[attr];
			for (int val = 0; val < pli.length; val++) {
				for (int rec : pli[val]) {
					records[rec][attr] = val;
				}
			}
		}
		this.log().info("Done creating pli records");
		
		
		
	//	for (ValidationMessage work : this.split(message))
	//		this.assign(work);
	}
	
	protected void handle(ValidationResultMessage message) {
		ActorRef worker = this.sender();
		ValidationMessage work = this.busyWorkers.remove(worker);
		this.finish(work, message);
		
		if (this.unassignedWork.isEmpty() && this.busyWorkers.isEmpty()) {
			this.finish();
			this.task = null;
		} else {
			this.assign(worker);
		}
	}
	
	protected void assign(ValidationMessage work) {
		ActorRef worker = this.idleWorkers.poll();
		
		if (worker == null) {
			this.unassignedWork.add(work);
			return;
		}
		
		this.busyWorkers.put(worker, work);
		worker.tell(work, this.self());
	}
	
	protected void assign(ActorRef worker) {
		ValidationMessage work = this.unassignedWork.poll();
		
		if (work == null) {
			this.idleWorkers.add(worker);
			return;
		}
		
		this.busyWorkers.put(worker, work);
		worker.tell(work, this.self());
	}
	
	protected List<ValidationMessage> split(DiscoveryTaskMessage message) throws Exception {
		return Arrays.asList(new ValidationMessage(new int[0], new int[0]));
	}

	protected void finish(ValidationMessage work, ValidationResultMessage result) {
		ValidationMessage validationMessage = (ValidationMessage) work;
		ValidationResultMessage validationResultMessage = (ValidationResultMessage) result;

		this.log().info("Completed: [{},{}]", Arrays.toString(validationMessage.getX()), Arrays.toString(validationMessage.getY()));
		
		switch (validationResultMessage.getResult()) {
			case MINIMAL: 
				this.report(validationMessage);
				break;
			case EXTENDABLE:
				this.split(validationMessage);
				break;
			case FALSE:
				// Ignore
				break;
		}
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

	private void split(ValidationMessage work) {
		int[] x = work.getX();
		int[] y = work.getY();
		
		int next = x.length + y.length;
		
		DiscoveryTaskMessage discoveryDiscoveryTaskMessage = this.task;
		
		if (next < discoveryDiscoveryTaskMessage.getPlis().length - 1) {
			int[] xNew = Arrays.copyOf(x, x.length + 1);
			xNew[x.length] = next;
			this.assign(new ValidationMessage(xNew, y));
			
			int[] yNew = Arrays.copyOf(y, y.length + 1);
			yNew[y.length] = next;
			this.assign(new ValidationMessage(x, yNew));
		}
	}
}