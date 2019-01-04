package de.hpi.octopus.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.octopus.OctopusMaster;
import de.hpi.octopus.actors.masters.Profiler;
import de.hpi.octopus.actors.slaves.Validator;
import lombok.AllArgsConstructor;
import lombok.Data;

public class Storekeeper extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "storekeeper";

	public static Props props() {
		return Props.create(Storekeeper.class);
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class PlisMessage implements Serializable {
		private static final long serialVersionUID = 1908085462567854277L;
		private PlisMessage() {}
		private int[][][] plis;
		private int numRecords;
	}

	@Data @AllArgsConstructor
	public static class SendDataMessage implements Serializable {
		private static final long serialVersionUID = 543626437035529604L;
	}

	/////////////////
	// Actor State //
	/////////////////

	private final Cluster cluster = Cluster.get(this.context().system());
	private ActorSelection profiler;
	
	private int[][][] plis;
	private int[][] records;

	private final List<ActorRef> waitingValidators = new ArrayList<>();
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		this.cluster.subscribe(this.self(), MemberUp.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(PlisMessage.class, this::handle)
				.match(SendDataMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.findProfiler(member);
		});
	}

	protected void handle(MemberUp message) {
		this.findProfiler(message.member());
	}

	protected void findProfiler(Member member) {
		if (member.hasRole(OctopusMaster.MASTER_ROLE))
			this.profiler = this.getContext().actorSelection(member.address() + "/user/" + Profiler.DEFAULT_NAME);
	}
	
	private void handle(PlisMessage message) {
		// Store plis
		this.plis = message.getPlis();
		
		// Generate and store pli-records
		this.records = new int[message.getNumRecords()][];
		for (int r = 0; r < message.getNumRecords(); r++) {
			this.records[r] = new int[message.getPlis().length];
			for (int a = 0; a < message.getPlis().length; a++) {
				this.records[r][a] = -1;
			}
		}
		for (int attr = 0; attr < message.getPlis().length; attr++) {
			int[][] pli = message.getPlis()[attr];
			for (int val = 0; val < pli.length; val++) {
				for (int rec : pli[val]) {
					this.records[rec][attr] = val;
				}
			}
		}
		this.log().info("Done creating pli records");
		
		// Send the data to all validators waiting for it
		for (ActorRef validator : this.waitingValidators)
			validator.tell(new Validator.DataMessage(this.plis, this.records), this.self());
		this.waitingValidators.clear();
	}
	
	private void handle(SendDataMessage message) {
		// If the data is already present, send the data
		if (this.plis != null) {
			this.sender().tell(new Validator.DataMessage(this.plis, this.records), this.self());
			return;
		}
		
		// If the data has not yet been requested, send data request
		if (this.waitingValidators.isEmpty()) {
			this.profiler.tell(new Profiler.SendPlisMessage(), this.self());
		}
		
		// Put the sender of the current request to the waiting list
		this.waitingValidators.add(this.sender());
	}
}
