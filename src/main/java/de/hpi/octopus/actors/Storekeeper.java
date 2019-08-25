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
import de.hpi.octopus.actors.masters.Profiler.SendPlisMessage;
import de.hpi.octopus.actors.slaves.Worker.DataMessage;
import de.hpi.octopus.structures.BloomFilter;
import de.hpi.octopus.structures.Dataset;
import de.hpi.octopus.structures.PliCache;
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

	@Data @AllArgsConstructor
	public static class SendDataMessage implements Serializable {
		private static final long serialVersionUID = 543626437035529604L;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class PlisMessage implements Serializable {
		private static final long serialVersionUID = 1908085462567854277L;
		private PlisMessage() {}
		private int[][][] plis;
		private int numRecords;
	}

	/////////////////
	// Actor State //
	/////////////////

	private final Cluster cluster = Cluster.get(this.context().system());

	@SuppressWarnings("unused") // Needed to receive large messages
	private final ActorRef largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
	
	private ActorSelection profiler;
	
	private Dataset dataset;
	private PliCache pliCache;
	private BloomFilter filter;
	private ActorRef filterManipulator;

	private final List<ActorRef> waitingValidators = new ArrayList<>();
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		// Subscribe to cluster events in order to find the cluster's master
		this.cluster.subscribe(this.self(), MemberUp.class);
		
		// Register at this actor system's reaper
		Reaper.watchWithDefaultReaper(this);
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
				.match(SendDataMessage.class, this::handle)
				.match(PlisMessage.class, this::handle)
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
	
	private void handle(SendDataMessage message) {
		// If the data is already present, send the data
		if (this.dataset != null) {
			final DataMessage dataMessage = new DataMessage(this.dataset.getPlis(), this.dataset.getRecords(), this.pliCache, this.filter, this.filterManipulator);
			this.sender().tell(dataMessage, this.self());
			return;
		}
		
		// If the data has not yet been requested, send data request
		if (this.waitingValidators.isEmpty())
			this.profiler.tell(new SendPlisMessage(), this.self());
		
		// Put the sender of the current request to the waiting list
		this.waitingValidators.add(this.sender());
	}

	private void handle(PlisMessage message) {
		// Store plis; this also generates and stores the pli-records
		this.dataset = new Dataset(message, this.log());
		
		// Create a pliCache for this dataset
		this.pliCache = new PliCache(message.getPlis().length);
		
		// Create a filter for this dataset
		this.filter = new BloomFilter();
		this.filterManipulator = this.context().actorOf(FilterManipulator.props(this.filter), FilterManipulator.DEFAULT_NAME);
		
		// Create the data message
		final DataMessage dataMessage = new DataMessage(this.dataset.getPlis(), this.dataset.getRecords(), this.pliCache, this.filter, this.filterManipulator);
		
		// Send the plis and pli-records to all validators waiting for it
		for (ActorRef validator : this.waitingValidators)
			validator.tell(dataMessage, this.self());
		this.waitingValidators.clear();
		
		// Write dataset to disk for debugging
//		this.dataset.writeToDisk("dataset");
	}
}
