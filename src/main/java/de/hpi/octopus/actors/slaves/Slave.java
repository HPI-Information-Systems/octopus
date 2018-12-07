package de.hpi.octopus.actors.slaves;

import akka.actor.AbstractLoggingActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.octopus.OctopusMaster;
import de.hpi.octopus.actors.masters.Master;

public abstract class Slave extends AbstractLoggingActor {

	////////////////////
	// Actor Messages //
	////////////////////
	
	/////////////////
	// Actor State //
	/////////////////
	
	protected final Cluster cluster = Cluster.get(this.context().system());

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
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	protected void handle(MemberUp message) {
		this.register(message.member());
	}

	protected void register(Member member) {
		if (member.hasRole(OctopusMaster.MASTER_ROLE))
			this.getContext()
				.actorSelection(member.address() + "/user/" + this.getMasterName())
				.tell(new Master.RegistrationMessage(this.getName()), this.self());
	}
	
	protected abstract String getName();
	
	protected abstract String getMasterName();
}