package de.hpi.octopus.actors.slaves;

import java.io.Serializable;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.octopus.OctopusMaster;
import de.hpi.octopus.actors.LargeMessageProxy;
import de.hpi.octopus.actors.Reaper;
import de.hpi.octopus.actors.masters.AbstractMaster;
import lombok.AllArgsConstructor;
import lombok.Data;

public abstract class AbstractSlave extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	////////////////////
	// Actor Messages //
	////////////////////

	@Data @AllArgsConstructor
	public static class TerminateMessage implements Serializable {
		private static final long serialVersionUID = 4184578526050265353L;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	protected final Cluster cluster = Cluster.get(this.context().system());

	protected final ActorRef largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
	
	protected Member masterSystem;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////
	
	@Override
	public void preStart() {
		// Subscribe to cluster events in order to register at the cluster's master and terminate if an assigned master is down
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
		
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
				.match(MemberRemoved.class, this::handle)
				.match(TerminateMessage.class, this::handle)
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
		// Ignore, if we already have a master
		if (this.masterSystem != null)
			return;
		
		// Register, if this member is a master
		if (member.hasRole(OctopusMaster.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + this.getMasterName())
				.tell(new AbstractMaster.RegistrationMessage(this.getName()), this.self());
		}
	}
	
	protected void handle(MemberRemoved message) {
		// Terminate, if the removed member is our master
		if (this.masterSystem.equals(message.member()))
			this.self().tell(new TerminateMessage(), ActorRef.noSender());
	}
	
	protected abstract void handle(TerminateMessage message);
	
	protected abstract String getName();
	
	protected abstract String getMasterName();
}