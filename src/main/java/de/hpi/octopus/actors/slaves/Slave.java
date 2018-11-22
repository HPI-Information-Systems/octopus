package de.hpi.octopus.actors.slaves;

import java.io.Serializable;

import akka.actor.AbstractActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.octopus.OctopusMaster;
import de.hpi.octopus.actors.masters.Master.RegistrationMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

public abstract class Slave extends AbstractActor {

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @AllArgsConstructor
	public static class WorkMessage implements Serializable {
		private static final long serialVersionUID = -1848021041995334016L;
	}

	/////////////////
	// Actor State //
	/////////////////
	
	protected final LoggingAdapter log = Logging.getLogger(this.context().system(), this);
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
				.match(WorkMessage.class, this::handle)
				.matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
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
				.tell(new RegistrationMessage(), this.self());
	}
	
	protected abstract String getMasterName();

	protected abstract void handle(WorkMessage message) throws Exception;
}