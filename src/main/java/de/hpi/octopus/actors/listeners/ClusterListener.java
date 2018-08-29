package de.hpi.octopus.actors.listeners;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberEvent;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.UnreachableMember;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class ClusterListener extends AbstractActor {
	
	public static final String DEFAULT_NAME = "clusterListener";
	
	LoggingAdapter log = Logging.getLogger(getContext().system(), this);
	Cluster cluster = Cluster.get(getContext().system());

	public static Props props() {
		return Props.create(ClusterListener.class);
	}
	
	@Override
	public void preStart() {
		this.cluster.subscribe(self(), MemberEvent.class, UnreachableMember.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(self());
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(CurrentClusterState.class, state -> {
			this.log.info("Current members: {}", state.members());
		}).match(MemberUp.class, mUp -> {
			this.log.info("Member is Up: {}", mUp.member());
		}).match(UnreachableMember.class, mUnreachable -> {
			this.log.info("Member detected as unreachable: {}", mUnreachable.member());
		}).match(MemberRemoved.class, mRemoved -> {
			this.log.info("Member is Removed: {}", mRemoved.member());
		}).match(MemberEvent.class, message -> {
			// ignore
		}).build();
	}
}
