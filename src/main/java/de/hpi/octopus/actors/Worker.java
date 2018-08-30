package de.hpi.octopus.actors;

import static de.hpi.octopus.transformation.TransformationMessages.BACKEND_REGISTRATION;

import java.io.Serializable;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.actor.AbstractActor.Receive;
import akka.cluster.Cluster;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberUp;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.octopus.actors.Profiler.CompletionMessage;
import de.hpi.octopus.actors.Profiler.RegistrationMessage;
import de.hpi.octopus.transformation.TransformationMessages.TransformationJob;
import de.hpi.octopus.transformation.TransformationMessages.TransformationResult;
import lombok.AllArgsConstructor;
import lombok.Data;

public class Worker extends AbstractActor {

	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class WorkMessage implements Serializable {
		private static final long serialVersionUID = -7643194361868862395L;
		private WorkMessage() {}
		private int[] x;
		private int[] y;
	}

	LoggingAdapter log = Logging.getLogger(getContext().system(), this);
	Cluster cluster = Cluster.get(getContext().system());

	@Override
	public void preStart() {
		this.cluster.subscribe(this.self(), MemberUp.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(WorkMessage.class, this::handle)
				.matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	void register(Member member) {
		if (member.hasRole("master")) work here // TODO read master name from somewhere else // TODO start a profiler and some worker in OctopusMaster/-Slave
			getContext()
				.actorSelection(member.address() + "/user/" + Profiler.DEFAULT_NAME)
				.tell(new RegistrationMessage(), this.self());
	}

	private void handle(WorkMessage message) {
		boolean y = true;
		for (int i = 0; i < 100000; i++)
			y = y && "aiurgvilabgvöaieuröiaebrvöiuervaöioeur".equals("aiurgvilabgvöaieuröiaebrvöiuervaöioeur");

		this.sender().tell(new CompletionMessage(CompletionMessage.status.EXTENDABLE), this.self());
	}
}