package de.hpi.octopus.actors;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import akka.actor.AbstractActor;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.actor.Terminated;
import lombok.AllArgsConstructor;
import lombok.Data;

public class Reaper extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "reaper";

	public static Props props() {
		return Props.create(Reaper.class);
	}

	public static void watchWithDefaultReaper(AbstractActor actor) {
		ActorSelection defaultReaper = actor.getContext().getSystem().actorSelection("/user/" + DEFAULT_NAME);
		defaultReaper.tell(new WatchMeMessage(), actor.getSelf());
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @AllArgsConstructor
	public static class WatchMeMessage implements Serializable {
		private static final long serialVersionUID = -5201749681392553264L;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	private final Set<ActorRef> watchees = new HashSet<>();

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(WatchMeMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(WatchMeMessage message) {
		final ActorRef sender = this.getSender();
		
		if (this.watchees.add(sender))
			this.context().watch(sender);
	}

	private void handle(Terminated message) {
		final ActorRef sender = this.getSender();
		
		if (this.watchees.remove(sender)) {
//			this.log().info("Reaping {} so that {} actors are still alive.", sender, this.watchees.size());
			if (this.watchees.isEmpty()) {
				this.log().info("Every local actor has been reaped. Terminating the actor system...");
				this.context().system().terminate();
			//	Cluster cluster = Cluster.get(this.context().system());
			//	cluster.leave(cluster.selfAddress());
			}
		} else {
			this.log().error("Got termination message from unwatched {}.", sender);
		}
	}
}
