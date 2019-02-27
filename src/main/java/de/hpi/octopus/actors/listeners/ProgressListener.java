package de.hpi.octopus.actors.listeners;

import java.io.Serializable;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import lombok.AllArgsConstructor;
import lombok.Data;

public class ProgressListener extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "progressListener";

	public static Props props() {
		return Props.create(ProgressListener.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @AllArgsConstructor
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = 2861299512936301431L;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class StewardsMessage implements Serializable {
		private static final long serialVersionUID = -1614511937688496806L;
		private StewardsMessage() {}
		private int numStewards;
	}
	
	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class FinishedMessage implements Serializable {
		private static final long serialVersionUID = -7509054641716826606L;
		private FinishedMessage() {}
		private int numFds;
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;
	private int numFDs;
	private int activeStewards;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////
	
	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(StewardsMessage.class, this::handle)
				.match(FinishedMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
	}
	
	private void handle(StewardsMessage message) {
		this.numFDs = 0;
		this.activeStewards = message.getNumStewards();
	}
	
	private void handle(FinishedMessage message) {
		this.activeStewards--;
		this.numFDs = this.numFDs + message.getNumFds();
		
		if (this.activeStewards == 0) {
			this.log().info("Found {} FDs in {} ms", this.numFDs, System.currentTimeMillis() - this.startTime);
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
	}
}
