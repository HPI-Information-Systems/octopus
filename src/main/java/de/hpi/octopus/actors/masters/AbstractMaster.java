package de.hpi.octopus.actors.masters;

import java.io.Serializable;

import akka.actor.AbstractLoggingActor;
import akka.actor.Terminated;
import lombok.AllArgsConstructor;
import lombok.Data;

public abstract class AbstractMaster extends AbstractLoggingActor {

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
		private RegistrationMessage() {}
		private String actorName;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////
	
	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(RegistrationMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());

		this.log().info("Registered {}", this.sender());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		
		this.log().info("Unregistered {}", message.getActor());
	}
	
}
