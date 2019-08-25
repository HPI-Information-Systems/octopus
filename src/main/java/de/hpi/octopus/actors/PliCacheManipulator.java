package de.hpi.octopus.actors;

import java.io.Serializable;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import de.hpi.octopus.structures.PliCache;
import lombok.AllArgsConstructor;
import lombok.Data;

public class PliCacheManipulator extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "pliCacheManipulator";

	public static Props props(final PliCache cache) {
		return Props.create(PliCacheManipulator.class, () -> new PliCacheManipulator(cache));
	}

	public PliCacheManipulator(final PliCache cache) {
		this.cache = cache;
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class AddMessage implements Serializable {
		private static final long serialVersionUID = -3287847057307621040L;
		private AddMessage() {}
		private int[][] pli;
		private int[] attributes;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private volatile PliCache cache;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(AddMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(AddMessage message) {
		this.cache.add(message.getAttributes(), message.getPli());
		
		// Reset the cache reference to make its elements effectively volatile
		this.cache = this.cache;
	}

}
