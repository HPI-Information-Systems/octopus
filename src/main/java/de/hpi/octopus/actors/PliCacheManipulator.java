package de.hpi.octopus.actors;

import java.io.Serializable;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import de.hpi.octopus.actors.Validator.CacheUpdatedMessage;
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
	public static class UpdateCacheMessage implements Serializable {
		private static final long serialVersionUID = -3287847057307621040L;
		private UpdateCacheMessage() {}
		private List<int[][]> plis;
		private List<int[]> pliAttributes;
		private List<int[]> blacklistAttributes;
		public boolean isEmpty() {
			return this.plis.isEmpty() && this.blacklistAttributes.isEmpty();
		}
		public void addPli(int[][] pli, int[] pliAttributes) {
			this.plis.add(pli);
			this.pliAttributes.add(pliAttributes);
		}
		public void addBlacklist(int[] blacklistAttributes) {
			this.blacklistAttributes.add(blacklistAttributes);
		}
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
				.match(UpdateCacheMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(UpdateCacheMessage message) {
		// Check memory consumption and remove plis from the cache if memory is exhausted
			// TODO
		
		// Add new plis
		for (int i = 0; i < message.getPlis().size(); i++)
			this.cache.add(message.getPliAttributes().get(i), message.getPlis().get(i));
		
		// Blacklist plis
		for (int i = 0; i < message.getBlacklistAttributes().size(); i++)
			this.cache.blacklist(message.getBlacklistAttributes().get(i));
		
		// Reset the cache reference to make its elements effectively volatile
		this.cache = this.cache;
		
		// Tell the sender of the update message that the update is done
		this.sender().tell(new CacheUpdatedMessage(), this.self());
	}
}
