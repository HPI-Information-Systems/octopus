package de.hpi.octopus.actors;

import java.io.Serializable;
import java.lang.management.ManagementFactory;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import de.hpi.octopus.actors.Validator.CacheUpdatedMessage;
import de.hpi.octopus.structures.PliCache;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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

		Runtime.getRuntime().gc();
		
		long maxMemory = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
		long usedMemory = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
		
		this.maxCacheSize = (long) (0.5 * (maxMemory - usedMemory)); // TODO: parameter? use fix 50% of available memory for cache?
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CacheMessage implements Serializable {
		private static final long serialVersionUID = -1216066687015611476L;
		private int[][] pli;
		private int[] attributes;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BlacklistMessage implements Serializable {
		private static final long serialVersionUID = 479879661255723452L;
		private int[] attributes;
	}
	
	@Data @NoArgsConstructor
	public static class NotifyMessage implements Serializable {
		private static final long serialVersionUID = 1267398974030641018L;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private volatile PliCache cache;
	
	private long maxCacheSize;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CacheMessage.class, this::handle)
				.match(BlacklistMessage.class, this::handle)
				.match(NotifyMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(CacheMessage message) {
		// Add new pli
		this.cache.add(message.getAttributes(), message.getPli());
		
		// Check memory consumption and remove plis from the cache if memory is exhausted
		this.ensureCapacity();
		
		// Reset the cache reference to make its elements effectively volatile
		this.cache = this.cache;
	}
	
	protected void handle(BlacklistMessage message) {
		// Blacklist pli
		this.cache.blacklist(message.getAttributes());
		
		// Check memory consumption and remove plis from the cache if memory is exhausted
		this.ensureCapacity();
		
		// Reset the cache reference to make its elements effectively volatile
		this.cache = this.cache;
	}
	
	protected void ensureCapacity() {
	//	this.log().info("{} MB", this.cache.getByteSize() / 1000 / 1000);
		
		if (this.cache.getByteSize() > this.maxCacheSize) {
			final long oldSize = this.cache.getByteSize();
			this.cache.prune(this.maxCacheSize);final long newSize = this.cache.getByteSize();
			this.log().info("Pruned pli cache from {} byte, which is over the maximum of {} byte, to under {} byte.", oldSize, this.maxCacheSize, newSize);
		}
	}
	
	protected void handle(NotifyMessage message) {
		// Tell the sender of the message that all its updates are done
		this.sender().tell(new CacheUpdatedMessage(), this.self());
	}
}
