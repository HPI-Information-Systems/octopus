package de.hpi.octopus.actors;

import java.io.Serializable;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import de.hpi.octopus.structures.BitSet;
import de.hpi.octopus.structures.BloomFilter;
import lombok.AllArgsConstructor;
import lombok.Data;

public class FilterManipulator extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "filterManipulator";

	public static Props props(final BloomFilter filter) {
		return Props.create(FilterManipulator.class, () -> new FilterManipulator(filter));
	}

	public FilterManipulator(final BloomFilter filter) {
		this.filter = new BloomFilter();
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class AddMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
		private AddMessage() {}
		private BitSet element;
	}
	
	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class AddAllMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private AddAllMessage() {}
		private List<BitSet> elements;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class MergeMessage implements Serializable {
		private static final long serialVersionUID = 6021174835855123367L;
		private MergeMessage() {}
		private BloomFilter other;
	}

	/////////////////
	// Actor State //
	/////////////////

	private volatile BloomFilter filter;
	
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
				.match(AddAllMessage.class, this::handle)
				.match(MergeMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(AddMessage message) {
		this.filter.add(message.getElement());
		
		// Reset the filter reference to make its elements effectively volatile
		this.filter = this.filter;
	}
	
	protected void handle(AddAllMessage message) {
		this.filter.addAll(message.getElements());
		
		// Reset the filter reference to make its elements effectively volatile
		this.filter = this.filter;
	}
	
	protected void handle(MergeMessage message) {
		this.filter.merge(message.getOther());
		
		// Reset the filter reference to make its elements effectively volatile
		this.filter = this.filter;
	}
}
