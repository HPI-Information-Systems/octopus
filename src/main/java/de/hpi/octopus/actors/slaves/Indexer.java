package de.hpi.octopus.actors.slaves;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.octopus.actors.masters.Preprocessor;
import de.hpi.octopus.actors.masters.Preprocessor.IndexingDoneMessage;
import de.hpi.octopus.actors.masters.Preprocessor.IndexingResultMessage;
import de.hpi.octopus.actors.masters.Preprocessor.ReallocationMessage;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import lombok.AllArgsConstructor;
import lombok.Data;

public class Indexer extends AbstractSlave {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "indexer";

	public static Props props() {
		return Props.create(Indexer.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class IndexingMessage implements Serializable {
		private static final long serialVersionUID = -3528385798844058013L;
		private IndexingMessage() {}
		private int attribute;
		private String[] values;
		private int watermark;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class FinalizeMessage implements Serializable {
		private static final long serialVersionUID = -3448209283277030409L;
		private FinalizeMessage() {}
		private int attribute;
		private int watermark;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class SendAttributesMessage implements Serializable {
		private static final long serialVersionUID = -5717818659598486449L;
		private SendAttributesMessage() {}
		private int amount;
		private ActorRef toActor;
		private int watermark;
	}
	
	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class ReceiveAttributesMessage implements Serializable {
		private static final long serialVersionUID = 9204994179561311962L;
		private ReceiveAttributesMessage() {}
		private Int2ObjectOpenHashMap<Map<String, IntArrayList>> attribute2value2positions = new Int2ObjectOpenHashMap<>();
		private Int2IntOpenHashMap attribute2offset = new Int2IntOpenHashMap();
		private int watermark;
	}

	/////////////////
	// Actor State //
	/////////////////

	private final Int2ObjectOpenHashMap<Map<String, IntArrayList>> attribute2value2positions = new Int2ObjectOpenHashMap<>();
	private final Int2IntOpenHashMap attribute2offset = new Int2IntOpenHashMap();
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(IndexingMessage.class, this::handle)
				.match(FinalizeMessage.class, this::handle)
				.match(SendAttributesMessage.class, this::handle)
				.match(ReceiveAttributesMessage.class, this::handle)
				.build()
				.orElse(super.createReceive());
	}

	@Override
	protected String getName() {
		return Indexer.DEFAULT_NAME;
	}
	
	@Override
	protected String getMasterName() {
		return Preprocessor.DEFAULT_NAME;
	}
	
	protected void handle(IndexingMessage message) throws IOException {
		final int attribute = message.getAttribute();
		final String[] values = message.getValues();
		
		if (!this.attribute2offset.containsKey(attribute)) {
			this.attribute2offset.put(attribute, 0);
			this.attribute2value2positions.put(attribute, new HashMap<>());
		}
		
		int row = this.attribute2offset.get(attribute);
		Map<String, IntArrayList> index = this.attribute2value2positions.get(attribute);
		
		for (String value : values) {
			// Simply using null values in a map implements the null = null semantic, which is used by most FD discovery algorithms; TODO: make the null semantics a parameter and implement both semantics here
			
			if (!index.containsKey(value))
				index.put(value, new IntArrayList());
			
			index.get(value).add(row);
			row++;
		}		
		this.attribute2offset.put(attribute, row);
		
		this.sender().tell(new IndexingDoneMessage(message.getWatermark()), this.self());
	}
	
	private void handle(FinalizeMessage message) throws IOException {
		int attribute = message.getAttribute();
		Map<String, IntArrayList> index = this.attribute2value2positions.remove(attribute);
		
		// Strip the values and partitions of size 1 from the index to obtain the pli
		List<IntArrayList> pliStripped = new ArrayList<>(index.size());
		for (IntArrayList cluster : index.values()) {
			if (cluster.size() > 1)
				pliStripped.add(cluster);
		}
		index = null;
		
		// Convert list into array
		int[][] pli = new int[pliStripped.size()][];
		int clusterId = 0;
		for (IntArrayList cluster : pliStripped) {
			pli[clusterId] = cluster.toIntArray();
			clusterId++;
		}
		
		int inputLength = this.attribute2offset.remove(attribute);
		
		this.sender().tell(new IndexingResultMessage(attribute, pli, inputLength, message.getWatermark()), this.self());
	}
	
	private void handle(SendAttributesMessage message) {
		Int2ObjectOpenHashMap<Map<String, IntArrayList>> sendAttribute2value2positions = new Int2ObjectOpenHashMap<>();
		Int2IntOpenHashMap seindAttribute2offset = new Int2IntOpenHashMap();
		
		IntIterator attributeIterator = this.attribute2offset.keySet().iterator();
		
		for (int i = 0; i < message.getAmount(); i++) {
			int sendAttribute = attributeIterator.nextInt();
			
			sendAttribute2value2positions.put(sendAttribute, this.attribute2value2positions.remove(sendAttribute));
			seindAttribute2offset.put(sendAttribute, this.attribute2offset.remove(sendAttribute));
		}
		
		ReceiveAttributesMessage receive = new ReceiveAttributesMessage(sendAttribute2value2positions, seindAttribute2offset, message.getWatermark());
		message.getToActor().tell(receive, this.sender());
	}
	
	private void handle(ReceiveAttributesMessage message) {
		int[] attributes = message.getAttribute2offset().keySet().toIntArray();
		
		for (int attribute : attributes) {
			this.attribute2offset.put(attribute, message.getAttribute2offset().get(attribute));
			this.attribute2value2positions.put(attribute, message.getAttribute2value2positions().get(attribute));
		}
		
		this.sender().tell(new ReallocationMessage(attributes, message.getWatermark()), this.self());
	}
}
