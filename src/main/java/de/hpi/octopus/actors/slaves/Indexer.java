package de.hpi.octopus.actors.slaves;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.octopus.actors.LargeMessageProxy.LargeMessage;
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

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class ReceiveAttributesCompactMessage implements Serializable {
		private static final long serialVersionUID = 9204994179561311962L;
		private ReceiveAttributesCompactMessage() {}
		private int[] attributes;
		private int[] offsets;
		private String[][] values;
		private int[][][] positions;
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
				.match(ReceiveAttributesCompactMessage.class, this::handle)
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
		for (IntArrayList cluster : index.values())
			if (cluster.size() > 1)
				pliStripped.add(cluster);
		index = null;
		
		// Convert list into array
		int[][] pli = new int[pliStripped.size()][];
		for (int clusterId = 0; clusterId < pliStripped.size(); clusterId++)
			pli[clusterId] = pliStripped.get(clusterId).toIntArray();
		pliStripped = null;
		
		int inputLength = this.attribute2offset.remove(attribute);
		
		this.sender().tell(new IndexingResultMessage(attribute, pli, inputLength, message.getWatermark()), this.self());
	}
	
	private void handle(SendAttributesMessage message) {
		Int2ObjectOpenHashMap<Map<String, IntArrayList>> sendAttribute2value2positions = new Int2ObjectOpenHashMap<>();
		Int2IntOpenHashMap sendAttribute2offset = new Int2IntOpenHashMap();
		
		// Collect indexes and offsets for attributes that should be send
		IntIterator attributeIterator = this.attribute2offset.keySet().iterator();
		for (int i = 1; i <= message.getAmount(); i++) {
			int sendAttribute = attributeIterator.nextInt();
			
			sendAttribute2value2positions.put(sendAttribute, this.attribute2value2positions.get(sendAttribute));
			sendAttribute2offset.put(sendAttribute, this.attribute2offset.get(sendAttribute));
		}
		
		// Remove indexes and offsets for attributes that should be send
		for (int sendAttribute : sendAttribute2offset.keySet()) {
			this.attribute2value2positions.remove(sendAttribute);
			this.attribute2offset.remove(sendAttribute);
		}
		
		// Send indexes and offsets
		ReceiveAttributesMessage receiveAttributesMessage = new ReceiveAttributesMessage(sendAttribute2value2positions, sendAttribute2offset, message.getWatermark());
		ReceiveAttributesCompactMessage receiveAttributesCompactMessage = this.compactMesage(receiveAttributesMessage); // TODO: Compact or not compact?
		this.largeMessageProxy.tell(new LargeMessage<>(receiveAttributesCompactMessage, message.getToActor(), false), this.sender());
	}
	
	private void handle(ReceiveAttributesCompactMessage message) {
		ReceiveAttributesMessage uncompactedMessage = this.uncompactMesage(message);
		int[] attributes = uncompactedMessage.getAttribute2offset().keySet().toIntArray();
		
		for (int attribute : attributes) {
			this.attribute2offset.put(attribute, uncompactedMessage.getAttribute2offset().get(attribute));
			this.attribute2value2positions.put(attribute, uncompactedMessage.getAttribute2value2positions().get(attribute));
		}
		
		this.sender().tell(new ReallocationMessage(attributes, message.getWatermark()), this.self());
	}
	
	private ReceiveAttributesCompactMessage compactMesage(ReceiveAttributesMessage message) {
		Int2ObjectOpenHashMap<Map<String, IntArrayList>> sendAttribute2value2positions = message.getAttribute2value2positions();
		Int2IntOpenHashMap sendAttribute2offset = message.getAttribute2offset();
		
		int[] attributes = new int[sendAttribute2value2positions.size()];
		int[] offsets = new int[sendAttribute2value2positions.size()];
		String[][] values = new String[sendAttribute2value2positions.size()][];
		int[][][] positions = new int[sendAttribute2value2positions.size()][][];
		
		int i = 0;
		for (it.unimi.dsi.fastutil.ints.Int2ObjectMap.Entry<Map<String, IntArrayList>> attributeEntry : sendAttribute2value2positions.int2ObjectEntrySet()) {
			int attribute = attributeEntry.getIntKey();
			int offset = sendAttribute2offset.get(attribute);
			
			attributes[i] = attribute;
			offsets[i] = offset;
			values[i] = new String[sendAttribute2value2positions.get(attribute).size()];
			positions[i] = new int[sendAttribute2value2positions.get(attribute).size()][];
			
			int j = 0;
			for (Entry<String, IntArrayList> valueEntry : attributeEntry.getValue().entrySet()) {
				String value = valueEntry.getKey();
				int[] position = valueEntry.getValue().toIntArray();
				
				values[i][j] = value;
				positions[i][j] = position;
				
				j++;
			}
			
			i++;
		}
		return new ReceiveAttributesCompactMessage(attributes, offsets, values, positions, message.getWatermark());
	}
	
	private ReceiveAttributesMessage uncompactMesage(ReceiveAttributesCompactMessage message) {
		Int2ObjectOpenHashMap<Map<String, IntArrayList>> attribute2value2positions = new Int2ObjectOpenHashMap<>();
		Int2IntOpenHashMap attribute2offset = new Int2IntOpenHashMap();
		
		int[] attributes = message.getAttributes();
		
		for (int i = 0; i < attributes.length; i++) {
			int attribute = message.getAttributes()[i];
			int offset = message.getOffsets()[i];
			String[] values = message.getValues()[i];
			int[][] positions = message.getPositions()[i];
			
			Map<String, IntArrayList> value2positions = new HashMap<String, IntArrayList>(values.length + values.length / 5); // initialize 20% larger than it is currently
			for (int j = 0; j < values.length; j++)
				value2positions.put(values[j], IntArrayList.wrap(positions[j]));
			
			attribute2offset.put(attribute, offset);
			attribute2value2positions.put(attribute, value2positions);
		}
		return new ReceiveAttributesMessage(attribute2value2positions, attribute2offset, message.getWatermark());
	}
}
