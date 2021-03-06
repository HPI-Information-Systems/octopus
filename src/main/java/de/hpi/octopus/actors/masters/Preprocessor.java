package de.hpi.octopus.actors.masters;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.octopus.actors.DatasetReader;
import de.hpi.octopus.actors.DatasetReader.ReadMessage;
import de.hpi.octopus.actors.DatasetReader.RestartMessage;
import de.hpi.octopus.actors.listeners.ProgressListener;
import de.hpi.octopus.actors.masters.Profiler.DiscoveryTaskMessage;
import de.hpi.octopus.actors.slaves.Indexer.FinalizeMessage;
import de.hpi.octopus.actors.slaves.Indexer.IndexingMessage;
import de.hpi.octopus.actors.slaves.Indexer.SendAttributesMessage;
import de.hpi.octopus.configuration.ConfigurationSingleton;
import de.hpi.octopus.configuration.DatasetDescriptorSingleton;
import de.hpi.octopus.io.RelationalInputGeneratorSingleton;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap.Entry;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Preprocessor extends AbstractMaster {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "preprocessor";

	public static Props props() {
		return Props.create(Preprocessor.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @NoArgsConstructor
	public static class PreprocessingTaskMessage implements Serializable {
		private static final long serialVersionUID = -4788853430111845038L;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class IndexingResultMessage implements Serializable {
		private static final long serialVersionUID = 5074517366545874380L;
		private int attribute;
		private int[][] pli;
		private int inputLength;
		private int watermark;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 7327628760076825469L;
		private List<List<String>> batch;
		private String relationName;
		private String[] columnNames;
		private int watermark;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class IndexingDoneMessage implements Serializable {
		private static final long serialVersionUID = 1221354994262265715L;
		private int watermark;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class ReallocationMessage implements Serializable {
		private static final long serialVersionUID = 8794479344949532177L;
		private int[] attributes;
		private int watermark;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private int[][][] plis;
	private int numRecords;
	private String relationName;
	private String[] columnNames;
	
	private int watermark = 0; // If the preprocessing needs to be restarted, the watermark is used to identify old messages that need to be dropped
	
	private ActorRef datasetReader;
	private boolean nullEqualsNull;
	
	private List<List<String>> waitingBatch = null;
	private List<ActorRef> idleIndexers = new ArrayList<ActorRef>();
	
	private Int2ObjectOpenHashMap<ActorRef> attribute2indexer = new Int2ObjectOpenHashMap<>();
	private Queue<ActorRef> indexers = new LinkedList<>();
	
	private int pendingResponses = 0;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(PreprocessingTaskMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(ReallocationMessage.class, this::handle)
				.match(IndexingDoneMessage.class, this::handle)
				.match(IndexingResultMessage.class, this::handle)
				.build()
				.orElse(super.createReceive());
	}
	
	@Override
	protected void handle(RegistrationMessage message) {
		super.handle(message);
		
		this.indexers.add(this.sender());
		
		// If there is a batch waiting, this is the first indexer and it will get all attributes
		if (this.waitingBatch != null) {
			this.assign(this.waitingBatch);
			this.waitingBatch = null;
			return;
		}
		
		// If work is in progress, re-assign work to this new indexer and let it steal work progress of that work
		if (!this.attribute2indexer.isEmpty())
			this.idleIndexers.add(this.sender());
	}
	
	@Override
	protected void handle(Terminated message) {
		super.handle(message);
		
		this.indexers.remove(message.getActor());
		
		// If the sender was tasked with an attribute, restart the indexing because the sender's part is inevitably lost
		if (this.attribute2indexer.values().contains(message.getActor())) {
			this.datasetReader.tell(new RestartMessage(this.watermark), this.self());
			
			this.watermark++;
			
			this.plis = null;
			this.numRecords = 0;
			this.waitingBatch = null;
			this.idleIndexers = new ArrayList<ActorRef>();
			this.attribute2indexer = new Int2ObjectOpenHashMap<>();
			this.pendingResponses = 0;
		}
	}
	
	private void handle(PreprocessingTaskMessage message) throws Exception {
		if (this.datasetReader != null) {
			this.log().error("Can process only one task! Dropping preprocessing request for " + DatasetDescriptorSingleton.get().getDatasetPathNameEnding());
			return;
		}
		
		this.nullEqualsNull = ConfigurationSingleton.get().isNullEqualsNull();
		
		this.context().actorSelection("/user/" + ProgressListener.DEFAULT_NAME).tell(new ProgressListener.StartMessage(), ActorRef.noSender());
		
		this.datasetReader = this.context().actorOf(DatasetReader.props(RelationalInputGeneratorSingleton.get(), ConfigurationSingleton.get().getBufferSize()), DatasetReader.DEFAULT_NAME);

		this.datasetReader.tell(new ReadMessage(this.watermark), this.self());
	}

	private void handle(BatchMessage message) {
		if (this.watermark != message.getWatermark())
			return;
		
		final List<List<String>> batch = message.getBatch();
		
		// If no batch data was send, the input has been read and processed so that we can query the results
		if (batch == null) {
			this.plis = new int[this.attribute2indexer.size()][][];
			this.numRecords = 0;
			this.relationName = message.getRelationName();
			this.columnNames = message.getColumnNames();
			
			if (this.attribute2indexer.isEmpty()) {
				this.endPreprocessing();
				return;
			}
			
			for (int attribute = 0; attribute < this.attribute2indexer.size(); attribute++)
				this.attribute2indexer.get(attribute).tell(new FinalizeMessage(attribute, this.watermark), this.self());
			
			this.pendingResponses = this.attribute2indexer.size();
			return;
		}
		
		// If no indexer exists (yet) to handle the batch, put this batch on waiting
		if (this.indexers.isEmpty()) {
			this.waitingBatch = batch;
			return;
		}
		
		// Assign batch to indexer(s)
		this.assign(batch);
	}
	
	private void assign(List<List<String>> batch) {
		// If no assignment has been made before, assign attributes to indexers
		if (this.attribute2indexer.isEmpty()) {
			int numAttributes = batch.get(0).size();
			
			Iterator<ActorRef> indexerIterator = this.indexers.iterator();
			ActorRef indexer = indexerIterator.next();
			
			for (int i = 0; i < numAttributes; i++) {
				this.attribute2indexer.put(i, indexer);
				if (!indexerIterator.hasNext())
					indexerIterator = this.indexers.iterator();
				indexer = indexerIterator.next();
			}
		}
		
		// Send attribute vectors to indexers
		for (int attribute = 0; attribute < this.attribute2indexer.size(); attribute++) {
			final String[] values = new String[batch.size()];
			for (int i = 0; i < batch.size(); i++)
				values[i] = batch.get(i).get(attribute);
			this.attribute2indexer.get(attribute).tell(new IndexingMessage(attribute, values, this.nullEqualsNull, this.watermark), this.self());
		}
		this.pendingResponses = this.attribute2indexer.size();
	}

	private void handle(IndexingDoneMessage message) {
		if (this.watermark != message.getWatermark())
			return;
		
		this.receivedPendingResponse();
	}
	
	private void receivedPendingResponse() {
		this.pendingResponses--;
		if (this.pendingResponses > 0)
			return;
		
		if (!this.idleIndexers.isEmpty()) {
			this.reallocateAttributes();
			
			if (this.pendingResponses > 0)
				return;
		}
		
		this.datasetReader.tell(new ReadMessage(this.watermark), this.self());
	}

	private void reallocateAttributes() {
		int numAttributes = this.attribute2indexer.size();
		int numAttributesPerIndexer = Math.max(numAttributes / this.indexers.size(), 1);
		
		final Object2IntOpenHashMap<ActorRef> counts = new Object2IntOpenHashMap<>(this.indexers.size());
		this.attribute2indexer.values().forEach(indexer -> counts.put(indexer, counts.getInt(indexer) + 1));
		
		this.log().info("Reallocation!");
		
		int idleIndexerAttributes = 0;
		ActorRef idleIndexer = this.idleIndexers.remove(this.idleIndexers.size() - 1);
		for (Entry<ActorRef> entry : counts.object2IntEntrySet()) {
			ActorRef busyIndexer = entry.getKey();
			int numSharableAttributes = entry.getIntValue() - numAttributesPerIndexer;
			while (numSharableAttributes > 0) {
				int numSendAttributes = Math.min(numSharableAttributes, numAttributesPerIndexer - idleIndexerAttributes);
				numSharableAttributes = numSharableAttributes - numSendAttributes;
				
				busyIndexer.tell(new SendAttributesMessage(numSendAttributes, idleIndexer, this.watermark), this.self());
				this.pendingResponses++;
				
				this.log().info(numSendAttributes + " attributes from " + busyIndexer.path() + " to " + idleIndexer.path());
				
				idleIndexerAttributes = idleIndexerAttributes + numSendAttributes;
				if (idleIndexerAttributes >= numAttributesPerIndexer) {
					if (this.idleIndexers.isEmpty())
						return;
					idleIndexerAttributes = 0;
					idleIndexer = this.idleIndexers.remove(this.idleIndexers.size() - 1);
				}
			}
		}
		this.idleIndexers.clear();
	}
	
	private void handle(ReallocationMessage message) {
		if (this.watermark != message.getWatermark())
			return;
		
		for (int attribute : message.getAttributes())
			this.attribute2indexer.put(attribute, this.sender());
		
		this.receivedPendingResponse();
	}
	
	private void handle(IndexingResultMessage message) {
		if (this.watermark != message.getWatermark())
			return;
		
		this.plis[message.getAttribute()] = message.getPli();
		this.numRecords = message.getInputLength();
		
		this.pendingResponses--;
		
		if (this.pendingResponses == 0)
			this.endPreprocessing();
	}
	
	private void endPreprocessing() {
		// Report resulting plis
		this.context().actorSelection("/user/" + Profiler.DEFAULT_NAME).tell(new DiscoveryTaskMessage(this.plis, this.numRecords, this.relationName, this.columnNames), this.self());
		
		// Terminate the preprocessing hierarchy
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.datasetReader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.indexers.forEach(indexer -> indexer.tell(PoisonPill.getInstance(), ActorRef.noSender()));
	}
}
