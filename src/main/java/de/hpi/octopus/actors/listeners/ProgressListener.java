package de.hpi.octopus.actors.listeners;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import de.hpi.octopus.actors.Reaper;
import de.hpi.octopus.structures.Dataset;
import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.result_receiver.FunctionalDependencyResultReceiver;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import lombok.AllArgsConstructor;
import lombok.Data;

public class ProgressListener extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "progressListener";

	public static Props props(FunctionalDependencyResultReceiver resultReceiver) {
		return Props.create(ProgressListener.class, () -> new ProgressListener(resultReceiver));
	}
	
	public ProgressListener(FunctionalDependencyResultReceiver resultReceiver) {
		this.resultReceiver = resultReceiver;
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
		private BitSet[] lhss;
		private int rhs;
		private Dataset dataset;
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;
	private int numFDs;
	private int activeStewards;
	private FunctionalDependencyResultReceiver resultReceiver;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		// Register at this actor system's reaper
		Reaper.watchWithDefaultReaper(this);
	}

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
		
		if ((this.resultReceiver != null) && (message.getLhss() != null)) {
			ColumnIdentifier[] columnIndentifiers = message.getDataset().getColumnIdentifiers();
			
			ColumnIdentifier rhs = columnIndentifiers[message.getRhs()];
			for (BitSet lhsAttributes : message.getLhss()) {
				ArrayList<ColumnIdentifier> lhs = new ArrayList<>(lhsAttributes.cardinality());
				for (int i = lhsAttributes.nextSetBit(0); i >= 0; i = lhsAttributes.nextSetBit(i + 1))
					lhs.add(columnIndentifiers[i]);
				
				this.resultReceiver.acceptedResult(new FunctionalDependency(new ColumnCombination(lhs.toArray(new ColumnIdentifier[0])), rhs));
			}
		}
	}
}
