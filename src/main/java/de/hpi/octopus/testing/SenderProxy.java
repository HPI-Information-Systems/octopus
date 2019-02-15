package de.hpi.octopus.testing;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import de.hpi.octopus.testing.ReceiverProxy.ReliableMessage;
import scala.concurrent.duration.Duration;

public class SenderProxy extends AbstractLoggingActor {

	private ActorRef receiverProxy;
	
	private int sequenceNumber = 0;
	private Map<Integer, Cancellable> pendingAcks = new HashMap<>();
	
	public SenderProxy(ActorRef receiverProxy) {
		this.receiverProxy = receiverProxy;
	}

	public static Props props(ActorRef receiverProxy) {
		return Props.create(SenderProxy.class, () -> new SenderProxy(receiverProxy));
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(String.class, this::handle)
				.match(Integer.class, this::handle)
				.matchAny(object -> this.log().info("Unknown message: \"{}\"", object.toString()))
				.build();
	}
	
	private void handle(String message) {
		Cancellable cancellable = this.createCancellableFor(
				new ReliableMessage(this.sequenceNumber, message, this.sender()));
		this.pendingAcks.put(this.sequenceNumber, cancellable);
		this.sequenceNumber++;
	}
	
	private void handle(Integer message) {
		Cancellable cancellable = this.pendingAcks.remove(message);
		this.stopCancellable(cancellable);
	}
	
	private Cancellable createCancellableFor(ReliableMessage message) {
		return this.getContext().system().scheduler().schedule(
				Duration.create(0, TimeUnit.SECONDS), 
				Duration.create(3, TimeUnit.SECONDS), 
				this.receiverProxy, 
				message, 
				this.getContext().dispatcher(), 
				this.self()); 
	}
	
	private void stopCancellable(Cancellable cancellable) {
		cancellable.cancel();
	}
}
