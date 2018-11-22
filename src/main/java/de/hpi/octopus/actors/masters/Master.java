package de.hpi.octopus.actors.masters;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Terminated;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.octopus.actors.slaves.Slave.WorkMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

public abstract class Master extends AbstractActor {

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @AllArgsConstructor
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}

	@Data @AllArgsConstructor
	public static class TaskMessage implements Serializable {
		private static final long serialVersionUID = -5966896383236151313L;
	}
	
	@Data @AllArgsConstructor
	public static class ResultMessage implements Serializable {
		private static final long serialVersionUID = -8362075416131270850L;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	protected final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	protected final Queue<WorkMessage> unassignedWork = new LinkedList<>();
	protected final Queue<ActorRef> idleWorkers = new LinkedList<>();
	protected final Map<ActorRef, WorkMessage> busyWorkers = new HashMap<>();
	
	protected TaskMessage task;

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
				.match(TaskMessage.class, this::handle)
				.match(ResultMessage.class, this::handle)
				.matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		
		this.assign(this.sender());
		this.log.info("Registered {}", this.sender());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		
		if (!this.idleWorkers.remove(message.getActor())) {
			WorkMessage work = this.busyWorkers.remove(message.getActor());
			if (work != null) {
				this.assign(work);
			}
		}		
		this.log.info("Unregistered {}", message.getActor());
	}
	
	protected void handle(TaskMessage message) throws Exception {
		if (this.task != null)
			this.log.error("Can process only one task at a time!");
		
		this.task = message;
		
		for (WorkMessage work : this.split(message))
			this.assign(work);
	}
	
	protected abstract List<WorkMessage> split(TaskMessage message) throws Exception;
	
	protected void handle(ResultMessage message) {
		ActorRef worker = this.sender();
		WorkMessage work = this.busyWorkers.remove(worker);
		this.finish(work, message);
		
		if (this.unassignedWork.isEmpty() && this.busyWorkers.isEmpty()) {
			this.finish();
			this.task = null;
		} else {
			this.assign(worker);
		}
	}
	
	protected abstract void finish(WorkMessage work, ResultMessage result);
	
	protected abstract void finish();
	
	protected void assign(WorkMessage work) {
		ActorRef worker = this.idleWorkers.poll();
		
		if (worker == null) {
			this.unassignedWork.add(work);
			return;
		}
		
		this.busyWorkers.put(worker, work);
		worker.tell(work, this.self());
	}
	
	protected void assign(ActorRef worker) {
		WorkMessage work = this.unassignedWork.poll();
		
		if (work == null) {
			this.idleWorkers.add(worker);
			return;
		}
		
		this.busyWorkers.put(worker, work);
		worker.tell(work, this.self());
	}
}
