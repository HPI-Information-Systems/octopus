package de.hpi.octopus.actors.masters;

import java.util.Arrays;
import java.util.List;

import akka.actor.Props;
import de.hpi.octopus.actors.slaves.Slave.WorkMessage;
import de.hpi.octopus.actors.slaves.Validator.ValidationMessage;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

public class Profiler extends Master {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "profiler";

	public static Props props() {
		return Props.create(Profiler.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @EqualsAndHashCode(callSuper=false)  @AllArgsConstructor @SuppressWarnings("unused")
	public static class DiscoveryTaskMessage extends TaskMessage {
		private static final long serialVersionUID = -8330958742629706627L;
		private DiscoveryTaskMessage() {}
		private int attributes;
		private List<int[][]> plis;
		private String input;
	}
	
	@Data @EqualsAndHashCode(callSuper=false) @AllArgsConstructor @SuppressWarnings("unused")
	public static class ValidationResultMessage extends ResultMessage {
		private static final long serialVersionUID = -6823011111281387872L;
		public enum status {MINIMAL, EXTENDABLE, FALSE}
		private ValidationResultMessage() {}
		private status result;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////
	
	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	protected List<WorkMessage> split(TaskMessage message) throws Exception {
		return Arrays.asList(new ValidationMessage(new int[0], new int[0]));
	}

	@Override
	protected void finish(WorkMessage work, ResultMessage result) {
		ValidationMessage validationMessage = (ValidationMessage) work;
		ValidationResultMessage validationResultMessage = (ValidationResultMessage) result;

		this.log.info("Completed: [{},{}]", Arrays.toString(validationMessage.getX()), Arrays.toString(validationMessage.getY()));
		
		switch (validationResultMessage.getResult()) {
			case MINIMAL: 
				this.report(validationMessage);
				break;
			case EXTENDABLE:
				this.split(validationMessage);
				break;
			case FALSE:
				// Ignore
				break;
		}
	}
	
	@Override
	protected void finish() {
		DiscoveryTaskMessage discoveryTaskMessage = (DiscoveryTaskMessage) this.task;
		
		this.log.info("Finished discovery task {} with {} attributes.", discoveryTaskMessage.getInput(), discoveryTaskMessage.getAttributes());
				
		// TODO Tell the Application that the discovery is done
	}
	
	private void report(ValidationMessage work) {
		this.log.info("UCC: {}", Arrays.toString(work.getX()));
		
		// TODO Write somewhere else or tell someone
	}

	private void split(ValidationMessage work) {
		int[] x = work.getX();
		int[] y = work.getY();
		
		int next = x.length + y.length;
		
		DiscoveryTaskMessage discoveryTaskMessage = (DiscoveryTaskMessage) this.task;
		
		if (next < discoveryTaskMessage.getAttributes() - 1) {
			int[] xNew = Arrays.copyOf(x, x.length + 1);
			xNew[x.length] = next;
			this.assign(new ValidationMessage(xNew, y));
			
			int[] yNew = Arrays.copyOf(y, y.length + 1);
			yNew[y.length] = next;
			this.assign(new ValidationMessage(x, yNew));
		}
	}
}