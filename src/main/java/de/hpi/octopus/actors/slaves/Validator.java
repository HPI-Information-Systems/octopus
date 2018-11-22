package de.hpi.octopus.actors.slaves;

import akka.actor.Props;
import de.hpi.octopus.actors.masters.Profiler;
import de.hpi.octopus.actors.masters.Profiler.ValidationResultMessage;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

public class Validator extends Slave {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "validator";

	public static Props props() {
		return Props.create(Validator.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @EqualsAndHashCode(callSuper=false) @AllArgsConstructor @SuppressWarnings("unused")
	public static class ValidationMessage extends WorkMessage {
		private static final long serialVersionUID = -7643194361868862395L;
		private ValidationMessage() {}
		private int[] x;
		private int[] y;
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
	protected String getMasterName() {
		return Profiler.DEFAULT_NAME;
	}
	
	@Override
	protected void handle(WorkMessage message) {
		ValidationMessage validationMessage = (ValidationMessage) message;
		
		long y = 0;
		for (int i = 0; i < 1000000; i++)
			if (this.isPrime(i))
				y = y + i;
		
		this.log.info("done: " + y);
		
		this.sender().tell(new ValidationResultMessage(ValidationResultMessage.status.EXTENDABLE), this.self());
	}
	
	private boolean isPrime(long n) {
		
		// Check for the most basic primes
		if (n == 1 || n == 2 || n == 3)
			return true;

		// Check if n is an even number
		if (n % 2 == 0)
			return false;

		// Check the odds
		for (long i = 3; i * i <= n; i += 2)
			if (n % i == 0)
				return false;
		
		return true;
	}
}