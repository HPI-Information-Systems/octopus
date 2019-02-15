package de.hpi.octopus.structures;

import java.util.Arrays;

import akka.actor.ActorRef;

public class DependencyStewardRing {

	private boolean[] busy;
	private boolean[] validation;
	
	private int current;
	
	public DependencyStewardRing(ActorRef[] dependencyStewards) {
		this.busy = new boolean[dependencyStewards.length];
		this.validation = new boolean[dependencyStewards.length];
		
		Arrays.fill(this.validation, true);
		
		this.current = 0;
	}
	
	public void setBusy(int attribute, boolean busy) {
		this.busy[attribute] = busy;
	}
	
	public void setValidation(int attribute, boolean validation) {
		this.validation[attribute] = validation;
	}
	
	public int nextIdleWithValidationPreference() {
		int candidate = -1;
		int start = this.current;
		do {
			if (!this.busy[this.current] && this.validation[this.current])
				candidate = this.current;
			
			this.current = (this.current == this.busy.length - 1) ? 0 : this.current + 1;
		}
		while ((candidate == -1) && (this.current != start));
		
		return candidate;
	}
	
	public int nextIdleWithSamplingPreference() {
		int candidate = -1;
		int start = this.current;
		do {
			if (!this.busy[this.current] && !this.validation[this.current])
				candidate = this.current;
			
			this.current = (this.current == this.busy.length - 1) ? 0 : this.current + 1;
		}
		while ((candidate == -1) && (this.current != start));
		
		return candidate;
	}
}
