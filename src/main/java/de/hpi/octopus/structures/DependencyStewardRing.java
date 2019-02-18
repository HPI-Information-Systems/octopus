package de.hpi.octopus.structures;

import java.util.Arrays;

public class DependencyStewardRing {

	private int[] busy;				// >0 if a dependency steward is either busy updating or collecting candidates
	private boolean[] validation;	// Indicates whether the dependency steward prefers validation or sampling; if it prefers validation, we should ask it for candidates, otherwise we start further sampling rounds
	
	private int current;
	
	public DependencyStewardRing(int numDependencyStewards) {
		this.busy = new int[numDependencyStewards];
		Arrays.fill(this.busy, 0);
		this.validation = new boolean[numDependencyStewards];
		
		Arrays.fill(this.validation, true);
		
		this.current = 0;
	}
	
	public void increaseBusy(int attribute) {
		this.busy[attribute] = this.busy[attribute] + 1;
	}
	
	public void decreaseBusy(int attribute) {
		this.busy[attribute] = this.busy[attribute] - 1;
	}
	
	public boolean isBusy(int attribute) {
		return !this.isIdle(attribute);
	}

	public boolean isIdle(int attribute) {
		return this.busy[attribute] == 0;
	}
	
	public void setValidation(int attribute, boolean validation) {
		this.validation[attribute] = validation;
	}
	
	public int nextIdleWithValidationPreference() {
		int candidate = -1;
		int start = this.current;
		do {
			if (this.isIdle(this.current) && this.validation[this.current])
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
			if (this.isIdle(this.current) && !this.validation[this.current])
				candidate = this.current;
			
			this.current = (this.current == this.busy.length - 1) ? 0 : this.current + 1;
		}
		while ((candidate == -1) && (this.current != start));
		
		return candidate;
	}
}
