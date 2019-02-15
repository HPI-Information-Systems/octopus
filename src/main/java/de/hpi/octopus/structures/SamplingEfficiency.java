package de.hpi.octopus.structures;

import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public class SamplingEfficiency implements Comparable<SamplingEfficiency> {
	
	private final int attribute;
	private double efficiency;
	private int distance;
	
	public SamplingEfficiency(int attribute) {
		this.attribute = attribute;
		this.efficiency = Double.MAX_VALUE;
		this.distance = 0;
	}
	
	public int step() {
		this.distance++;
		this.efficiency = 0;
		return this.distance;
	}
	
	public void update(int comparisons, int matches, int distance) {
		if (this.distance > distance)
			return;
		
		if (comparisons == 0) {
			this.efficiency = 0;
			return;
		}
		
		this.efficiency = (double) matches / (double) comparisons;
	}
	
	public boolean hasStepped() {
		return this.distance > 0;
	}
	
	@Override
	public int compareTo(SamplingEfficiency other) {
		if (other.getEfficiency() == this.efficiency)
			return other.getDistance() - this.distance;
		
		return (int) Math.signum(other.getEfficiency() - this.efficiency);
	}
}
