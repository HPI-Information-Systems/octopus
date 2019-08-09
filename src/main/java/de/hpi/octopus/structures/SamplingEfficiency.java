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
	
	public static double calculateEfficiency(int comparisons, int matches) {
		// Report an efficiency of 0, if no record comparisons have been executed
		if (comparisons == 0)
			return 0;
		
		// Report the percentage of matching records by all compared records as efficiency
		return (double) matches / (double) comparisons;
	}
	
	public void update(double efficiency, int distance) {
		if (this.distance > distance)
			return;
		
		this.efficiency = efficiency;
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
