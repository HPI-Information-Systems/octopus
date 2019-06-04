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
		return (comparisons == 0) ? 0 : (double) matches / (double) comparisons;
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
