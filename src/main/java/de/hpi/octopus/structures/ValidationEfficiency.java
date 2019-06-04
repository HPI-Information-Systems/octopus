package de.hpi.octopus.structures;

import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public class ValidationEfficiency {

	public static double calculateEfficiency(int numCandidates, int numInvalidCandidates) {
		return (double) (numCandidates - numInvalidCandidates) / (double) numCandidates;
	}
	
}
