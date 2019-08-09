package de.hpi.octopus.structures;

import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public class ValidationEfficiency {

	public static double calculateEfficiency(int numCandidates, int numInvalidCandidates) {
		// Report an efficiency of 1, if invalid candidates have been produced with no candidates
		if (numCandidates <= 0)
			return 1;
		
		// Report the percentage of true candidates by all candidates as efficiency
		return (double) (numCandidates - numInvalidCandidates) / (double) numCandidates;
	}
	
}
