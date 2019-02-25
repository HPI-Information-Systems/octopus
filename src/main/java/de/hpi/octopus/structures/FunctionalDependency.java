package de.hpi.octopus.structures;

import java.util.BitSet;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter @AllArgsConstructor
public class FunctionalDependency implements Comparable<FunctionalDependency> {

	private final BitSet lhs;
	private final int rhs;
	
	@Override
	public int compareTo(FunctionalDependency other) {
		int compare = this.rhs - other.getRhs();
		if (compare == 0)
			compare = other.getLhs().cardinality() - this.lhs.cardinality(); // Sort descendingly by lhs length to work on large lhss first; that is more efficient when updating the positive cover!
		return compare;
	}
}
