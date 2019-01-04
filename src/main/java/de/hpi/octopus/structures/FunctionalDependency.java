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
		return this.rhs - other.getRhs();
	}
}
