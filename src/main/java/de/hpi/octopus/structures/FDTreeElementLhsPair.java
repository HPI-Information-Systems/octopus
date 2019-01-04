package de.hpi.octopus.structures;

import java.util.BitSet;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter @AllArgsConstructor
public class FDTreeElementLhsPair {
	
	private final FDTreeElement element;
	private final BitSet lhs;
}

