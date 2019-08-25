package de.hpi.octopus.logic;

import de.hpi.octopus.structures.BitSet;

public class MatchingLogic {

	public static BitSet match(final int[] record1, final int[] record2) {
		BitSet matches = new BitSet(record1.length);
		for (int attribute = 0; attribute < record1.length; attribute++)
			if (isEqual(record1[attribute], record2[attribute]))
				matches.set(attribute);
		return matches;
	}
	
	public static boolean isMatch(final int[] record1, final int[] record2, final int[] attributes) {
		for (int i = 0; i < attributes.length; i++)
			if (!isEqual(record1[attributes[i]], record2[attributes[i]]))
				return false;
		return true;
	}
	
	public static boolean isMatch(final int[] record1, final int[] record2, final int attribute) {
		return isEqual(record1[attribute], record2[attribute]);
	}

	public static boolean isEqual(final int value1, final int value2) {
		return (value1 == value2) && (value1 != -1);
	}
}
