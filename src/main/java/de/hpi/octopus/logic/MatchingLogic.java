package de.hpi.octopus.logic;

public class MatchingLogic {

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
