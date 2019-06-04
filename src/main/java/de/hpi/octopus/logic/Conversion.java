package de.hpi.octopus.logic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import de.hpi.octopus.actors.masters.Profiler.ValidationResultMessage;
import de.hpi.octopus.structures.BitSet;
import de.hpi.octopus.structures.FunctionalDependency;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class Conversion {

	public static List<FunctionalDependency> matches2FDs(BitSet[] matches, int numAttributes) {
		List<FunctionalDependency> fds = new ArrayList<>(matches.length * numAttributes / 2);
		for (int i = 0; i < matches.length; i++) {
			final BitSet lhs = matches[i];
			for (int rhs = 0; rhs < numAttributes; rhs++)
				if (!lhs.get(rhs))
					fds.add(new FunctionalDependency(lhs, rhs));
		}
		return fds;
	}
	
	public static List<FunctionalDependency> matches2FDs(List<BitSet> matches, int numAttributes) {
		List<FunctionalDependency> fds = new ArrayList<>(matches.size() * numAttributes / 2);
		for (int i = 0; i < matches.size(); i++) {
			final BitSet lhs = matches.get(i);
			for (int rhs = 0; rhs < numAttributes; rhs++)
				if (!lhs.get(rhs))
					fds.add(new FunctionalDependency(lhs, rhs));
		}
		return fds;
	}

	public static int[] bitset2Array(BitSet bitSet) {
		int[] array = new int[bitSet.cardinality()];
		for (int trueBit = bitSet.nextSetBit(0), index = 0; trueBit >= 0; trueBit = bitSet.nextSetBit(trueBit + 1), index++)
			array[index] = trueBit;
		return array;
	}
	
	public static ValidationResultMessage fds2ValidationResultMessage(List<FunctionalDependency> invalidFDs) {
		Collections.sort(invalidFDs);
		List<BitSet[]> invalidLhss = new ArrayList<>();
		IntList invalidRhss = new IntArrayList();
		int i = 0;
		while (i < invalidFDs.size()) {
			int j = i + 1;
			while ((j < invalidFDs.size()) && (invalidFDs.get(j).getRhs() == invalidFDs.get(i).getRhs()))
				j++;
			
			BitSet[] currentLhss = new BitSet[j - i];
			for (int k = 0, l = i; l < j; k++, l++)
				currentLhss[k] = invalidFDs.get(l).getLhs();
			
			invalidLhss.add(currentLhss);
			invalidRhss.add(invalidFDs.get(i).getRhs());
			
			i = j;
		}
		
		return new ValidationResultMessage(
				invalidLhss.toArray(new BitSet[invalidRhss.size()][]), 
				invalidRhss.toArray(new int[invalidRhss.size()]));
	}
}
