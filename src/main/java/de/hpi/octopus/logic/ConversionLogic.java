package de.hpi.octopus.logic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import de.hpi.octopus.actors.masters.Profiler.SamplingResultMessage;
import de.hpi.octopus.actors.masters.Profiler.ValidationResultMessage;
import de.hpi.octopus.structures.BitSet;
import de.hpi.octopus.structures.FunctionalDependency;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class ConversionLogic {

	public static List<FunctionalDependency> matches2FDs(final BitSet[] matches, final int numAttributes, boolean[] finishedRhsAttributes) {
		List<FunctionalDependency> fds = new ArrayList<>(matches.length * numAttributes / 2);
		for (int i = 0; i < matches.length; i++) {
			final BitSet lhs = matches[i];
			for (int rhs = 0; rhs < numAttributes; rhs++)
				if (!lhs.get(rhs) && !finishedRhsAttributes[rhs])
					fds.add(new FunctionalDependency(lhs, rhs));
		}
		return fds;
	}
	
	public static List<FunctionalDependency> matches2FDs(final List<BitSet> matches, final int numAttributes, boolean[] finishedRhsAttributes) {
		List<FunctionalDependency> fds = new ArrayList<>(matches.size() * numAttributes / 2);
		for (int i = 0; i < matches.size(); i++) {
			final BitSet lhs = matches.get(i);
			for (int rhs = 0; rhs < numAttributes; rhs++)
				if (!lhs.get(rhs) && !finishedRhsAttributes[rhs])
					fds.add(new FunctionalDependency(lhs, rhs));
		}
		return fds;
	}

	public static int[] bitset2Array(final BitSet bitSet) {
		int[] array = new int[bitSet.cardinality()];
		for (int trueBit = bitSet.nextSetBit(0), index = 0; trueBit >= 0; trueBit = bitSet.nextSetBit(trueBit + 1), index++)
			array[index] = trueBit;
		return array;
	}
	
	public static ValidationResultMessage fds2ValidationResultMessage(List<FunctionalDependency> invalidFDs, int rhs, int numCandidates) {
		Collections.sort(invalidFDs);
		
		List<BitSet[]> invalidLhss = new ArrayList<>();
		IntList invalidRhss = new IntArrayList();
		int i = 0;
		while (i < invalidFDs.size()) {
			int currentRhs = invalidFDs.get(i).getRhs();
			
			int j = i + 1;
			while ((j < invalidFDs.size()) && (invalidFDs.get(j).getRhs() == invalidFDs.get(i).getRhs()))
				j++;
			
			BitSet[] currentLhss = new BitSet[j - i];
			for (int k = 0, l = i; l < j; k++, l++)
				currentLhss[k] = invalidFDs.get(l).getLhs();
			
			invalidLhss.add(currentLhss);
			invalidRhss.add(currentRhs);
			
			i = j;
		}
		
		return new ValidationResultMessage(
				invalidLhss.toArray(new BitSet[invalidRhss.size()][]), 
				invalidRhss.toArray(new int[invalidRhss.size()]),
				numCandidates);
	}
	
	public static SamplingResultMessage fds2SamplingResultMessage(List<FunctionalDependency> invalidFDs, int numComparisons, int numMatches) {
		Collections.sort(invalidFDs);
		
		List<BitSet[]> invalidLhss = new ArrayList<>();
		IntList invalidRhss = new IntArrayList();
		int i = 0;
		while (i < invalidFDs.size()) {
			int currentRhs = invalidFDs.get(i).getRhs();
			
			int j = i + 1;
			while ((j < invalidFDs.size()) && (invalidFDs.get(j).getRhs() == invalidFDs.get(i).getRhs()))
				j++;
			
			BitSet[] currentLhss = new BitSet[j - i];
			for (int k = 0, l = i; l < j; k++, l++)
				currentLhss[k] = invalidFDs.get(l).getLhs();
			
			invalidLhss.add(currentLhss);
			invalidRhss.add(currentRhs);
			
			i = j;
		}
		
		return new SamplingResultMessage(
				invalidLhss.toArray(new BitSet[invalidRhss.size()][]), 
				invalidRhss.toArray(new int[invalidRhss.size()]),
				numComparisons,
				numMatches);
	}

	public static String pli2String(int[][] pli) {
		StringBuffer buffer = new StringBuffer("[");
		int maxC = 5;
		for (int[] cluster : pli) {
			maxC--;
			
			int maxR = 5;
			buffer.append("[");
			for (int record : cluster) {
				maxR--;
				
				buffer.append(" " + record);
				
				if (maxR == 0) {
					buffer.append("...");
					break;
				}
			}
			buffer.append(" ]");
			
			if (maxC == 0) {
				buffer.append("...");
				break;
			}
		}
		
		buffer.append("]");
		return buffer.toString();
	}
	
	public static String record2String(int[] record) {
		StringBuffer buffer = new StringBuffer("[ ");
		for (int i = 0; i < record.length; i++) {
			buffer.append(record[i] + " ");
		}
		buffer.append("]");
		return buffer.toString();
	}
	
}
