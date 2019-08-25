package de.hpi.octopus.logic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import de.hpi.octopus.structures.BitSet;
import de.hpi.octopus.structures.FunctionalDependency;

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
	
/*	public static int[] bitset2Array(final BitSet bitSet) {
		//BitSet bitSet = bitSetIn.clone();
		int[] array = new int[bitSet.cardinality()];
		int last = 0;
		try {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for (int trueBit = bitSet.nextSetBit(0), index = 0; trueBit >= 0; trueBit = bitSet.nextSetBit(trueBit + 1), index++) {
				last = trueBit;
				array[index] = trueBit;
			}
		} catch (java.lang.ArrayIndexOutOfBoundsException e) {
			System.out.println(array.length);
			System.out.println(bitSet.cardinality());
			System.out.println(last);
			System.out.println(Arrays.toString(array));
			
			System.out.println(Arrays.toString(bitset2Array(bitSet)));
			System.out.println(Arrays.toString(bitset2Array(bitSet)));
			System.out.println(Arrays.toString(bitset2Array(bitSet)));
			System.out.println(Arrays.toString(bitset2Array(bitSet)));
			System.out.println(Arrays.toString(bitset2Array(bitSet)));
			throw e;
		}
		return array;
	}
*/	
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
