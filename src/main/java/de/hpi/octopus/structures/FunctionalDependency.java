package de.hpi.octopus.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter @AllArgsConstructor
public class FunctionalDependency implements Comparable<FunctionalDependency> {

	public static String toString(BitSet lhs, int rhs, Dataset dataset) {
		StringBuffer buffer = new StringBuffer("[");
		for (int attribute = lhs.nextSetBit(0); attribute >= 0; attribute = lhs.nextSetBit(attribute + 1)) {
			buffer.append(dataset.getColumnNames()[attribute]);
			buffer.append(", ");
		}
		if (buffer.length() > 2)
			buffer.delete(buffer.length() - 2 , buffer.length());
		buffer.append("] --> ");
		buffer.append(dataset.getColumnNames()[rhs]);
		buffer.append("\r\n");

		return buffer.toString();
	}
	
	private final BitSet lhs;
	private final int rhs;
	
	@Override
	public int compareTo(FunctionalDependency other) {
		int compare = this.rhs - other.getRhs();
		if (compare == 0)
			compare = other.getLhs().cardinality() - this.lhs.cardinality(); // Sort descendingly by lhs length to work on large lhss first; that is more efficient when updating the positive cover!
		return compare;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof FunctionalDependency))
			return false;
		if (this == obj)
			return true;

		FunctionalDependency fd = (FunctionalDependency) obj;

		if ((this.rhs == fd.getRhs()) && this.lhs.equals(fd.getLhs()))
			return true;
		return false;
	}
	
	@Override
	public int hashCode() {
		return MurmurHash.hash(this.rhs) + this.lhs.hashCode();
	}
}
