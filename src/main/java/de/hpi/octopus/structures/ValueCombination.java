package de.hpi.octopus.structures;

import lombok.Getter;

@Getter
public class ValueCombination {

	private final int[] values;
	
	public ValueCombination(final int[] values) {
		this.values = values;
	}
	
	public int size() {
		return this.values.length;
	}
	
	public boolean isUnique() {
		for (int i = 0; i < this.values.length; i++)
			if (this.values[i] == -1)
				return true;
		return false;
	}

	@Override
	public int hashCode() {
		return MurmurHash.hash(this.values);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (!(obj instanceof ValueCombination))
			return false;
		final ValueCombination other = (ValueCombination) obj;
		
		if (this.size() != other.size())
			return false;
		
		final int[] values1 = this.getValues();
		final int[] values2 = other.getValues();
		
		for (int i = 0; i < this.size(); i++)
			if (values1[i] != values2[i])
				return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("[");
		for (int i = 0; i < this.size(); i++) {
			builder.append(this.values[i]);
			if (i + 1 < this.size())
				builder.append(", ");
		}
		builder.append("]");
		return builder.toString();
	}
}
