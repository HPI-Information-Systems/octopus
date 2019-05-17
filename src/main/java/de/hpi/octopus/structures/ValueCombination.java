package de.hpi.octopus.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ValueCombination {

	private final int[] record;
	private final int[] attributes;
	
	public boolean isUnique() {
		for (int i = 0; i < this.attributes.length; i++)
			if (this.record[this.attributes[i]] == -1)
				return true;
		return false;
	}

	@Override
	public int hashCode() {
		return MurmurHash.hashBy(this.record, this.attributes);
	}

	@Override
	public boolean equals(Object obj) {
		// We should never compare ValueCombinations to something else; hence, it is indented that this cast throws an exception if we try
		final ValueCombination other = (ValueCombination) obj; 
		
		// We should never compare incompatible ValueCombinations; hence, it is indented that this iteration might fail if the ValueCombinations have different attributes
		for (int i = 0; i < this.attributes.length; i++)
			if (this.record[this.attributes[i]] != other.record[this.attributes[i]])
				return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("[");
		for (int i = 0; i < this.attributes.length; i++) {
			builder.append(this.record[this.attributes[i]]);
			if (i + 1 < this.attributes.length)
				builder.append(", ");
		}
		builder.append("]");
		return builder.toString();
	}
}
