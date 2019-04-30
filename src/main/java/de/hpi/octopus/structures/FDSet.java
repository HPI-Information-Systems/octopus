package de.hpi.octopus.structures;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class FDSet implements FDStore {

	private Set<BitSet> lhss;
	private LinkedList<BitSet> unannounced;
	private int numAttributes;
	
	public FDSet(int numAttributes, int rhsAttribute) {
		this.lhss = new HashSet<>(numAttributes);
		this.unannounced = new LinkedList<>();
		this.numAttributes = numAttributes;
		
		this.addMostGeneralDependencies(rhsAttribute);
	}
	
	@Override
	public int getNumAttributes() {
		return this.numAttributes;
	}
	
	protected void addMostGeneralDependencies(int rhsAttribute) {
		for (int i = 0; i < this.numAttributes; i++) {
			if (i != rhsAttribute) {
				BitSet lhs = new BitSet(this.numAttributes);
				lhs.set(i);
				this.addLhs(lhs);
			}
		}
	}
	
	@Override
	public void addLhs(BitSet lhs) {
		BitSet clone = lhs.clone();
		this.lhss.add(clone);
		this.unannounced.add(clone);
	}
	
	@Override
	public void removeLhs(BitSet lhs) {
		this.lhss.remove(lhs);
		this.unannounced.remove(lhs);
	}
	
	@Override
	public BitSet[] announceLhss(int amount) {
		BitSet[] announcees = new BitSet[Math.min(amount, this.unannounced.size())];
		for (int i = 0; i < announcees.length; i++)
			announcees[i] = this.unannounced.removeFirst();
		return announcees;
	}
	
	@Override
	public List<BitSet> getLhsAndGeneralizations(BitSet lhs) {
		List<BitSet> generalizations = new ArrayList<>();
		for (BitSet generalization : this.lhss)
			if (this.subsetOrEqual(generalization, lhs))
				generalizations.add(generalization);
		return generalizations;
	}
	
	@Override
	public boolean containsLhs(BitSet lhs) {
		for (BitSet stored : this.lhss)
			if (stored.equals(lhs))
				return true;
		return false;
	}
	
	@Override
	public boolean containsLhsOrGeneralization(BitSet lhs) {
		for (BitSet generalization : this.lhss)
			if (this.subsetOrEqual(generalization, lhs))
				return true;
		return false;
	}

	private boolean subsetOrEqual(BitSet subset, BitSet other) {
		if (subset.cardinality() > other.cardinality())
			return false;
		
		for (int i = subset.nextSetBit(0); i >= 0; i = subset.nextSetBit(i + 1)) {
			if (!other.get(i))
				return false;
		}
		
		return true;
	}

	@Override
	public void trimTree(int toDepth) {
		List<BitSet> toRemove = new ArrayList<>();
		for (BitSet lhs : this.lhss) {
			if (lhs.cardinality() > toDepth) {
				toRemove.add(lhs);
			}
		}
		this.lhss.removeAll(toRemove);
		this.unannounced.removeAll(toRemove);
	}
}
