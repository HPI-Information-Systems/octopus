package de.hpi.octopus.structures;

import java.util.List;

public interface FDStore {

	public int getNumAttributes();
	
	public boolean containsLhs(BitSet lhs);
	
	public boolean containsLhsOrGeneralization(BitSet lhs);

	public List<BitSet> getLhsAndGeneralizations(BitSet lhs);
	
	public void addLhs(BitSet lhs);

	public void removeLhs(BitSet lhs);
	
	public BitSet[] announceLhss(int amount);
	
	public void trimTree(int toDepth);
}
