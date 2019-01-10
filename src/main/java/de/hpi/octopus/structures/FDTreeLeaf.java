package de.hpi.octopus.structures;

import java.util.BitSet;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

/**
 * Leaf nodes indicate FD candidates and hold some additional data.
 */
@Getter @Setter
public class FDTreeLeaf extends FDTreeElement {
	
	private final BitSet lhs;		// The lhs of this candidate
	private FDTreeLeaf next;		// If this is an unannounced candidate, next points to the next unannounced candidate
	private FDTreeLeaf previous;	// If this is an unannounced candidate, previous points to the previous unannounced candidate
	
	public FDTreeLeaf(BitSet lhs, FDTreeLeaf next, FDTreeLeaf previous) {
		this.lhs = lhs;
		this.next = next;
		this.previous = previous;
	}
	
	@Override
	public boolean containsLhsOrGeneralization(BitSet lhs, int currentLhsAttr) {
		return true;
	}
	
	@Override
	public void collectLhsAndGeneralizations(BitSet lhs, int currentLhsAttr, List<BitSet> result) {
		result.add(this.lhs);
	}
	
	@Override
	public boolean trim(int toDepth) {
		if (this.lhs.cardinality() > toDepth) {
			if (this.getPrevious() != null)
				this.getPrevious().setNext(this.getNext());
			if (this.getNext() != null)
				this.getNext().setPrevious(this.getPrevious());
			return true;
		}
		return false;
	}
}

