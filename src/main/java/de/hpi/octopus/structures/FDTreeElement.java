package de.hpi.octopus.structures;

import java.util.BitSet;
import java.util.List;

import lombok.AllArgsConstructor;

public class FDTreeElement {

	/**
	 * Leaf nodes indicate FD candidates and hold some additional data. 
	 * This data is encapsulated in Leaf objects so that non-leaf nodes do not need to hold all these extra attributes.
	 */
	@AllArgsConstructor
	private class Leaf {
		public final BitSet lhs;		// The lhs of this candidate
		public FDTreeElement next;		// If this is a leaf node and unannounced candidate: next points to the next unannounced candidate
		public FDTreeElement previous;	// If this is a leaf node and unannounced candidate: previous points to the previous unannounced candidate
	}
	
	protected Leaf leaf; 				// If this is a leaf node: leaf indicates the lhs of an FD candidate and children = null
	protected FDTreeElement[] children;	// If this is an inner node: children points to at least one child and lhs = null 
	
	public FDTreeElement() {
		this.leaf = null;
		this.children = null;
	}

	public void makeLeaf(BitSet lhs, FDTreeElement next, FDTreeElement previous) {
		this.leaf = new Leaf(lhs, next, previous);
	}
	
	public BitSet getLhs() {
		return this.leaf.lhs;
	}
	
	public FDTreeElement getNext() {
		return this.leaf.next;
	}
	
	public void setNext(FDTreeElement next) {
		this.leaf.next = next;
	}

	public FDTreeElement getPrevious() {
		return this.leaf.previous;
	}
	
	public void setPrevious(FDTreeElement previous) {
		this.leaf.previous = previous;
	}
	
	public FDTreeElement[] getChildren() {
		return this.children;
	}
	
	public void setChildren(FDTreeElement[] children) {
		this.children = children;
	}
	
	public void removeChild(int attribute) {
		this.children[attribute] = null;
		for (int i = 0; i < this.children.length; i++)
			if (this.children[i] != null)
				return;
		this.children = null;
	}
	
	public boolean containsLhsOrGeneralization(BitSet lhs, int currentLhsAttr) {
		if (this.leaf != null)
			return true;
		
		while (currentLhsAttr >= 0) {
			int nextLhsAttr = lhs.nextSetBit(currentLhsAttr + 1);
			
			if (this.children[currentLhsAttr] != null)
				if (this.children[currentLhsAttr].containsLhsOrGeneralization(lhs, nextLhsAttr))
					return true;
			
			currentLhsAttr = nextLhsAttr;
		}
		return false;
	}
	
	public void collectLhsAndGeneralizations(BitSet lhs, int currentLhsAttr, List<BitSet> result) {
		if (this.leaf != null) {
			result.add(this.leaf.lhs);
			return;
		}
		
		while (currentLhsAttr >= 0) {
			int nextLhsAttr = lhs.nextSetBit(currentLhsAttr + 1);
			
			if (this.children[currentLhsAttr] != null)
				this.children[currentLhsAttr].collectLhsAndGeneralizations(lhs, nextLhsAttr, result);
			
			currentLhsAttr = nextLhsAttr;
		}
	}
	
	public boolean trim(int toDepth) {
		if (this.leaf != null) {
			if (this.leaf.lhs.cardinality() > toDepth) {
				if (this.getPrevious() != null)
					this.getPrevious().setNext(this.getNext());
				if (this.getNext() != null)
					this.getNext().setPrevious(this.getPrevious());
				return true;
			}
			return false;
		}
		
		for (int i = 0; i < this.children.length; i++)
			if ((this.children[i] != null) && this.children[i].trim(toDepth))
				this.children[i] = null;
		
		for (int i = 0; i < this.children.length; i++)
			if (this.children[i] != null)
				return false;
		return true;
	}
/*	
	public void collectLevel(int levelsToHopOver, List<BitSet> levelElements) {
		if ((levelsToHopOver == 0) && (this.children == null)) {
			levelElements.add(this.lhs);
			return;
		}

		for (FDTreeElement child : this.children)
			if (child != null)
				child.collectLevel(levelsToHopOver - 1, levelElements);
	}
*/	
/*	public boolean removeLhs(BitSet lhs, int currentLhsAttr) {
		int nextLhsAttr = lhs.nextSetBit(currentLhsAttr + 1);
		
		if (nextLhsAttr < 0)
			return true;
		
		if (this.children[currentLhsAttr].removeLhs(lhs, nextLhsAttr))
			this.children[currentLhsAttr] = null;
		
		for (FDTreeElement child : this.children)
			if (child != null)
				return true;
		return false;
	}
*/
}
