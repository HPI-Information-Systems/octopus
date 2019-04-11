package de.hpi.octopus.structures;

import java.util.List;

public class FDTreeElement {

	protected FDTreeElement[] children;	// If this is an inner node: children points to at least one child and lhs = null 
	
	public FDTreeElement() {
		this.children = null;
	}

	public FDTreeElement[] getChildren() {
		return this.children;
	}
	
	public void setChildren(FDTreeElement[] children) {
		this.children = children;
	}
	
	public boolean hasChild(int attribute) {
		return (this.children != null) && (this.children[attribute] != null);
	}
	
	public void addChild(int numAttributes, int attribute, FDTreeElement child) {
		if (this.children == null)
			this.children = new FDTreeElement[numAttributes];
		this.children[attribute] = child;
	}
	
	public void removeChild(int attribute) {
		this.children[attribute] = null;
		for (int i = 0; i < this.children.length; i++)
			if (this.children[i] != null)
				return;
		this.children = null;
	}
	
	public boolean containsLhsOrGeneralization(BitSet lhs, int currentLhsAttr) {
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
		while (currentLhsAttr >= 0) {	
			int nextLhsAttr = lhs.nextSetBit(currentLhsAttr + 1);

			if (this.children[currentLhsAttr] != null)
				this.children[currentLhsAttr].collectLhsAndGeneralizations(lhs, nextLhsAttr, result);
			
			currentLhsAttr = nextLhsAttr;
		}
	}
	
	public boolean trim(int toDepth) {
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
