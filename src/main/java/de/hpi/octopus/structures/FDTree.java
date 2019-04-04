package de.hpi.octopus.structures;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

public class FDTree extends FDTreeElement implements FDStore {

	protected int depth;
	protected int numAttributes;
	
	protected FDTreeLeaf first;
	protected FDTreeLeaf last;
	
	public FDTree(int numAttributes, int rhsAttribute) {
		this.depth = 1;
		this.numAttributes = numAttributes;
		
		this.addMostGeneralDependencies(rhsAttribute);
	}

	protected void addMostGeneralDependencies(int rhsAttribute) {
		this.children = new FDTreeElement[this.numAttributes];
		BitSet lhs = new BitSet(this.numAttributes);
		for (int i = 0; i < this.numAttributes; i++) {
			if (i == rhsAttribute)
				continue;
			
			lhs.set(i);
			this.addLhs(lhs);
			lhs.clear(i);
		}
	}
	
	@Override
	public int getNumAttributes() {
		return this.numAttributes;
	}
	
	@Override
	public void removeChild(int attribute) {
		this.children[attribute] = null;
	//	for (FDTreeElement child : this.children)
	//		if (child != null)
	//			return;
	//	this.children = null; // Never set the children of the root to null. In this way, root is never considered to be a valid FD.
	}
	
	@Override
	public boolean containsLhs(BitSet lhs) {
		if (lhs.cardinality() == 0)
			return false;
		
		FDTreeElement element = this;
		for (int current = lhs.nextSetBit(0); current >= 0; current = lhs.nextSetBit(current + 1)) {
			element = element.children[current];
			if (element == null)
				return false;
		}
		
		if (element instanceof FDTreeLeaf)
			return true;
		
		return false;
	}
	
	@Override
	public boolean containsLhsOrGeneralization(BitSet lhs) {
		int nextLhsAttr = lhs.nextSetBit(0);
		return this.containsLhsOrGeneralization(lhs, nextLhsAttr);
	}

	@Override
	public List<BitSet> getLhsAndGeneralizations(BitSet lhs) {
		List<BitSet> result = new ArrayList<>();
		int nextLhsAttr = lhs.nextSetBit(0);
		this.collectLhsAndGeneralizations(lhs, nextLhsAttr, result);
		return result;
	}
	
	@Override
	public void addLhs(BitSet lhs) {
		// Add the elements for the lhs
		FDTreeElement element = this;
		int lhsSize = 0;
		int attribute = lhs.nextSetBit(0);
		for (int child = lhs.nextSetBit(attribute + 1); child >= 0; child = lhs.nextSetBit(child + 1)) {
			if (!element.hasChild(attribute))
				element.addChild(this.numAttributes, attribute, new FDTreeElement());
			
			element = element.getChildren()[attribute];
			lhsSize++;
			attribute = child;
		}
		
		// Add the last element as a leaf that indicates an FD
		element.addChild(this.numAttributes, attribute, new FDTreeLeaf(lhs, null, this.last));
		
		FDTreeLeaf leaf = (FDTreeLeaf) element.getChildren()[attribute];
		
		// Add the last element to the linked list of unannounced leaf elements
		if (this.first == null) {
			this.first = leaf;
		}
		else {
			this.last.setNext(leaf);
		}
		this.last = leaf;
		
		// Adjust the depth of this tree
		this.depth = Math.max(this.depth, lhsSize);
	}

	@Override
	public void removeLhs(BitSet lhs) {
		// Unsafe fast-remove: 
		// - if lhs does not exist, we get a NullPointerException
		// - if only a specialization exists, we get a ClassCastException
		
		int lhsCardinality = lhs.cardinality();

		// Collect all FDTreeElements of the lhs from this FDTree
		int[] lhsAttributes = new int[lhsCardinality + 1];
		FDTreeElement[] lhsElements = new FDTreeElement[lhsCardinality + 1];
		lhsElements[0] = this;
		for (int attribute = lhs.nextSetBit(0), i = 1; attribute >= 0; attribute = lhs.nextSetBit(attribute + 1), i++) {
			lhsAttributes[i] = attribute;
			lhsElements[i] = lhsElements[i - 1].getChildren()[attribute];
		}
		
		// Remove the leaf element from the linked list of unannounced leaf elements		
		FDTreeLeaf leaf = (FDTreeLeaf) lhsElements[lhsCardinality];
		if (this.first == leaf)
			this.first = leaf.getNext();
		if (this.last == leaf)
			this.last = leaf.getPrevious();
		if (leaf.getPrevious() != null)
			leaf.getPrevious().setNext(leaf.getNext());
		if (leaf.getNext() != null)
			leaf.getNext().setPrevious(leaf.getPrevious());
		
		// Remove FDTreeElements for the given lhs path
		for (int i = lhsCardinality; i > 0; i--) {
			if (lhsElements[i].getChildren() != null)
				return;
			lhsElements[i - 1].removeChild(lhsAttributes[i]);
		}
	}

	@Override
	public BitSet[] announceLhss(int amount) {
		// Collect "amount"-many lhss from the head of the leaf elements list
		BitSet[] lhss = new BitSet[amount];
		for (int i = 0; i < amount; i++) {
			if (this.first == null) {
				lhss = Arrays.copyOf(lhss, i);
				break;
			}
			
			lhss[i] = this.first.getLhs();
			this.first = this.first.getNext();
		}
		
		// Disconnect the collected lhss sequence from the linked list
		if (this.first == null) {
			this.last = null;
		}
		else {
			this.first.getPrevious().setNext(null);
			this.first.setPrevious(null);
		}
		
		return lhss;
	}
	
	@Override
	public void trimTree(int toDepth) {
		if (this.depth <= toDepth)
			return;
		
		this.depth = toDepth;
		
		// Reset the first and last pointer of the leaf list (because the recursive trimming cannot do that)
		while ((this.first != null) && (this.first.getLhs().cardinality() > toDepth))
			this.first = this.first.getNext();
		if ((this.first != null) && (this.first.getPrevious() != null)) {
			this.first.getPrevious().setNext(null);
			this.first.setPrevious(null);
		}
		
		while ((this.last != null) && (this.last.getLhs().cardinality() > toDepth))
			this.last = this.last.getPrevious();
		if ((this.last != null) && (this.last.getNext() != null)) {
			this.last.getNext().setPrevious(null);
			this.last.setNext(null);
		}
		
		// Trim leaf elements from the tree recursively. They remove themselves from the leaf list
		this.trim(toDepth);
	}
}
