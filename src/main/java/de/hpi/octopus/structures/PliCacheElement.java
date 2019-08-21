package de.hpi.octopus.structures;

public class PliCacheElement {

	private int[][] pli;
	private boolean blacklisted;
	private PliCacheElement[] children;
	
	public PliCacheElement(final int numAttributes) {
		this.children = new PliCacheElement[numAttributes];
		this.blacklisted = false;
	}
	
	public boolean isBlacklisted(final int[] attributes, final int childIndex) {
		if (childIndex == attributes.length)
			return this.blacklisted;
		
		if ((this.children == null) || (this.children[attributes[childIndex]] == null))
			return this.blacklisted;
		
		return this.children[attributes[childIndex]].isBlacklisted(attributes, childIndex + 1);
	}
	
	public int[][] get(final int[] attributes, final int childIndex) {
		if (childIndex == attributes.length)
			return this.pli;
		
		if ((this.children == null) || (this.children[attributes[childIndex]] == null))
			return null;
		
		return this.children[attributes[childIndex]].get(attributes, childIndex + 1);
	}
	
	public void blacklist(final int[] attributes, final int childIndex) {
		if (childIndex == attributes.length) {
			this.blacklisted = true;
			return;
		}
		
		if (this.children[attributes[childIndex]] == null)
			this.children[attributes[childIndex]] = new PliCacheElement(this.children.length);
		
		this.children[attributes[childIndex]].blacklist(attributes, childIndex + 1);
	}
	
	public void add(final int[] attributes, final int childIndex, final int[][] pli) {
		if (childIndex == attributes.length) {
			this.pli = pli;
			this.blacklisted = false;
			return;
		}
		
		if (this.children[attributes[childIndex]] == null)
			this.children[attributes[childIndex]] = new PliCacheElement(this.children.length);
		
		this.children[attributes[childIndex]].add(attributes, childIndex + 1, pli);
	}
}