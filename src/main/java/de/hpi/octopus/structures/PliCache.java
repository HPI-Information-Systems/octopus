package de.hpi.octopus.structures;

public class PliCache {

	private PliCacheElement[] children;
	
	public PliCache(int numAttributes) {
		this.children = new PliCacheElement[numAttributes];
	}

	public boolean isBlacklisted(int... attributes) {
		if (this.children[attributes[0]] == null)
			return false;
		
		return this.children[attributes[0]].isBlacklisted(attributes, 1);
	}
	
	public int[][] get(int... attributes) {
		if (this.children[attributes[0]] == null)
			return null;
		
		return this.children[attributes[0]].get(attributes, 1);
	}

	public void blacklist(final int[] attributes) {
		if (this.children[attributes[0]] == null)
			this.children[attributes[0]] = new PliCacheElement(this.children.length);
		
		this.children[attributes[0]].blacklist(attributes, 1);
	}
	
	public void add(final int[] attributes, final int[][] pli) {
		if (this.children[attributes[0]] == null)
			this.children[attributes[0]] = new PliCacheElement(this.children.length);
		
		this.children[attributes[0]].add(attributes, 1, pli);
	}
}
