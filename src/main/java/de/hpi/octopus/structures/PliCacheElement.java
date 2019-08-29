package de.hpi.octopus.structures;

import lombok.Getter;

@Getter
public class PliCacheElement {

	private int[][] pli;
	private PliCacheElement[] children;
	private int reads;
	private long byteSize;
	
	public PliCacheElement(final int[][] pli, final int numAttributes) {
		this.pli = pli;
		this.children = new PliCacheElement[numAttributes];
		this.reads = 0;
		this.byteSize = this.calculateByteSize();
	}
	
	public PliCacheElement(final int[][] pli, final int numAttributes, final int reads) {
		this(pli, numAttributes);
		this.reads = reads;
	}
	
	public PliCacheElement() {
		this.pli = null;
		this.children = null;
		this.reads = 0;
		this.byteSize = this.calculateByteSize();
	}
	
	public PliCacheElement read() {
		this.reads++;
		return this;
	}
	
	public boolean isBlacklisted() {
		return this.children == null;
	}
	
	public long blacklist() {
		long byteSizeBefore = this.getDeepByteSize();
		this.pli = null;
		this.children = null;
		this.byteSize = this.calculateByteSize();
		return this.byteSize - byteSizeBefore;
	}
	
	public long getDeepByteSize() {
		long deepByteSize = this.byteSize;
		if (this.children != null)
			for (int i = 0; i < this.children.length; i++)
				if (this.children[i] != null)
					deepByteSize += this.children[i].getDeepByteSize();
		return deepByteSize;
	}
	
	public PliCacheElement get(final int[] attributes, final int childIndex) {
		if (childIndex == attributes.length)
			return this.read();
		
		if ((this.children == null) || (this.children[attributes[childIndex]] == null))
			return null;
		
		return this.children[attributes[childIndex]].get(attributes, childIndex + 1);
	}
	
	public long blacklist(final int[] attributes, final int childIndex) {
		// Return without adding the element if this pli was pruned
		if (this.children == null)
			return 0;

		// Let a child blacklist the pli if the pli is not this pli's direct child
		if (childIndex + 1 < attributes.length)
			return this.children[attributes[childIndex]].add(attributes, childIndex + 1, pli);
		
		// Create the blacklist cache entry if it does not exist
		if (this.children[attributes[childIndex]] == null) {
			this.children[attributes[childIndex]] = new PliCacheElement();
			return this.children[attributes[childIndex]].getByteSize();
		}
		
		// Blacklist the child read counter if it exists
		if (!this.children[attributes[childIndex]].isBlacklisted())
			return this.children[attributes[childIndex]].blacklist();
		
		// Do nothing, because the pli was already blacklisted
		return 0;
	}
	
	public long add(final int[] attributes, final int childIndex, final int[][] pli) {
		// Return without adding the pli if this pli was pruned
		if (this.children == null)
			return 0;
		
		// Let a child add the pli if the pli is not this pli's direct child
		if (childIndex + 1 < attributes.length)
			return this.children[attributes[childIndex]].add(attributes, childIndex + 1, pli);
		
		// Create the pli cache entry if it does not exist
		if (this.children[attributes[childIndex]] == null) {
			this.children[attributes[childIndex]] = new PliCacheElement(pli, this.children.length);
			return this.children[attributes[childIndex]].getByteSize();
		}
		
		// Increase read counter if it exists
		if (!this.children[attributes[childIndex]].isBlacklisted()) {
			this.children[attributes[childIndex]].read();
			return 0;
		}
		
		// Do nothing, because the pli was blacklisted
		return 0;
	}
	
	public long prune(int readsThreshold) {
		if (this.reads <= readsThreshold)
			return this.blacklist();
		
		long byteSize = 0;
		for (int i = 0; i < this.children.length; i++)
			if ((this.children[i] != null) && (!this.children[i].isBlacklisted()))
				byteSize += this.children[i].prune(readsThreshold);
		
		return byteSize;
	}
	
	private long calculateByteSize() {
		long pliSize = 4;
		if (this.pli != null) {
			pliSize = sizeIntArrayOf(this.pli.length);
			for (int i = 0; i < this.pli.length; i++)
				pliSize += sizeIntArrayOf(this.pli[i].length);
		}
		
		long childrenSize = 4;
		if (this.children != null) {
			childrenSize = sizeIntArrayOf(this.children.length);
		}
		
		final int readsSize = 4;
		final int byteSizeSize = 8;
		
		return roundToMultipleOfEight(12 + pliSize + childrenSize + readsSize + byteSizeSize) * 10;
	}
	
	private static long sizeIntArrayOf(final int length) {
		final long size = 12 + length * 4;
		return roundToMultipleOfEight(size);
	}
	
	private static long roundToMultipleOfEight(final long size) {
		return (size % 8 == 0) ? size : size + 4;
	}
}
