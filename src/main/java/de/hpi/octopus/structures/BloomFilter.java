package de.hpi.octopus.structures;

import java.util.BitSet;

public class BloomFilter {

	public static int DEFAULT_SIZE = 83886080; // 10 MB
	
	private final BitSet bits;
	private final int size;

	public BloomFilter() {
		this.bits = new BitSet(DEFAULT_SIZE);
		this.size = DEFAULT_SIZE;
	}
	
	/**
	 * Retrieves the BitSet that stores the elements of this BloomFilter
	 * @return the BitSet that stores the elements of this BloomFilter
	 */
	public BitSet getBits() {
		return this.bits;
	}
	
	/**
	 * Add all elements of the other BloomFilter to this BloomFilter.
	 * @param other the other BloomFilter whose elements are to be added
	 */
	public void add(BloomFilter other) {
		this.bits.or(other.getBits());
	}
	
	/**
	 * Add the element to the BloomFilter. The manipulation operation is synchronized to avoid concurrent modification exceptions.
	 * @param element the element to be added
	 * @return true if the element was added; false if it existed already
	 */
	public boolean add(BitSet element) {
		int code = element.hashCode();
		int hash = this.hash(code);
		int bucket = Math.abs(hash % this.size);
		
		if (this.bits.get(bucket))
			return false;
		
		this.set(bucket);
		return true;
	}
	
	private synchronized void set(int bucket) {
		this.bits.set(bucket);
	}

	// MurmurHash
	private int hash(int data) {
		int m = 0x5bd1e995;
		int r = 24;

		int h = 0;

		int k = (int) data * m;
		k ^= k >>> r;
		h ^= k * m;

		k = (int) (data >> 32) * m;
		k ^= k >>> r;
		h *= m;
		h ^= k * m;

		h ^= h >>> 13;
		h *= m;
		h ^= h >>> 15;

		return h;
	}
}
