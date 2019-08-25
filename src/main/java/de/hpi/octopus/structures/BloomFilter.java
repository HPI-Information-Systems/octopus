package de.hpi.octopus.structures;

import java.util.List;

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
	 * Merge all elements of the other BloomFilter into this BloomFilter.
	 * @param other the other BloomFilter whose elements are to be added
	 */
	public void merge(BloomFilter other) {
		this.bits.or(other.getBits());
	}
	
	/**
	 * Add the element to the BloomFilter.
	 * @param element the element to be added
	 * @return true if the element was added; false if it existed already
	 */
	public boolean add(BitSet element) {
		int code = element.hashCode();
		//int hash = MurmurHash.hash(code); // TODO: really needed or is hashCode() already good enough?
		int bucket = Math.abs(code % this.size);
		
		if (this.bits.get(bucket))
			return false;
		
		this.set(bucket);
		return true;
	}
	
	/**
	 * Add the element to the BloomFilter. This method is synchronized for concurrent calls.
	 * @param element the element to be added
	 * @return true if the element was added; false if it existed already
	 */
	public synchronized boolean addSynchronized(BitSet element) {
		int code = element.hashCode();
		//int hash = MurmurHash.hash(code); // TODO: really needed or is hashCode() already good enough?
		int bucket = Math.abs(code % this.size);
		
		if (this.bits.get(bucket))
			return false;
		
		this.set(bucket);
		return true;
	}
	
	/**
	 * Adds all the elements to the BloomFilter.
	 * @param element the element to be added
	 * @return true if the element was added; false if it existed already
	 */
	public void addAll(List<BitSet> elements) {
		int[] buckets = new int[elements.size()];
		for (int i = 0; i < elements.size(); i++) {
			int code = elements.get(i).hashCode();
			//int hash = MurmurHash.hash(code); // TODO: really needed or is hashCode() already good enough?
			buckets[i] = Math.abs(code % this.size);
		}
		this.setAll(buckets);
	}
	
	/**
	 * Test if this BloomFilter contains the element.
	 * @param element the element to be tested
	 */
	public boolean contains(BitSet element) {
		int code = element.hashCode();
		//int hash = MurmurHash.hash(code); // TODO: really needed or is hashCode() already good enough?
		int bucket = Math.abs(code % this.size);
		
		return this.bits.get(bucket);
	}
	
//	static final int hash(Object key) {
//        int h;
//        return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
//    }

	private void set(int bucket) {
		this.bits.set(bucket);
	}
	
	private void setAll(int[] buckets) {
		for (int i = 0; i < buckets.length; i++)
			this.bits.set(buckets[i]);
	}
}
