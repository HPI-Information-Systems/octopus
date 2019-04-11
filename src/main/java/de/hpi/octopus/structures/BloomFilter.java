package de.hpi.octopus.structures;

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
	public synchronized boolean add(BitSet element) {
		int code = element.hashCode();
		//int hash = MurmurHash.hash(code); // TODO: really needed or is hashCode() already good enough?
		int bucket = Math.abs(code % this.size);
		
		if (this.bits.get(bucket))
			return false;
		
		this.set(bucket);
		return true;
	}
	
	static final int hash(Object key) {
        int h;
        return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
    }
	
	private synchronized void set(int bucket) {
		this.bits.set(bucket);
	}
}
