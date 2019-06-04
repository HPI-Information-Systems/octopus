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
	public synchronized void add(BloomFilter other) {
		this.bits.or(other.getBits());
	}
	
	/**
	 * Add the element to the BloomFilter. The manipulation operation is synchronized to avoid concurrent modification exceptions.
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
	 * Adds all the elements to the BloomFilter. The manipulation operation is synchronized to avoid concurrent modification exceptions.
	 * @param element the element to be added
	 * @return true if the element was added; false if it existed already
	 */
	public void addAll(BitSet[] elements) {
		int[] buckets = new int[elements.length];
		for (int i = 0; i < elements.length; i++) {
			int code = elements[i].hashCode();
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

	private synchronized void set(int bucket) {
		this.bits.set(bucket);
	}
	
	private synchronized void setAll(int[] buckets) {
		for (int i = 0; i < buckets.length; i++)
			this.bits.set(buckets[i]);
	}
}
