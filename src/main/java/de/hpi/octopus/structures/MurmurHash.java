package de.hpi.octopus.structures;

public class MurmurHash {

	public static int hash(int[] data) {
		int hash = 0;
		for (int i = 0; i < data.length; i++)
			hash += hash(data[i]);
		return hash;
	}
	
	public static int hash(int data) {
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
