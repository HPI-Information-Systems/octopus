package de.hpi.octopus.structures;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BloomFilterTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void test() {
		System.out.println("Start testing BloomFilter");
		
		BloomFilter filter = new BloomFilter();
		
		BitSet bitset = new BitSet(20);
		assertTrue(filter.add(bitset));
		assertFalse(filter.add(bitset));
		
		bitset.set(3);
		assertTrue(filter.add(bitset));
		assertFalse(filter.add(bitset));
		
		bitset.set(7);
		assertTrue(filter.add(bitset));
		assertFalse(filter.add(bitset));
		
		bitset.set(8);
		assertTrue(filter.add(bitset));
		assertFalse(filter.add(bitset));
		
		bitset.set(1);
		assertTrue(filter.add(bitset));
		assertFalse(filter.add(bitset));
		
		bitset.set(2);
		assertTrue(filter.add(bitset));
		assertFalse(filter.add(bitset));
		
		bitset.set(4);
		assertTrue(filter.add(bitset));
		assertFalse(filter.add(bitset));
		
		bitset.set(5);
		assertTrue(filter.add(bitset));
		assertFalse(filter.add(bitset));
		
		bitset.set(6);
		assertTrue(filter.add(bitset));
		assertFalse(filter.add(bitset));
		
		bitset.set(9);
		assertTrue(filter.add(bitset));
		assertFalse(filter.add(bitset));
		
		bitset.set(0);
		assertTrue(filter.add(bitset));
		assertFalse(filter.add(bitset));
	}
	
}
