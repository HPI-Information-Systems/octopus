package de.hpi.octopus.structures;

import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BitSetTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testPerformance() {
		int size = 2000;
		BitSet oBitset = new BitSet(size);
		java.util.BitSet jBitset = new java.util.BitSet(size);
		
		long t = System.currentTimeMillis();
		for (int i = 2; i < size; i += 2)
			oBitset.set(i);
		System.out.println("oBitset set: " + (System.currentTimeMillis() - t));
		
		t = System.currentTimeMillis();
		for (int i = 2; i < size; i += 2)
			jBitset.set(i);
		System.out.println("jBitset set: " + (System.currentTimeMillis() - t));
		
		assertTrue(oBitset.cardinality() == jBitset.cardinality());
		assertTrue(oBitset.nextSetBit(65) == jBitset.nextSetBit(65));
		
		t = System.currentTimeMillis();
		int oCount = 0;
		for (int i = 0; i < size; i++)
			oCount += oBitset.cardinality();
		System.out.println("oBitset cardinality: " + (System.currentTimeMillis() - t));
		
		t = System.currentTimeMillis();
		int jCount = 0;
		for (int i = 0; i < size; i++)
			jCount += jBitset.cardinality();
		System.out.println("jBitset cardinality: " + (System.currentTimeMillis() - t));

		assertTrue(oCount == jCount);
		
		t = System.currentTimeMillis();
		oCount = 0;
		for (int i = 0; i < size; i++)
			oCount -= oBitset.nextSetBit(i);
		System.out.println("oBitset nextSetBit: " + (System.currentTimeMillis() - t));
		
		t = System.currentTimeMillis();
		jCount = 0;
		for (int i = 0; i < size; i++)
			jCount -= jBitset.nextSetBit(i);
		System.out.println("jBitset nextSetBit: " + (System.currentTimeMillis() - t));
		
		assertTrue(oCount == jCount);
		
		System.out.println("oBitset serial size: " + oBitset.toBinary().length);
		System.out.println("jBitset serial size: " + jBitset.toByteArray().length);
		
		t = System.currentTimeMillis();
		oCount = 0;
		for (int i = 0; i < size * 10; i++)
			oCount -= oBitset.toBinary().length;
		System.out.println("oBitset toBytes: " + (System.currentTimeMillis() - t));
		
		t = System.currentTimeMillis();
		jCount = 0;
		for (int i = 0; i < size * 10; i++)
			jCount -= jBitset.toByteArray().length;
		System.out.println("jBitset toBytes: " + (System.currentTimeMillis() - t));
		
		t = System.currentTimeMillis();
		oCount = 0;
		byte[] bytes = new byte[4 + oBitset.physicalLength() * 8];
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		for (int i = 0; i < size * 10; i++) {
			oBitset.toBinary(buffer);
			buffer.rewind();
		}
		System.out.println("oBitset toBytes(buffer): " + (System.currentTimeMillis() - t));
		
		t = System.currentTimeMillis();
		jCount = 0;
		for (int i = 0; i < size * 10; i++)
			jCount -= jBitset.toByteArray().length;
		System.out.println("jBitset toBytes: " + (System.currentTimeMillis() - t));
	}
	
}
