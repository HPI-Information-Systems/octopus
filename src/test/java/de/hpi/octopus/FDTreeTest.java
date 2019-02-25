package de.hpi.octopus;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.BitSet;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.hpi.octopus.structures.FDTree;

public class FDTreeTest {
	
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testInitial() {
		System.out.println("Start testing initial");
		
		FDTree fdtree = new FDTree(5);
		
		BitSet lhs = new BitSet();
		
		lhs.set(0);
		assertTrue(fdtree.containsLhsOrGeneralization(lhs));
		lhs.clear();
		
		lhs.set(1);
		assertTrue(fdtree.containsLhsOrGeneralization(lhs));
		lhs.clear();
		
		lhs.set(2);
		assertTrue(fdtree.containsLhsOrGeneralization(lhs));
		lhs.clear();
		
		lhs.set(3);
		assertTrue(fdtree.containsLhsOrGeneralization(lhs));
		lhs.clear();
		
		lhs.set(4);
		assertTrue(fdtree.containsLhsOrGeneralization(lhs));
		lhs.clear();
	}
	
	@Test
	public void testRemoveInitial() {
		System.out.println("Start testing remove initial");
		
		FDTree fdtree = new FDTree(5);
		
		BitSet lhs = new BitSet();
		
		lhs.set(0);
		fdtree.removeLhs(lhs);
		assertFalse(fdtree.containsLhsOrGeneralization(lhs));
		lhs.clear();
		
		lhs.set(1);
		fdtree.removeLhs(lhs);
		assertFalse(fdtree.containsLhsOrGeneralization(lhs));
		lhs.clear();
		
		lhs.set(2);
		fdtree.removeLhs(lhs);
		assertFalse(fdtree.containsLhsOrGeneralization(lhs));
		lhs.clear();
		
		lhs.set(3);
		fdtree.removeLhs(lhs);
		assertFalse(fdtree.containsLhsOrGeneralization(lhs));
		lhs.clear();
		
		lhs.set(4);
		fdtree.removeLhs(lhs);
		assertFalse(fdtree.containsLhsOrGeneralization(lhs));
		lhs.clear();
		
		fdtree = new FDTree(5);
		lhs.set(0, 5);
		List<BitSet> lhss = fdtree.getLhsAndGeneralizations(lhs);
		for (BitSet l : lhss)
			fdtree.removeLhs(l);
		assertFalse(fdtree.containsLhsOrGeneralization(lhs));
		lhs.clear();
	}
	
	@Test
	public void testFill() {
		System.out.println("Start testing fill");
		
		FDTree fdtree = new FDTree(5);
		BitSet lhs = new BitSet();
		lhs.set(0, 5);
		List<BitSet> lhss = fdtree.getLhsAndGeneralizations(lhs);
		for (BitSet l : lhss)
			fdtree.removeLhs(l);
		lhs = new BitSet();
		
		lhs.set(0);
		lhs.set(2);
		fdtree.addLhs(lhs);
		assertTrue(fdtree.containsLhsOrGeneralization(lhs));
		
		lhs.clear();
		
		lhs.set(3);
		lhs.set(4);
		fdtree.addLhs(lhs);
		assertTrue(fdtree.containsLhsOrGeneralization(lhs));
		
		lhs.clear();
		
		lhs.set(1);
		lhs.set(2);
		lhs.set(3);
		fdtree.addLhs(lhs);
		assertTrue(fdtree.containsLhsOrGeneralization(lhs));
		
		lhs.clear();
		
		lhs.set(0);
		lhs.set(1);
		lhs.set(4);
		assertFalse(fdtree.containsLhsOrGeneralization(lhs));
	}
	
	@Test 
	public void testGetGeneralizations() {
		System.out.println("Start testing get generalizations");
		
		FDTree fdtree = new FDTree(5);
		BitSet lhs = new BitSet();
		lhs.set(0, 5);
		List<BitSet> lhss = fdtree.getLhsAndGeneralizations(lhs);
		for (BitSet l : lhss)
			fdtree.removeLhs(l);
		
		BitSet lhs1 = new BitSet();
		lhs1.set(0);
		lhs1.set(2);
		fdtree.addLhs(lhs1);
		
		BitSet lhs2 = new BitSet();
		lhs2.set(3);
		lhs2.set(4);
		fdtree.addLhs(lhs2);
		
		BitSet lhs3 = new BitSet();
		lhs3.set(1);
		lhs3.set(2);
		lhs3.set(3);
		fdtree.addLhs(lhs3);
		
		lhs = new BitSet();
		lhs.set(0);
		lhs.set(1);
		lhs.set(2);
		lhs.set(3);
		lhss = fdtree.getLhsAndGeneralizations(lhs);

		assertTrue(lhss.size() == 2);
		
		assertTrue(lhss.contains(lhs1));
		assertTrue(lhss.contains(lhs3));
	}
	
}
