package de.hpi.octopus.structures;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.BitSet;
import java.util.List;

import javax.validation.constraints.AssertTrue;

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
		
		FDTree fdtree = new FDTree(5, 0);
		
		BitSet lhs = new BitSet();
		
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
		
		assertTrue(fdtree.announceLhss(100).length == 4);
		assertTrue(fdtree.announceLhss(100).length == 0);
	}
	
	@Test
	public void testRemoveInitial() {
		System.out.println("Start testing remove initial");
		
		FDTree fdtree = new FDTree(5, 0);
		
		BitSet lhs = new BitSet();
		
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
		
		assertTrue(fdtree.announceLhss(100).length == 0);
		
		fdtree = new FDTree(5, 0);
		lhs.set(0, 5);
		assertTrue(fdtree.containsLhsOrGeneralization(lhs));
		List<BitSet> lhss = fdtree.getLhsAndGeneralizations(lhs);
		assertTrue(lhss.size() == 4);
		for (BitSet l : lhss)
			fdtree.removeLhs(l);
		assertFalse(fdtree.containsLhsOrGeneralization(lhs));
		lhs.clear();

		assertTrue(fdtree.announceLhss(100).length == 0);
	}
	
	@Test
	public void testFill() {
		System.out.println("Start testing fill");
		
		FDTree fdtree = new FDTree(5, 0);
		BitSet lhs = new BitSet();
		lhs.set(0, 5);
		List<BitSet> lhss = fdtree.getLhsAndGeneralizations(lhs);
		for (BitSet l : lhss)
			fdtree.removeLhs(l);
		lhs = new BitSet();

		assertTrue(fdtree.announceLhss(100).length == 0);
		
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

		assertTrue(fdtree.announceLhss(100).length == 3);
		
	}
	
	@Test 
	public void testGetGeneralizations() {
		System.out.println("Start testing get generalizations");
		
		FDTree fdtree = new FDTree(5, 0);
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

		assertTrue(fdtree.announceLhss(100).length == 3);
	}
	
	@Test 
	public void testRemove() {
		System.out.println("Start testing remove");
		
		for (int x = 0; x < 3; x++)
			testRemove(x);
	}
	
	protected void testRemove(int x) {
		FDTree fdtree = new FDTree(5, 0);
		BitSet lhs = new BitSet();
		lhs.set(0, 5);
		List<BitSet> lhss = fdtree.getLhsAndGeneralizations(lhs);
		for (BitSet l : lhss)
			fdtree.removeLhs(l);
		
		BitSet lhs1 = new BitSet();
		lhs1.set(1);
		lhs1.set(2);
		lhs1.set(3);
		fdtree.addLhs(lhs1);
		
		BitSet lhs2 = new BitSet();
		lhs2.set(1);
		lhs2.set(2);
		lhs2.set(4);
		fdtree.addLhs(lhs2);
		
		BitSet lhs3 = new BitSet();
		lhs3.set(1);
		lhs3.set(4);
		fdtree.addLhs(lhs3);
		
		BitSet[] all = {lhs1, lhs2, lhs3};
		
		assertTrue(fdtree.containsLhs(lhs1));
		assertTrue(fdtree.containsLhs(lhs2));
		assertTrue(fdtree.containsLhs(lhs3));
		
		fdtree.removeLhs(all[x]);

		for (int i = 0; i < all.length; i++) {
			if (i != x)
				assertTrue(fdtree.containsLhs(all[i]));
			else
				assertFalse(fdtree.containsLhs(all[i]));
		}
	}
}
