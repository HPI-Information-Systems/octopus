package de.hpi.octopus.structures;

import java.util.Arrays;

import lombok.Getter;

public class PliCache {

	@Getter
	private long byteSize;					// Size of this cache in bytes
	
	private PliCacheElement[] children;
	private int readsThreshold;				// Number of reads a pli needs to survive the next prune() call; increases with every prune() call

	public PliCache(int[][][] unaryPlis) {
		this.children = new PliCacheElement[unaryPlis.length];
		this.byteSize = 0;					// We ignore the size of this PliCache object and the size of the unary plis, because they are not optional, i.e., no real cache items
		this.readsThreshold = 0;
		
		for (int i = 0; i < unaryPlis.length; i++)
			this.children[i] = new PliCacheElement(unaryPlis[i], unaryPlis.length, Integer.MAX_VALUE);
	}

	public PliCacheElement get(int... attributes) {
		return this.children[attributes[0]].get(attributes, 1);
	}

	public void blacklist(final int[] attributes) {
		int childIndex = 0;
		
		// Assert that the unary plis are never blacklisted
		if (childIndex + 1 == attributes.length)
			throw new IllegalArgumentException("The blacklist() method was called for a unary pli, but the unary plis should never be blacklisted.");
		
		this.byteSize += this.children[attributes[childIndex]].blacklist(attributes, childIndex + 1);
	}
	
	public void add(final int[] attributes, final int[][] pli) {
		int childIndex = 0;

		// Assert that the unary plis are never added (they are present by default)
		if (childIndex + 1 == attributes.length) {
			System.out.println(Arrays.toString(attributes));
			throw new IllegalArgumentException("The add() method was called for a unary pli, but the unary plis are always present by default.");
		}
		
		this.byteSize += this.children[attributes[childIndex]].add(attributes, childIndex + 1, pli);
	}
	
	public void prune(long bytesToPrune) {
		final long minSize = this.children.length * (this.children.length * (12 + 4 + 4 + 4 + 8));	// If all prefixes are pruned, each unary pli has all children pruned, where a pruned child has a size of 32 byte.
		final long targetSize = Math.max(this.byteSize - bytesToPrune, minSize);
		
		while (this.byteSize > targetSize) {
			for (int i = 0; i < this.children.length; i++)
				this.byteSize += this.children[i].prune(this.readsThreshold);
			this.readsThreshold++;
		}
	}
	
	public boolean isWorthCaching(int[][] reducedPli, int[] reducedPliAttributes, int[][] originalPli) {
		final float originalNumClusters = originalPli.length;
		final float reducedNumClusters = reducedPli.length;
		
		if (originalNumClusters == 0)
			return false;
		
		if (reducedNumClusters == 0)
			return true;

		final float originalNumRecords = countRecords(originalPli);
		final float reducedNumRecords = countRecords(reducedPli);
		
		final float reductionRecordsPerPli = 1 - reducedNumRecords / originalNumRecords;
		
		final float originalRecordsPerCluster = originalNumRecords / originalNumClusters;
		final float reducedRecordsPerCluster = originalNumRecords / (reducedNumClusters + (originalNumRecords - reducedNumRecords)); // The same records from before are now in the reducedNumClusters + the new clusters of size 1
		final float reductionRecordsPerCluster = 1 - reducedRecordsPerCluster / originalRecordsPerCluster;
		
		// The average cluster size should decrease significantly
		if (reductionRecordsPerCluster < 0.05f) // TODO: too strict or too insignificant? parameter?
			return false;
		
		// The total pli size should, in addition, decrease significantly for larger attribute combinations
/*		if ((reducedPliAttributes.length > 2) && (reductionRecordsPerPli < 0.2f))
			return false;
*/		
		return true;
	}

	private static int countRecords(int[][] pli) {
		int numRecords = 0;
		for (int i = 0; i < pli.length; i++)
			numRecords += pli[i].length;
		return numRecords;
	}
}
