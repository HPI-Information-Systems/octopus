package de.hpi.octopus.structures;

public class PliCache {

	private PliCacheElement[] children;
	
	public PliCache(int numAttributes) {
		this.children = new PliCacheElement[numAttributes];
	}

	public PliCacheElement get(int... attributes) {
		if (this.children[attributes[0]] == null)
			return null;
		
		return this.children[attributes[0]].get(attributes, 1);
	}

	public void blacklist(final int[] attributes) {
		if (this.children[attributes[0]] == null)
			this.children[attributes[0]] = new PliCacheElement(this.children.length);
		
		this.children[attributes[0]].blacklist(attributes, 1);
	}
	
	public void add(final int[] attributes, final int[][] pli) {
		if (this.children[attributes[0]] == null)
			this.children[attributes[0]] = new PliCacheElement(this.children.length);
		
		this.children[attributes[0]].add(attributes, 1, pli);
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
		if (reductionRecordsPerCluster < 0.2f)
			return false;
		
		// The total pli size should, in addition, decrease significantly for larger attribute combinations
		if ((reducedPliAttributes.length > 2) && (reductionRecordsPerPli < 0.5f))
			return false;
		
		return true;
	}

	private static int countRecords(int[][] pli) {
		int numRecords = 0;
		for (int i = 0; i < pli.length; i++)
			numRecords += pli[i].length;
		return numRecords;
	}
}
