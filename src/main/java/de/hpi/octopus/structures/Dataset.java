package de.hpi.octopus.structures;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import akka.event.LoggingAdapter;
import de.hpi.octopus.actors.Storekeeper.PlisMessage;
import de.hpi.octopus.actors.masters.Profiler.DiscoveryTaskMessage;
import de.hpi.octopus.actors.slaves.Validator.DataMessage;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

@Getter
public class Dataset {

	private final int[][][] plis;
	private final String[] schema;
	private final int numRecords;
	private final int[][] records;
	private final String datasetName;

	public int getNumAtrributes() {
		return this.plis.length;
	}
	
	public Dataset(DiscoveryTaskMessage message, LoggingAdapter log) {
		int[][][] plis = message.getPlis();
		String[] schema = message.getSchema();
		int numRecords = message.getNumRecords();
		
		int numAttributes = this.getNumAtrributes();
		
		// Sort the attributes by their number of pli clusters (including clusters of size 1)
		@Data @AllArgsConstructor
		final class Attribute implements Comparable<Attribute> {
			private int schemaIndex;
			private int numClusters;
			@Override
			public int compareTo(Attribute other) {
				return other.getNumClusters() - this.getNumClusters();
			}
		}
		
		Attribute[] attributes = new Attribute[numAttributes];
		for (int i = 0; i < numAttributes; i++) {
			int numNonUniqueValues = Arrays.stream(plis[i]).map(cluster -> cluster.length).reduce((a,b) -> a + b).get().intValue();
			int numStrippedClusters = plis[i].length;
			attributes[i] = new Attribute(i, numRecords - numNonUniqueValues + numStrippedClusters);
		}
		
		Arrays.sort(attributes);
		
		int[][][] sortedPlis = new int[numAttributes][][];
		String[] sortedSchema = new String[numAttributes];
		for (int i = 0; i < numAttributes; i++) {
			sortedPlis[i] = plis[attributes[i].getSchemaIndex()];
			sortedSchema[i] = schema[attributes[i].getSchemaIndex()];
		}
		this.plis = sortedPlis;
		this.schema = sortedSchema;
		this.numRecords = message.getNumRecords();
		this.records = null;
		this.datasetName = message.getDatasetName();
		
		// Debug output
		for (Attribute attribute : attributes)
			log.info(attribute.getSchemaIndex() + "  " + attribute.getNumClusters() + "   " + this.schema[attribute.getSchemaIndex()]);
	}
	
	public Dataset(PlisMessage message, LoggingAdapter log) {
		this.plis = message.getPlis();
		this.schema = null;
		this.numRecords = message.getNumRecords();
		this.records = new int[message.getNumRecords()][];
		this.datasetName = null;
		
		// Generate and store pli-records
		for (int r = 0; r < message.getNumRecords(); r++) {
			this.records[r] = new int[this.plis.length];
			for (int a = 0; a < this.plis.length; a++) {
				this.records[r][a] = -1;
			}
		}
		for (int attr = 0; attr < this.plis.length; attr++) {
			int[][] pli = this.plis[attr];
			for (int val = 0; val < pli.length; val++) {
				for (int rec : pli[val]) {
					this.records[rec][attr] = val;
				}
			}
		}
		log.info("Done creating pli records");

		// Sort the records in all pli-clusters such that similar records are closer and the ordering of records differs in different plis
		for (int attr = 0; attr < this.plis.length; attr++) {
			int[][] pli = this.plis[attr];
			
			for (int i = 0; i < pli.length; i++) {
				int[] cluster = pli[i];
				
				final int attribute = attr;
				Comparator<Integer> comparator;
				if (attr == 0) {
					comparator = new Comparator<Integer>() {
						@Override
						public int compare(Integer record1, Integer record2) {
							int compare = records[record1][attribute + 1] - records[record1][attribute + 1];
					    	
					    	if (compare == 0)
					    		compare = records[record1][plis.length - 1] - records[record1][pli.length - 1];
					    	
					        return compare;
						}
					};
				}
				else if (attr == this.plis.length - 1) {
					comparator = new Comparator<Integer>() {
						@Override
						public int compare(Integer record1, Integer record2) {
							int compare = records[record1][0] - records[record1][0];
					    	
					    	if (compare == 0)
					    		compare = records[record1][attribute - 1] - records[record1][attribute - 1];
					    	
					        return compare;
						}
					};
				}
				else {
					comparator = new Comparator<Integer>() {
						@Override
					    public int compare(Integer record1, Integer record2) {
					    	int compare = records[record1][attribute + 1] - records[record1][attribute + 1];
					    	
					    	if (compare == 0)
					    		compare = records[record1][attribute - 1] - records[record1][attribute - 1];
					    	
					        return compare;
					    }
					};
				}
				IntArrayList sortedCluster = new IntArrayList(cluster);
				Collections.sort(sortedCluster, comparator);
				pli[i] = sortedCluster.elements();
			}
		}
		log.info("Done sorting pli-clusters");
	}
	
	public PlisMessage toPlisMessage() {
		return new PlisMessage(this.plis, this.numRecords);
	}
	
	public DataMessage toDataMessage() {
		return new DataMessage(this.plis, this.records);
	}
}
