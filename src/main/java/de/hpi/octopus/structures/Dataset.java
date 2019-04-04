package de.hpi.octopus.structures;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import akka.event.LoggingAdapter;
import de.hpi.octopus.actors.Storekeeper.PlisMessage;
import de.hpi.octopus.actors.masters.Profiler.DiscoveryTaskMessage;
import de.hpi.octopus.actors.slaves.Validator.DataMessage;
import de.metanome.algorithm_integration.ColumnIdentifier;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

@Getter
public class Dataset {

	private final int[][][] plis;
	private final String relationName;
	private final String[] columnNames;
	private final int numRecords;
	private final int[][] records;

	public int getNumAtrributes() {
		return this.plis.length;
	}
	
	public Dataset(DiscoveryTaskMessage message, LoggingAdapter log) {
		int[][][] plis = message.getPlis();
		String[] schema = message.getColumnNames();
		int numRecords = message.getNumRecords();
		
		// Debug output
//		for (int[][] pli : plis)
//			log.info(Utils.pliToString(pli));
//		log.info("-------mm---------");
		
		int numAttributes = plis.length;
		
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
			int numNonUniqueValues = (plis[i].length == 0) ? 0 : Arrays.stream(plis[i]).map(cluster -> cluster.length).reduce((a,b) -> a + b).get().intValue();
			int numStrippedClusters = plis[i].length;
			attributes[i] = new Attribute(i, numRecords - numNonUniqueValues + numStrippedClusters);
		}
		Arrays.sort(attributes);

		// Debug output
//		for (Attribute attribute : attributes)
//			log.info(attribute.getSchemaIndex() + "  " + attribute.getNumClusters() + "   " + schema[attribute.getSchemaIndex()]);
		
		int[][][] sortedPlis = new int[numAttributes][][];
		String[] sortedSchema = new String[numAttributes];
		for (int i = 0; i < numAttributes; i++) {
			sortedPlis[i] = plis[attributes[i].getSchemaIndex()];
			sortedSchema[i] = schema[attributes[i].getSchemaIndex()];
		}
		this.plis = sortedPlis;
		this.relationName = message.getRelationName();
		this.columnNames = sortedSchema;
		this.numRecords = message.getNumRecords();
		this.records = null;
		
		// Debug output
//		for (int[][] pli : this.plis)
//			log.info(Utils.pliToString(pli));
//		log.info("-------II---------");
	}
	
	public Dataset(PlisMessage message, LoggingAdapter log) {
		this.plis = message.getPlis();
		this.relationName = null;
		this.columnNames = null;
		this.numRecords = message.getNumRecords();
		this.records = new int[message.getNumRecords()][];
		
		// Debug output
//		for (int[][] pli : this.plis)
//			log.info(Utils.pliToString(pli));
//		log.info("-------oo---------");
		
		// Generate and store pli-records
		for (int r = 0; r < message.getNumRecords(); r++) {
			this.records[r] = new int[this.plis.length];
			Arrays.fill(this.records[r], -1);
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
		
		// Debug output
//		for (int i = 0; i < 1000; i++) {
//			log.info(Utils.recordToString(this.records[i]));
//		}
//		log.info("-------88---------");
		
		// Sort the records in all pli-clusters such that similar records are closer and the ordering of records differs in different plis
		for (int attr = 0; attr < this.plis.length; attr++) {
			int[][] pli = this.plis[attr];
			
			for (int i = 0; i < pli.length; i++) {
				int[] cluster = pli[i];
				
			//	Random rnd = ThreadLocalRandom.current(); // TODO: Add random shuffling to make sortations different? (if two attributes are the same, then its probably good if one gets discarded early on, because keeping both isn't worth it anyway. So its not a got idea?)
			//	for (int j = cluster.length - 1; j > 0; j--) {
			//		int index = rnd.nextInt(j + 1);
			//		int a = cluster[index];
			//		cluster[index] = cluster[j];
			//		cluster[j] = a;
			//	}
				
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
		
		// Debug output
//		for (int i = 0; i < 50; i++) {
//			log.info(Utils.recordToString(this.records[i]));
//		}
//		log.info("-------VV---------");
	}
	
	public PlisMessage toPlisMessage() {
		return new PlisMessage(this.plis, this.numRecords);
	}
	
	public DataMessage toDataMessage() {
		return new DataMessage(this.plis, this.records);
	}
	
	public void writeToDisk(String fileName) {
		String fileString = fileName + ".txt";
		
		File file = new File(fileString);
		if (file.exists())
			file.delete();
		
		try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileString), Charset.forName("UTF8"))) {
		    for (int[][] pli : this.plis) {
				writer.write("[");
				for (int[] cluster : pli) {
					writer.write("(");
					for (int record : cluster)
						writer.write(" " + record + " ");
					writer.write(")");
				}
				writer.write("]\r\n");
			}
		    
		    for (int[] record : this.records)
		    	for (int val : record)
		    		writer.write(val + " ");
		    
		} catch (IOException x) {
		    System.out.println(x.getMessage());
		}
	}

	public ColumnIdentifier[] getColumnIdentifiers() {
		ColumnIdentifier[] columnIdentifiers = new ColumnIdentifier[this.columnNames.length];
		for (int i = 0; i < this.columnNames.length; i++)
			columnIdentifiers[i] = new ColumnIdentifier(this.relationName, this.columnNames[i]);
		return columnIdentifiers;
	}
}
