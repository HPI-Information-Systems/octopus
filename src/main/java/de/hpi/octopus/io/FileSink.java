package de.hpi.octopus.io;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import akka.event.LoggingAdapter;
import de.hpi.octopus.structures.BitSet;
import de.hpi.octopus.structures.Dataset;
import de.hpi.octopus.structures.FunctionalDependency;

public class FileSink {

	public static void write(int[][][] plis, int[][] records, String path) {
		try (BufferedWriter writer = createWriter(Paths.get(path))) {
			for (int[][] pli : plis) {
				writer.write("[");
				for (int[] cluster : pli) {
					writer.write("(");
					for (int record : cluster)
						writer.write(" " + record + " ");
					writer.write(")");
				}
				writer.write("]\r\n");
			}
		    
		    for (int[] record : records)
		    	for (int val : record)
		    		writer.write(val + " ");
		}
		catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}
	
	public static void write(BitSet lhs, int rhs, Dataset dataset, LoggingAdapter log) {
		try (BufferedWriter writer = createWriter(dataset.getOutputPathFor(rhs))) {
			writer.write(FunctionalDependency.toString(lhs, rhs, dataset));
		}
		catch (IOException e) {
			logError(e, rhs, log);
		}
	}
	
	public static void write(List<BitSet> lhss, int rhs, Dataset dataset, LoggingAdapter log) {
		try (BufferedWriter writer = createWriter(dataset.getOutputPathFor(rhs))) {
			for (BitSet lhs : lhss) {
				writer.write(FunctionalDependency.toString(lhs, rhs, dataset));
			}
		}
		catch (IOException e) {
			logError(e, rhs, log);
		}
	}
	
	private static BufferedWriter createWriter(Path path) throws IOException {
		Files.createDirectories(path.getParent());
		
		return Files.newBufferedWriter(path, StandardCharsets.UTF_8);
	}
	
	private static void logError(Exception e, int rhs, LoggingAdapter log) {
		log.error(e, "Failed storing results for rhs attribute " + rhs);
	}
}
