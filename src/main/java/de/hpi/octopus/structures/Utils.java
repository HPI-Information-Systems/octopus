package de.hpi.octopus.structures;

public class Utils {

	public static String pliToString(int[][] pli) {
		StringBuffer buffer = new StringBuffer("[");
		int maxC = 5;
		for (int[] cluster : pli) {
			maxC--;
			
			int maxR = 5;
			buffer.append("[");
			for (int record : cluster) {
				maxR--;
				
				buffer.append(" " + record);
				
				if (maxR == 0) {
					buffer.append("...");
					break;
				}
			}
			buffer.append(" ]");
			
			if (maxC == 0) {
				buffer.append("...");
				break;
			}
		}
		
		buffer.append("]");
		return buffer.toString();
	}
	
	public static String recordToString(int[] record) {
		StringBuffer buffer = new StringBuffer("[ ");
		for (int i = 0; i < record.length; i++) {
			buffer.append(record[i] + " ");
		}
		buffer.append("]");
		return buffer.toString();
	}
}
