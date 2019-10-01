package de.hpi.octopus.testing;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.twitter.chill.KryoInstantiator;
import com.twitter.chill.KryoPool;

import akka.actor.ActorSystem;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import de.hpi.octopus.actors.slaves.Worker;
import de.hpi.octopus.serialization.OctopusMessageSerializer;
import de.hpi.octopus.structures.BitSet;
import de.hpi.octopus.structures.KryoPoolSingleton;
import de.hpi.octopus.structures.PliCache;
import lombok.Data;
import scala.util.Random;

public class Test {

	public static void testMemoryCalculation() {
		Random rand = new Random(42);
		
		int numAttributes = 1000;
		int[][][] unaryPlis = new int[numAttributes][][];
		for (int i = 0; i < unaryPlis.length; i++)
			unaryPlis[i] = generatePli(rand);
		
		PliCache cache = new PliCache(unaryPlis);
		
		long used = measureUsedMemory();
		
		for (int i = 0; i < numAttributes - 1; i++) {
			int[] addAttributes = {i, i};
			cache.add(addAttributes, generatePli(rand));
			int[] addAttributes2 = {i, i, i};
			cache.add(addAttributes2, generatePli(rand));
			
			int[] blacklistAttributes = {i, i + 1};
			cache.blacklist(blacklistAttributes);
			int[] blacklistAttributes2 = {i, i + 1, i + 1};
			cache.blacklist(blacklistAttributes2);
			
			int randomBlacklist = rand.nextInt(numAttributes);
			int[] blacklistAttributes3 = {randomBlacklist, i};
			cache.blacklist(blacklistAttributes3);
		}
		
		System.out.println((measureUsedMemory() - used) + " bytes (measured)");
		
		System.out.println(cache.getByteSize() + " bytes (calculated)");
	}
	
	private static int[][] generatePli(Random rand) {
		int randClusterCountMin = 100;
		int randClusterCountMax = 1000;
		int randClusterSizeMin = 100;
		int randClusterSizeMax = 1000;
		
		int[][] pli = new int[rand.nextInt(randClusterCountMax - randClusterCountMin) + randClusterCountMin][];
		for (int i = 0; i < pli.length; i++) {
			pli[i] = new int[rand.nextInt(randClusterSizeMax - randClusterSizeMin) + randClusterSizeMin];
			for (int j = 0; j < pli[i].length; j++) {
				pli[i][j] = rand.nextInt();
			}
		}
		
		return pli;
	}
	
	private static long measureUsedMemory() {
		Runtime.getRuntime().gc();
		long max = Runtime.getRuntime().maxMemory();
		long free = Runtime.getRuntime().freeMemory();
		long used = max - free;
		return used;
	}
	
	public static void testKryo() throws NotSerializableException {

		BitSet[] bitsets = new BitSet[200];
		for (int i = 0; i < 200; i++) {
			bitsets[i] = new BitSet(50);
			for (int j = 0; j < 25; j++)
				bitsets[i].set(j);
		}
		Worker.ValidationMessage message = new Worker.ValidationMessage(bitsets, 8);
		
		int POOL_SIZE = 10;
		KryoPool kryo = KryoPool.withByteArrayOutputStream(POOL_SIZE, new KryoInstantiator());
		byte[] ser = kryo.toBytesWithClass(message);
		Object deserObj = kryo.fromBytes(ser);
		
		System.out.println(ser.length);
		
		OctopusMessageSerializer os = new OctopusMessageSerializer();
		byte[] ser2 = os.toBinary(message);
		Object deser2Obj = os.fromBinary(ser2, os.manifest(message));
		
		System.out.println(ser2.length);
		
		Object o = message;
		byte[] ser3 = kryo.toBytesWithClass(o);
		Object deserO = kryo.fromBytes(ser);
		
		System.out.println(deserO instanceof Worker.ValidationMessage);
		
		System.out.println(Arrays.copyOfRange(ser, 0, 0).length);
		System.out.println(Arrays.copyOfRange(ser, 2, 2).length);
		System.out.println(Arrays.copyOfRange(ser, ser.length, ser.length).length);
	}
	
	public static void testKryoPerformance(ActorSystem system) {
		@Data
		class Message implements Serializable {
			private static final long serialVersionUID = 1L;
			int[] data;
			String name;
			boolean validity;
		}
		
		int size = 100;
		int[] data = new int[size];
		for (int i = 0; i < size; i++)
			data[i] = i * 3;
		Message m = new Message();
		m.data = data;
		m.name = "ALongLongLongLongLongLongLongLongLongLongLongLongLongLongLongLongName";
		m.validity = true;

		// Test
		long t = 0;
		long tests = 1000000;
		long count = 0;
		
		System.out.println("Kryo with class");
		byte[] b1 = KryoPoolSingleton.get().toBytesWithClass(m);
		Message m1 = (Message) KryoPoolSingleton.get().fromBytes(b1);
		if (!m1.equals(m)) System.out.println("Error 1");
		System.out.println("\tSize: " + b1.length + " byte");
		t = System.currentTimeMillis();
		count = 0;
		for (int i = 0; i < tests; i++) {
			b1 = KryoPoolSingleton.get().toBytesWithClass(m);
			m1 = (Message) KryoPoolSingleton.get().fromBytes(b1);
			count += m1.data[0];
		}
		System.out.println("\tTime: " + (System.currentTimeMillis() - t) + " ms\t\t\t" + count);

		System.out.println("Kryo without class");
		byte[] b2 = KryoPoolSingleton.get().toBytesWithoutClass(m);
		Message m2 = (Message) KryoPoolSingleton.get().fromBytes(b2, Message.class);
		if (!m2.equals(m)) System.out.println("Error 2");
		System.out.println("\tSize: " + b2.length + " byte");
		t = System.currentTimeMillis();
		count = 0;
		for (int i = 0; i < tests; i++) {
			b2 = KryoPoolSingleton.get().toBytesWithoutClass(m);
			m2 = (Message) KryoPoolSingleton.get().fromBytes(b2, Message.class);
			count += m2.data[0];
		}
		System.out.println("\tTime: " + (System.currentTimeMillis() - t) + " ms\t\t\t" + count);

		System.out.println("Kryo from akka");
		Serialization serialization = SerializationExtension.get(system);
		byte[] b3 = serialization.serialize(m).get();
		Message m3 = (Message) serialization.deserialize(b3, Message.class).get();
		if (!m3.equals(m)) System.out.println("Error 3");
		System.out.println("\tSize: " + b3.length + " byte");
		t = System.currentTimeMillis();
		count = 0;
		for (int i = 0; i < tests; i++) {
			b3 = serialization.serialize(m).get();
			m3 = (Message) serialization.deserialize(b3, Message.class).get();
			count += m3.data[0];
		}
		System.out.println("\tTime: " + (System.currentTimeMillis() - t) + " ms\t\t\t" + count);
	}
	
	public static void testKryoSize() {
		@SuppressWarnings("unused")
		class Message implements Serializable {
			private static final long serialVersionUID = 6455048433435395034L;
			int[] data = {1,2,3};
			String name = "message42";
			boolean validity = true;
			Map<String, String> map = Stream.of(new String[][] {
				  { "key1", "value1" }, 
				  { "key2", "value2" }, 
				}).collect(Collectors.toMap(data -> data[0], data -> data[1]));;
		}
		
		byte[] b0 = {};
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		try {
			out = new ObjectOutputStream(bos);   
			out.writeObject(new Message());
			out.flush();
			b0 = bos.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bos.close();
			} catch (IOException ex) {
			}
		}
		
		byte[] b1 = KryoPoolSingleton.get().toBytesWithClass(new Message());
		
		byte[] b2 = KryoPoolSingleton.get().toBytesWithoutClass(new Message());
		
		System.out.println(b0.length);
		System.out.println(bytesToHex(b0));
		System.out.println();
		System.out.println(b1.length);
		System.out.println(bytesToHex(b1));
		System.out.println();
		System.out.println(b2.length);
		System.out.println(bytesToHex(b2));
		System.out.println();
	}
	
	public static String bytesToHex(byte[] bytes) {
		final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
		char[] hexChars = new char[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = HEX_ARRAY[v >>> 4];
			hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
		}
		return new String(hexChars);
	}
}
