package de.hpi.octopus.testing;

import java.io.NotSerializableException;
import java.io.Serializable;
import java.util.Arrays;

import com.twitter.chill.KryoInstantiator;
import com.twitter.chill.KryoPool;

import akka.actor.ActorSystem;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import de.hpi.octopus.actors.slaves.Validator;
import de.hpi.octopus.serialization.OctopusMessageSerializer;
import de.hpi.octopus.structures.BitSet;
import de.hpi.octopus.structures.KryoPoolSingleton;
import lombok.Data;

public class Test {

	public static void testKryo() throws NotSerializableException {

		BitSet[] bitsets = new BitSet[200];
		for (int i = 0; i < 200; i++) {
			bitsets[i] = new BitSet(50);
			for (int j = 0; j < 25; j++)
				bitsets[i].set(j);
		}
		Validator.ValidationMessage message = new Validator.ValidationMessage(bitsets, 8);
		
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
		
		System.out.println(deserO instanceof Validator.ValidationMessage);
		
		System.out.println(Arrays.copyOfRange(ser, 0, 0).length);
		System.out.println(Arrays.copyOfRange(ser, 2, 2).length);
		System.out.println(Arrays.copyOfRange(ser, ser.length, ser.length).length);
	}
	
	private static void testKryoPerformance(ActorSystem system) {
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
}
