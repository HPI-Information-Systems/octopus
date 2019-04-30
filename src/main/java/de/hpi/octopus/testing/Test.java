package de.hpi.octopus.testing;

import java.io.NotSerializableException;
import java.util.Arrays;

import com.twitter.chill.KryoInstantiator;
import com.twitter.chill.KryoPool;

import de.hpi.octopus.actors.slaves.Validator;
import de.hpi.octopus.serialization.OctopusMessageSerializer;
import de.hpi.octopus.structures.BitSet;

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
}
