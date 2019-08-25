package de.hpi.octopus.serialization;

import static org.junit.Assert.assertTrue;

import java.io.NotSerializableException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.hpi.octopus.actors.slaves.Worker.ValidationMessage;
import de.hpi.octopus.structures.BitSet;

public class OctopusMessageSerializerTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testOldSerialization() throws NotSerializableException {
		BitSet[] lhss = new BitSet[4];
		for (int i = 0; i < lhss.length; i++) {
			lhss[i] = new BitSet(lhss.length * 40);
			lhss[i].set(i * 1);
			lhss[i].set(i * 10);
			lhss[i].set(i * 30);
		}
		ValidationMessage message = new ValidationMessage(lhss, 88);
		
		OctopusMessageSerializer serializer = new OctopusMessageSerializer();
		
		byte[] bytes = serializer.toBinary(message);
		ValidationMessage messageReconstruction = (ValidationMessage) serializer.fromBinary(bytes, serializer.manifest(message));
		
		assertTrue(message.getRhs() == messageReconstruction.getRhs());
		assertTrue(message.getLhss().length == messageReconstruction.getLhss().length);
		for (int i = 0; i < message.getLhss().length; i++)
			assertTrue(message.getLhss()[i].equals(messageReconstruction.getLhss()[i]));
	}
	
	@Test
	public void testNewSerializationLittleEndian() throws NotSerializableException {
		BitSet[] lhss = new BitSet[4];
		for (int i = 0; i < lhss.length; i++) {
			lhss[i] = new BitSet(lhss.length * 40);
			lhss[i].set(i * 1);
			lhss[i].set(i * 10);
			lhss[i].set(i * 30);
		}
		ValidationMessage message = new ValidationMessage(lhss, 88);
		
		OctopusMessageSerializer serializer = new OctopusMessageSerializer();
		
		int bufSize = 4 + serializer.binarySizeOf(message.getLhss());
		ByteBuffer buf = ByteBuffer.allocate(bufSize).order(ByteOrder.LITTLE_ENDIAN);
		
		serializer.toBinary(message, buf);
		buf.flip();
		ValidationMessage messageReconstruction = (ValidationMessage) serializer.fromBinary(buf, serializer.manifest(message));
		
		assertTrue(message.getRhs() == messageReconstruction.getRhs());
		assertTrue(message.getLhss().length == messageReconstruction.getLhss().length);
		for (int i = 0; i < message.getLhss().length; i++)
			assertTrue(message.getLhss()[i].equals(messageReconstruction.getLhss()[i]));
	}
	
	@Test
	public void testNewSerializationBigEndian() throws NotSerializableException {
		BitSet[] lhss = new BitSet[4];
		for (int i = 0; i < lhss.length; i++) {
			lhss[i] = new BitSet(lhss.length * 40);
			lhss[i].set(i * 1);
			lhss[i].set(i * 10);
			lhss[i].set(i * 30);
		}
		ValidationMessage message = new ValidationMessage(lhss, 88);
		
		OctopusMessageSerializer serializer = new OctopusMessageSerializer();
		
		int bufSize = 4 + serializer.binarySizeOf(message.getLhss());
		ByteBuffer buf = ByteBuffer.allocate(bufSize).order(ByteOrder.BIG_ENDIAN);
		
		serializer.toBinary(message, buf);
		buf.flip();
		ValidationMessage messageReconstruction = (ValidationMessage) serializer.fromBinary(buf, serializer.manifest(message));
		
		assertTrue(message.getRhs() == messageReconstruction.getRhs());
		assertTrue(message.getLhss().length == messageReconstruction.getLhss().length);
		for (int i = 0; i < message.getLhss().length; i++)
			assertTrue(message.getLhss()[i].equals(messageReconstruction.getLhss()[i]));
	}
}
