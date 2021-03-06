package de.hpi.octopus.serialization;

import java.io.NotSerializableException;
import java.nio.ByteBuffer;

import akka.serialization.ByteBufferSerializer;
import akka.serialization.SerializerWithStringManifest;
import de.hpi.octopus.actors.slaves.Worker.ValidationMessage;
import de.hpi.octopus.structures.BitSet;

public class OctopusMessageSerializer extends SerializerWithStringManifest implements ByteBufferSerializer {

	@Override
	public int identifier() {
		return 10081987;
	}

	@Override
	public String manifest(Object o) {
		return o.getClass().getSimpleName();
	}

	@Override
	public byte[] toBinary(Object o) {
		if (o instanceof ValidationMessage) {
			return this.toBinary((ValidationMessage) o);
		}
		else {		
			throw new IllegalArgumentException("Unknown type: " + o);
		}
	}
	
	public byte[] toBinary(ValidationMessage msg) {
		byte[] bytes = new byte[4 + this.binarySizeOf(msg.getLhss())]; // rhs + lhss
		ByteBuffer buf = ByteBuffer.wrap(bytes); // TODO: use buffer pool for bytebuffers
		this.toBinary(msg, buf);
		return bytes;
	}

	@Override
	public void toBinary(Object o, ByteBuffer buf) {
		if (o instanceof ValidationMessage) {
			this.toBinary((ValidationMessage) o);
		}
		else {		
			throw new IllegalArgumentException("Unknown type: " + o);
		}
	}
	
	public void toBinary(ValidationMessage msg, ByteBuffer buf) {
		buf.putInt(msg.getRhs());
		this.putBitSets(msg.getLhss(), buf);
	}

	@Override
	public Object fromBinary(byte[] bytes, String manifest) throws NotSerializableException {
		ByteBuffer buf = ByteBuffer.wrap(bytes);
		return this.fromBinary(buf, manifest);
	}

	@Override
	public Object fromBinary(ByteBuffer buf, String manifest) throws NotSerializableException {
		if (manifest.equals(ValidationMessage.class.getSimpleName())) {
			return this.fromBinaryValidationMessage(buf);
		}
		else {		
			throw new IllegalArgumentException("Unknown type: " + manifest);
		}
	}
	
	public Object fromBinaryValidationMessage(ByteBuffer buf) throws NotSerializableException {
		final int rhs = buf.getInt();
		final BitSet[] lhss = this.getBitSets(buf);
		return new ValidationMessage(lhss, rhs);
	}
	
	public int binarySizeOf(BitSet[] bitsets) {
		int bitsetBinarySize = bitsets[0].binarySize(); // all bitsets in Octopus are initialized with the same size of words, so we check only the first bitset for its size and use it for all bitsets
		return 4 + bitsets.length * bitsetBinarySize; // array length (1 int) + size of each bitset
	}
	
	public void putBitSets(BitSet[] bitsets, ByteBuffer buf) {
		buf.putInt(bitsets.length);
		for (BitSet bitset : bitsets)
			bitset.toBinary(buf);
	}
	
	public BitSet[] getBitSets(ByteBuffer buf) {
		final BitSet[] bitsets = new BitSet[buf.getInt()];
		for (int i = 0; i < bitsets.length; i++)
			bitsets[i] = BitSet.fromBinary(buf);
		return bitsets;
	}
}
