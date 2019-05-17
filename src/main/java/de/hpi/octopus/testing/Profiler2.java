package de.hpi.octopus.testing;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.octopus.actors.DependencySteward;
import de.hpi.octopus.actors.DependencySteward.CandidateRequestMessage;
import de.hpi.octopus.actors.DependencySteward.FinalizeMessage;
import de.hpi.octopus.actors.DependencySteward.InvalidFDsMessage;
import de.hpi.octopus.actors.Storekeeper;
import de.hpi.octopus.actors.masters.AbstractMaster;
import de.hpi.octopus.actors.masters.Profiler;
import de.hpi.octopus.actors.masters.AbstractMaster.RegistrationMessage;
import de.hpi.octopus.actors.masters.Profiler.CandidateMessage;
import de.hpi.octopus.actors.masters.Profiler.DiscoveryTaskMessage;
import de.hpi.octopus.actors.masters.Profiler.FDsUpdatedMessage;
import de.hpi.octopus.actors.masters.Profiler.SamplingResultMessage;
import de.hpi.octopus.actors.masters.Profiler.SendPlisMessage;
import de.hpi.octopus.actors.masters.Profiler.ValidationResultMessage;
import de.hpi.octopus.actors.slaves.Validator;
import de.hpi.octopus.actors.slaves.Validator.SamplingMessage;
import de.hpi.octopus.actors.slaves.Validator.TerminateMessage;
import de.hpi.octopus.actors.slaves.Validator.ValidationMessage;
import de.hpi.octopus.structures.BitSet;
import de.hpi.octopus.structures.Dataset;
import de.hpi.octopus.structures.DependencyStewardRing;
import de.hpi.octopus.structures.FDStore;
import de.hpi.octopus.structures.FDTree;
import de.hpi.octopus.structures.SamplingEfficiency;
import de.hpi.octopus.structures.ValueCombination;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class Profiler2 extends AbstractMaster {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "profiler";

	public static Props props() {
		return Props.create(Profiler2.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	/////////////////
	// Actor State //
	/////////////////

	private Dataset dataset;

	private Queue<ActorRef> idleValidators = new LinkedList<>();	// Idle: This validation branch is stopped here and pauses until a dependency steward becomes idle again.
	private Queue<ActorRef> waitingValidators = new LinkedList<>();	// Waiting: This validation branch is collecting candidates and waits for them to come.
	private Map<ActorRef, Object> busyValidators = new HashMap<>();	// Busy: This validation branch is working on a message.
	
	private ActorRef[] dependencyStewards;							// List of non-finished dependency stewards
	private DependencyStewardRing dependencyStewardRing;			// Ring of dependency stewards to find the next steward serving new validation tasks
	
	private int children = 0;
	
	private SamplingEfficiency[] samplingEfficiencies;
	private PriorityQueue<SamplingEfficiency> prioritizedSamplingEfficiencies;

	private Queue<Object> unassignedWork = new LinkedList<>();

	private int[] rhss;
	private FDStore[] fdss;
//	private FDStore[] fdss2;
	private int[][] records;
	private int[][][] plis;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////
	
	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(DiscoveryTaskMessage.class, this::handle)
				.match(SendPlisMessage.class, this::handle)
				.match(CandidateMessage.class, this::handle)
				.match(ValidationResultMessage.class, this::handle)
				.match(SamplingResultMessage.class, this::handle)
				.match(FDsUpdatedMessage.class, this::handle)
				.build()
				.orElse(super.createReceive());
	}
	
	@Override
	protected void handle(RegistrationMessage message) {
		super.handle(message);
		
		this.assign(this.sender());
	}
	
	@Override
	protected void handle(Terminated message) {
		super.handle(message);
		
		String actorName = message.getActor().path().name();
		
		// If a dependency steward terminates, it is done and we have one child less
		if (actorName.contains(DependencySteward.DEFAULT_NAME)) {
			this.children--;
			
			// Terminate the profiling if all children, i.e., all dependency stewards are done
			if (this.children == 0) {
				this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
				for (ActorRef validator : this.busyValidators.keySet())
					validator.tell(new TerminateMessage(), ActorRef.noSender());
				for (ActorRef validator : this.idleValidators)
					validator.tell(new TerminateMessage(), ActorRef.noSender());
				for (ActorRef validator : this.waitingValidators)
					validator.tell(new TerminateMessage(), ActorRef.noSender());
				
				this.log().info("Finished discovery task.");
			}
		}
		
		// If a validator terminates, we forget him and might need to reschedule his work
		if (actorName.contains(Validator.DEFAULT_NAME)) {
			this.idleValidators.remove(message.getActor());
			this.waitingValidators.remove(message.getActor());
			Object work = this.busyValidators.remove(message.getActor());
			
			if (work != null) {
				if (this.idleValidators.isEmpty()) {
					this.unassignedWork.add(work);
					return;
				}
				
				ActorRef validator = this.idleValidators.poll();
				this.busyValidators.put(validator, work);
				validator.tell(work, this.self());
			}
		}
	}
	
	protected void handle(DiscoveryTaskMessage message) throws Exception {
		// Handle edge cases
		if (this.dataset != null) {
			this.log().error("Can process only one task! Dropping profiling request for {} plis.", message.getPlis().length);
			return;
		}
		if (message.getPlis().length <= 1) {
			this.log().error("Found {} attributes in the input and stopped processing, because at least 2 attributes are needed to make FDs possible.", message.getPlis().length);
			return;
		}
		
		// Initialize local dataset with the given task; this also sorts the attributes by the number of their pli clusters
		this.dataset = new Dataset(message, this.log());
		this.plis = this.dataset.getPlis();
		
		// Start one dependency steward for each attribute
		int numAttributes = this.dataset.getNumAtrributes();
		this.rhss = new int[numAttributes];
		this.fdss = new FDStore[numAttributes];
//		this.fdss2 = new FDStore[numAttributes];
		for (int i = 0; i < numAttributes; i++) {
			this.rhss[i] = i;
			this.fdss[i] = new FDTree(numAttributes, i);
//			this.fdss2[i] = new FDSet(numAttributes, i);
		}

		int count = 0; 
		
		Dataset d = new Dataset(new Storekeeper.PlisMessage(this.dataset.getPlis(), this.dataset.getNumRecords()), this.log());
		// Store the data
		this.records = d.getRecords();
		
/*		for (int i = 0; i < this.records.length; i++) {
			if (i % 500 == 0)
				System.out.println(i);
			
			for (int j = i + 1; j < this.records.length; j++) {
				BitSet invalidLhs = new BitSet(numAttributes);
				IntList invalidRhss = new IntArrayList();
				for (int attribute = 0; attribute < numAttributes; attribute++) {
					if (this.isMatch(i, j, attribute))
						invalidLhs.set(attribute);
					else
						invalidRhss.add(attribute);
				}
				
				for (int k : invalidRhss) {
					for (BitSet specLhs : this.fdss[k].getLhsAndGeneralizations(invalidLhs)) {
						this.fdss[k].removeLhs(specLhs);
						
						for (int attribute = 0; attribute < numAttributes; attribute++) {
							if (invalidLhs.get(attribute) || (attribute == k))
								continue;
							
							specLhs.set(attribute);
							if (!this.fdss[k].containsLhsOrGeneralization(specLhs)) {
								this.fdss[k].addLhs(specLhs);
								
								// If dynamic memory management is enabled, frequently check the memory consumption and trim the positive cover if it does not fit anymore
							//	this.memoryGuardian.memoryChanged(1);
							//	this.memoryGuardian.match(this.negCover, this.posCover, invalidLhs); // TODO: Someone needs to supervise the overall memory consumption
							}
							specLhs.clear(attribute);
						}
					}
				}
			}
		}
*/		
		for (int i = 0; i < numAttributes; i++) {
			int rhs = i;
			while (true) {
				BitSet[] lhss = this.fdss[i].announceLhss(100);
//				BitSet[] lhss2 = this.fdss2[i].announceLhss(100);
//				if (lhss.length != lhss2.length)
//					throw new RuntimeException("announce()");
				
				if (lhss.length == 0)
					break;
				
				int falses = 0;
				for (BitSet lhsBitSet : lhss) {
					int[] lhs = toArray(lhsBitSet);
					
					int[] violation = this.findViolation(lhs, rhs);
					if (violation == null)
						continue;
					
					falses++;
					
				//	this.printBitSet(lhsBitSet, numAttributes);
				//	System.out.println(Utils.recordToString(lhs));
				//	System.out.println();
					
					// Add the violated FD to the container of invalid FDs
				//	invalidFDs.add(new FunctionalDependency(lhsBitSet, rhs)); // Not necessary, because we compare the two records and find and add this non-FD again 
					
					// Compare the two records that caused the violation to find violations for other FDs (= execute comparison suggestion)
					BitSet invalidLhs = new BitSet(numAttributes);
					IntList invalidRhss = new IntArrayList();
					for (int attribute = 0; attribute < numAttributes; attribute++) {
						if (this.isMatch(violation[0], violation[1], attribute))
							invalidLhs.set(attribute);
						else
							invalidRhss.add(attribute);
					}
					
				/*	System.out.println(Utils.recordToString(this.records[violation[0]]));
					System.out.println(Utils.recordToString(this.records[violation[1]]));
					this.printBitSet(invalidLhs, numAttributes);
					System.out.println();
				*/	
					//invalidLhs = lhsBitSet;
					
//					List<BitSet> specLhs2 = this.fdss2[i].getLhsAndGeneralizations(invalidLhs);
					for (BitSet specLhs : this.fdss[i].getLhsAndGeneralizations(invalidLhs)) {
//						if (!specLhs2.contains(specLhs))
//							throw new RuntimeException("getLhsAndGeneralizations()");
						
						this.fdss[i].removeLhs(specLhs);
//						this.fdss2[i].removeLhs(specLhs);
						
//						if (this.fdss[i].containsLhs(specLhs))
//							throw new RuntimeException("remove()" + specLhs.toString());
//						if (this.fdss2[i].containsLhs(specLhs))
//							throw new RuntimeException("remove()" + specLhs.toString());
						
						for (int attribute = 0; attribute < numAttributes; attribute++) {
							if (invalidLhs.get(attribute) || (attribute == rhs))
								continue;
							
							specLhs.set(attribute);
							if (!this.fdss[i].containsLhsOrGeneralization(specLhs)) {
//								if (this.fdss2[i].containsLhsOrGeneralization(specLhs))
//									throw new RuntimeException("containsLhsOrGeneralization()" + specLhs.toString());
								
								
								this.fdss[i].addLhs(specLhs);
//								this.fdss2[i].addLhs(specLhs);
								
//								if (!this.fdss[i].containsLhs(specLhs))
//									throw new RuntimeException("add()" + specLhs.toString());
//								if (!this.fdss2[i].containsLhs(specLhs))
//									throw new RuntimeException("add()" + specLhs.toString());
								
								// If dynamic memory management is enabled, frequently check the memory consumption and trim the positive cover if it does not fit anymore
							//	this.memoryGuardian.memoryChanged(1);
							//	this.memoryGuardian.match(this.negCover, this.posCover, invalidLhs); // TODO: Someone needs to supervise the overall memory consumption
							}
							specLhs.clear(attribute);
						}
					}
				}

				//System.out.println("Given " + lhss.length + "; false " + falses);
				
			}
			
			// Collect all valid lhss
			BitSet allAttributes = new BitSet(numAttributes);
			for (int j = 0; j < numAttributes; j++)
				allAttributes.set(j);
			
			List<BitSet> allLhss = this.fdss[i].getLhsAndGeneralizations(allAttributes);
			
			count += allLhss.size();
			
			System.out.println("Finished " + i + " with " + allLhss.size() + " FDs");
	/*		for (BitSet l : allLhss) {
				for (int x = l.nextSetBit(0); x >= 0; x = l.nextSetBit(x + 1)) {
					System.out.print(this.dataset.getColumnNames()[x] + " ");
				}
				System.out.println("--> " + this.dataset.getColumnNames()[i]);
			}
	*/	}
		
		
		
		// Collect all valid lhss
		BitSet allAttributes = new BitSet(numAttributes);
		for (int j = 0; j < numAttributes; j++)
			allAttributes.set(j);
		
/*		for (int i = 0; i < numAttributes; i++) { 
			List<BitSet> allLhss = this.fdss[i].getLhsAndGeneralizations(allAttributes);
			
			// Free the FDTree
			this.fdss[i] = null;
			
			count += allLhss.size();
			
			// Write all FDs to disk
			String pathString = message.getOutputPath() + File.separatorChar + message.getDataset().getRelationName();
			String fileString = pathString + File.separatorChar + message.getDataset().getColumnNames()[this.rhs] + ".txt";
			
			File path = new File(pathString);
			if (!path.exists())
				path.mkdirs();
			
			File file = new File(fileString);
			if (file.exists())
				file.delete();
			
			try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileString), Charset.forName("UTF8"))) {
			    for (BitSet lhs : allLhss) {
					StringBuffer buffer = new StringBuffer("[");
					for (int attribute = lhs.nextSetBit(0); attribute >= 0; attribute = lhs.nextSetBit(attribute + 1)) {
						buffer.append(message.getDataset().getColumnNames()[attribute]);
						buffer.append(", ");
					}
					buffer.delete(buffer.length() - 2 , buffer.length());
					buffer.append("] --> ");
					buffer.append(message.getDataset().getColumnNames()[this.rhs]);
					buffer.append("\r\n");
	
					writer.write(buffer.toString());
				}
			} catch (IOException x) {
			    this.log().error(x, x.getMessage());
			}
			
			// Tell the progress listener that this dependency steward is done
			this.context().actorSelection("/user/" + ProgressListener.DEFAULT_NAME).tell(new ProgressListener.FinishedMessage(allLhss.size(), allLhss.toArray(new BitSet[allLhss.size()]), this.rhs, message.getDataset()), ActorRef.noSender());
			
			// Terminate
			this.self().tell(PoisonPill.getInstance(), this.self());
		}		
*/		
		System.out.println("Found FDs: " + count);
		
	}

	private void printBitSet(BitSet bitset, int length) {
		for (int u = 0; u < length; u++)
			if (bitset.get(u))
				System.out.print(1 + " ");
			else
				System.out.print(0 + " ");
		System.out.println();
	}
	
	private void print(BitSet bitset, int length) {
		for (int u = 0; u < length; u++)
			if (bitset.get(u))
				System.out.print(u + " ");
		System.out.println();
	}
	
	private int[] findViolation(int[] lhs, int rhs) {
		for (int[] cluster : this.plis[lhs[0]]) {
			final Map<ValueCombination, int[]> lhsValue2rhsValue = new HashMap<>();
			for (int recordID : cluster) {
				ValueCombination lhsValue = new ValueCombination(this.records[recordID], lhs);
				final int rhsValue = this.records[recordID][rhs];

				if (lhsValue.isUnique())
					continue;
				
				// If the lhs value is new, add a new mapping to the rhs value
				if (!lhsValue2rhsValue.containsKey(lhsValue)) {
					int[] rhsValueAndRecord = new int[2];
					rhsValueAndRecord[0] = rhsValue;
					rhsValueAndRecord[1] = recordID;
					lhsValue2rhsValue.put(lhsValue, rhsValueAndRecord);
					continue;
				}
				
				// If the lhs value hash is known, test if the rhs value is the same and return a violation if not
				final int[] rhsValueAndRecord = lhsValue2rhsValue.get(lhsValue);
				if (!isEqual(rhsValueAndRecord[0], rhsValue)) {
					final int[] violation = {recordID, rhsValueAndRecord[1]};
					return violation;
				}
			}
		}
		return null;
	}
	
	private boolean isMatch(final int recordID1, final int recordID2, final int attribute) {
		return isEqual(this.records[recordID1][attribute], this.records[recordID2][attribute]);
	}

	private static boolean isEqual(final int value1, final int value2) {
		return (value1 == value2) && (value1 != -1);
	}
	
	private static int[] toArray(BitSet bitSet) {
		int[] array = new int[bitSet.cardinality()];
		for (int trueBit = bitSet.nextSetBit(0), index = 0; trueBit >= 0; trueBit = bitSet.nextSetBit(trueBit + 1), index++)
			array[index] = trueBit;
		return array;
	}
	
	
	private void handle(SendPlisMessage mesage) {
		this.sender().tell(this.dataset.toPlisMessage(), this.self());
	}
	
	protected void handle(ValidationResultMessage validationResultMessage) {
		ActorRef validator = this.sender();
		ValidationMessage validationMessage = (ValidationMessage) this.busyValidators.remove(validator);
		
		// Consider the current validator as idle; this is important, because if the profiling is done, we need to notify all validators that they can terminate
		this.idleValidators.add(validator);
		
		int validationRequester = validationMessage.getRhs(); // Who asked for this validation
		
		// Forward the validation results to the different dependency stewards
		for (int i = 0; i < validationResultMessage.getInvalidRhss().length; i++) {
			int rhs = validationResultMessage.getInvalidRhss()[i];
			BitSet[] lhss = validationResultMessage.getInvalidLhss()[i];

			if (this.dependencyStewards[rhs] == null) // If the steward is already done
				continue;
			
			this.dependencyStewardRing.increaseBusy(rhs);
			
			// Update the validation efficiency only for the dependency steward that actually requested this validation
			if (rhs == validationRequester) {
				double validationEfficiency = (double) (validationMessage.getLhss().length - lhss.length) / (double) validationMessage.getLhss().length;
				this.dependencyStewards[rhs].tell(new InvalidFDsMessage(lhss, true, true, validationEfficiency), this.self());
			}
			else {
				this.dependencyStewards[rhs].tell(new InvalidFDsMessage(lhss, true, false, 0), this.self());
			}
		}
		
		// Check if the validation requester is done and, if true, finish that dependency steward; finish the discovery if done entirely
		this.finishStewardIfDone(validationRequester);
		
		// Assign new work to the validator
		this.assign(this.idleValidators.poll());
	}
	
	protected void handle(SamplingResultMessage samplingResultMessage) {
		ActorRef validator = this.sender();
		SamplingMessage samplingMessage = (SamplingMessage) this.busyValidators.remove(validator);
		
		// Update the sampling efficiency for the target attribute
		int attribute = samplingMessage.getAttribute();
		int distance = samplingMessage.getDistance();
		int comparisons = samplingResultMessage.getComparisons();
		int matches = samplingResultMessage.getMatches();
		
		SamplingEfficiency samplingEfficiency = this.samplingEfficiencies[attribute];
		this.prioritizedSamplingEfficiencies.remove(samplingEfficiency);
		samplingEfficiency.update(comparisons, matches, distance);
		this.prioritizedSamplingEfficiencies.add(samplingEfficiency);

		// Forward the sampling results to the different dependency stewards
		for (int i = 0; i < samplingResultMessage.getInvalidRhss().length; i++) {
			int rhs = samplingResultMessage.getInvalidRhss()[i];
			BitSet[] lhss = samplingResultMessage.getInvalidLhss()[i];
			
			if (this.dependencyStewards[rhs] == null) // If the steward is already done
				continue;
			
			this.dependencyStewardRing.increaseBusy(rhs);
			this.dependencyStewards[rhs].tell(new InvalidFDsMessage(lhss, false, true, samplingEfficiency.getEfficiency()), this.self()); // Any SamplingResultMessage is marked to trigger an efficiency update (not only the own sampling requests), because the optimal sampling attribute is chosen independently of the dependency steward's attribute anyway
		}
		
		// The sampling fulfills all sampling preferences for all dependency stewards whether or not they actually issued this sampling request or got an update from it
		this.dependencyStewardRing.setValidation(true);
		
		// Assign new work to the validator
		this.assign(validator);
	}
	
	protected void handle(FDsUpdatedMessage message) {
		// The dependency steward is now one message less busy
		this.dependencyStewardRing.decreaseBusy(message.getRhs());
		
		// The dependency steward may have new candidates now
		this.dependencyStewardRing.setCandidates(message.getRhs(), true);
		
		// If the dependency steward updated its preference, the profiler has to update it, too
		if (message.isUpdatePreference())
			this.dependencyStewardRing.setValidation(message.getRhs(), message.isValidation());
		
		// If the dependency steward is idle now and idle validators exist, we can assign work from this idle dependency steward to one of them; because sending sampling tasks does not make this steward busy, we assign tasks until it gets idle
		while (this.dependencyStewardRing.isIdle(message.getRhs()) && !this.idleValidators.isEmpty())
			this.assign(this.idleValidators.poll());
	}
	
	private void handle(CandidateMessage message) {
		// The dependency steward is now one message less busy
		this.dependencyStewardRing.decreaseBusy(message.getRhs());
		
		// The dependency steward may have no more candidates now
		if (message.getLhss().length == 0)
			this.dependencyStewardRing.setCandidates(message.getRhs(), false);
		
		// Check if the validation requester is done and, if true, finish that dependency steward; finish the discovery if done entirely
		this.finishStewardIfDone(message.getRhs());
		
		// Get the validator that is waiting for this candidate message
		ActorRef validator = this.waitingValidators.poll();
		
		// Assign the validator to somthing else if the candidate message did not deliver candidates
		if ((message.getLhss().length == 0) && (validator != null)) {
			this.assign(validator);
			return;
		}
		
		// Create the validation message
		ValidationMessage validationMessage = new ValidationMessage(message.getLhss(), message.getRhs());
		
		// If the validator terminated while the dependency steward was collecting candidates
		if (validator == null) {
			// Hold the validation message
			this.unassignedWork.add(validationMessage);
		}
		else {
			// Assign the candidates to the waiting validator
			this.busyValidators.put(validator, validationMessage);
			validator.tell(validationMessage, this.self());
		}
		
		// If the dependency steward is idle now and idle validators exist, we can assign work from this idle dependency steward to one of them; because sending sampling tasks does not make this steward busy, we assign tasks until it gets idle
		while (this.dependencyStewardRing.isIdle(message.getRhs()) && !this.idleValidators.isEmpty())
			this.assign(this.idleValidators.poll());
	}
	
	private void assign(ActorRef validator) {
		// Let the validator idle if no discovery task is present yet
		if (this.dataset == null) {
			this.idleValidators.add(validator);
			return;
		}
		
		// Assign work that is waiting for a worker
		Object work = this.unassignedWork.poll();
		if (work != null) {
			validator.tell(work, this.self());
			this.busyValidators.put(validator, work);
			return;
		}
		
		// Assign sampling tasks for attributes that have not yet been used for sampling
		if (!this.prioritizedSamplingEfficiencies.peek().hasStepped()) {
			SamplingEfficiency samplingEfficiency = this.prioritizedSamplingEfficiencies.poll();
			int distance = samplingEfficiency.step();
			this.prioritizedSamplingEfficiencies.add(samplingEfficiency);
			
			SamplingMessage samplingMessage = new SamplingMessage(samplingEfficiency.getAttribute(), distance);
			validator.tell(samplingMessage, this.self());
			this.busyValidators.put(validator, samplingMessage);
			return;
		}
		
		// Try to assign a validation task
		int attribute = this.dependencyStewardRing.nextIdleWithValidationPreferenceAndCandidates();
		if (attribute >= 0) {
			this.dependencyStewardRing.increaseBusy(attribute);
			this.dependencyStewards[attribute].tell(new CandidateRequestMessage(), this.self());
			
			this.waitingValidators.add(validator);
			return;
		}
		
		// Try to assign a sampling task
		attribute = this.dependencyStewardRing.nextIdleWithSamplingPreference();
		if (attribute >= 0) {
			SamplingEfficiency samplingEfficiency = this.prioritizedSamplingEfficiencies.poll();
			int distance = samplingEfficiency.step();
			this.prioritizedSamplingEfficiencies.add(samplingEfficiency);
			
			this.dependencyStewardRing.setValidation(attribute, true); // We fulfilled the sampling wish and set the preference to validation; otherwise we would sample forever, if the sampling does not discover any further nonFDs (which were needed to change the preference)
			
			SamplingMessage samplingMessage = new SamplingMessage(samplingEfficiency.getAttribute(), distance);
			validator.tell(samplingMessage, this.self());
			this.busyValidators.put(validator, samplingMessage);
			return;
		}
		
		// Let the validator idle if all dependency stewards are busy (i.e., we apply backpressure to not exhaust the profiler and its dependency stewards)
		this.idleValidators.add(validator);
	}

	private boolean finishStewardIfDone(int stewardAttribute) {
		// Check if the dependency steward is actually done
		if (!this.isDone(stewardAttribute))
			return false;
		
		// Tell the dependency steward to finalize, i.e., write results and terminate	
		this.dependencyStewards[stewardAttribute].tell(new FinalizeMessage(this.dataset), this.self());

		// Make the dependency steward permanently busy so that it is never asked for more candidates
		this.dependencyStewardRing.increaseBusy(stewardAttribute);
		
		// Remove the dependency steward from the local list so that we do not forward any further results to it
		this.dependencyStewards[stewardAttribute] = null;
		
/*		System.out.println("Done " + stewardAttribute);
		for (int i = 0; i < this.dataset.getNumAtrributes(); i++)
			if (this.dependencyStewards[i] == null)
				System.out.print(1);
			else
				System.out.print(0);
		System.out.println(" done");
		for (int i = 0; i < this.dataset.getNumAtrributes(); i++)
			if (this.dependencyStewardRing.isCandidates(i))
				System.out.print(1);
			else
				System.out.print(0);
		System.out.println(" candidates");
		for (int i = 0; i < this.dataset.getNumAtrributes(); i++)
			if (this.dependencyStewardRing.isBusy(i))
				System.out.print(1);
			else
				System.out.print(0);
		System.out.println(" busy");
		for (int i = 0; i < this.dataset.getNumAtrributes(); i++)
			if (this.dependencyStewardRing.isValidation(i))
				System.out.print(1);
			else
				System.out.print(0);
		System.out.println(" validation");
*/		
		return true;
	}
	
	private boolean isDone(int attribute) {
		// A dependency steward that is done will eventually send an empty candidate message, because ...
		// - it will be idle at some point (= all messages are processed)
		//   - the profiler will at some point ask for candidates, because the sampling-validation strategy always comes back to validation once a sampling wish has been fulfilled
		
		// A dependency steward is done if the following three conditions are fulfilled:
		// (1) it has no more candidates, i.e., it has sent an empty candidate message (= no candidates need validation)
		// (2) it has no work in progress, i.e., is idle (= no new results are on their way and may create new candidates)
		// (3) it has no validation in progress (= no candidates are being validated and could potentially be non-FDs)
		
		// Check if the dependency steward has no more candidates
		if (this.dependencyStewardRing.isCandidates(attribute))
			return false;
		
		// Check if the dependency steward is idle
		if (this.dependencyStewardRing.isBusy(attribute))
			return false;
		
		// Check if any validator is validating candidates from the dependency steward
		for (Object task : this.busyValidators.values())
			if ((task instanceof ValidationMessage) && ((ValidationMessage) task).getRhs() == attribute)
				return false;
		
		return true;
	}
}