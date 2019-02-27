package de.hpi.octopus;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

import com.typesafe.config.Config;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import de.hpi.octopus.actors.Storekeeper;
import de.hpi.octopus.actors.listeners.ProgressListener;
import de.hpi.octopus.actors.masters.Preprocessor;
import de.hpi.octopus.actors.masters.Profiler;
import de.hpi.octopus.actors.slaves.Indexer;
import de.hpi.octopus.actors.slaves.Validator;
import de.hpi.octopus.structures.DatasetDescriptor;

public class OctopusMaster extends OctopusSystem {
	
	public static final String MASTER_ROLE = "master";

	public static void start(String actorSystemName, int workers, String host, int port) {

		final Config config = createConfiguration(actorSystemName, MASTER_ROLE, host, port, host, port);
		
		final ActorSystem system = createSystem(actorSystemName, config);
		
		Cluster.get(system).registerOnMemberUp(new Runnable() {
			@Override
			public void run() {
			//	ActorRef clusterListener = system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
			//	ActorRef metricsListener = system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);
				ActorRef progressListener = system.actorOf(ProgressListener.props(), ProgressListener.DEFAULT_NAME);
				
				ActorRef preprocessor = system.actorOf(Preprocessor.props(), Preprocessor.DEFAULT_NAME);

				for (int i = 0; i < workers; i++)
					system.actorOf(Indexer.props(), Indexer.DEFAULT_NAME + i);

				ActorRef storekeeper = system.actorOf(Storekeeper.props(), Storekeeper.DEFAULT_NAME);
				
				ActorRef profiler = system.actorOf(Profiler.props(), Profiler.DEFAULT_NAME);
				
				for (int i = 0; i < workers; i++)
					system.actorOf(Validator.props(storekeeper), Validator.DEFAULT_NAME + i);
				
			//	ActorRef testActor1 = system.actorOf(TestActor.props(null), TestActor.DEFAULT_NAME + 1);
			//	testActor1.tell("hi", ActorRef.noSender());
			//	ActorRef testActor2 = system.actorOf(TestActor.props(testActor1), TestActor.DEFAULT_NAME + 2);
			//	int[] data = {1,2,3};
			//	testActor2.tell(new TestActor.TestMessage(data), ActorRef.noSender());
				
			//	int maxInstancesPerNode = workers; // TODO: Every node gets the same number of workers, so it cannot be a parameter for the slave nodes
			//	Set<String> useRoles = new HashSet<>(Arrays.asList("master", "slave"));
			//	ActorRef router = system.actorOf(
			//		new ClusterRouterPool(
			//			new AdaptiveLoadBalancingPool(SystemLoadAverageMetricsSelector.getInstance(), 0),
			//			new ClusterRouterPoolSettings(10000, workers, true, new HashSet<>(Arrays.asList("master", "slave"))))
			//		.props(Props.create(Worker.class)), "router");
			}
		});
		
		final Scanner scanner = new Scanner(System.in);
		String line = scanner.nextLine();
		System.out.println(line);
		
		DatasetDescriptor dataset = new DatasetDescriptor(
				"ncvoter_Statewide_10001r_71c",//"ncvoter_Statewide_1024001r_71c", 
				"/home/thorsten/Data/Development/workspace/papenbrock/HyFDTestRunner/data/", ".csv",
				true, StandardCharsets.UTF_8, ',', '"', '\\', "", false, true, 100, 0, true);
		
		system.actorSelection("/user/" + Preprocessor.DEFAULT_NAME).tell(new Preprocessor.PreprocessingTaskMessage(dataset), ActorRef.noSender());
		
		//system.actorSelection("/user/" + Profiler.DEFAULT_NAME).tell(new Profiler.DiscoveryTaskMessage(attributes, null, null), ActorRef.noSender());
		
		line = scanner.nextLine();
		System.out.println(line);
		scanner.close();
		
		system.terminate();
	}
}
