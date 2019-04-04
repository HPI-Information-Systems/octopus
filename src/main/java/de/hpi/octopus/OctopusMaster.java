package de.hpi.octopus;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.typesafe.config.Config;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import de.hpi.octopus.actors.Reaper;
import de.hpi.octopus.actors.Storekeeper;
import de.hpi.octopus.actors.listeners.ProgressListener;
import de.hpi.octopus.actors.masters.Preprocessor;
import de.hpi.octopus.actors.masters.Preprocessor.PreprocessingTaskMessage;
import de.hpi.octopus.actors.masters.Profiler;
import de.hpi.octopus.actors.masters.Profiler2;
import de.hpi.octopus.actors.slaves.Indexer;
import de.hpi.octopus.actors.slaves.Validator;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.FunctionalDependencyResultReceiver;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

public class OctopusMaster extends OctopusSystem {
	
	public static final String MASTER_ROLE = "master";

	public static void start(String actorSystemName, int workers, String host, int port, RelationalInputGenerator relationalInputGenerator, FunctionalDependencyResultReceiver resultReceiver, boolean startPaused) {

		final Config config = createConfiguration(actorSystemName, MASTER_ROLE, host, port, host, port);
		
		final ActorSystem system = createSystem(actorSystemName, config);
		
		Cluster.get(system).registerOnMemberUp(new Runnable() {
			@Override
			public void run() {
				ActorRef reaper = system.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);
				
			//	ActorRef clusterListener = system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
			//	ActorRef metricsListener = system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);
				ActorRef progressListener = system.actorOf(ProgressListener.props(resultReceiver), ProgressListener.DEFAULT_NAME);
				
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
				
				if (!startPaused) {
					int bufferSize = 100; // TODO: make parameter
					system.actorSelection("/user/" + Preprocessor.DEFAULT_NAME).tell(new PreprocessingTaskMessage(relationalInputGenerator, bufferSize), ActorRef.noSender());
				}
			}
		});
		
		if (startPaused) {
			try (final Scanner scanner = new Scanner(System.in)) {
				String line = scanner.nextLine();
				System.out.println(line);
			}
			
			int bufferSize = 100; // TODO: make parameter
			system.actorSelection("/user/" + Preprocessor.DEFAULT_NAME).tell(new PreprocessingTaskMessage(relationalInputGenerator, bufferSize), ActorRef.noSender());
		}
		
		while (true) {
			try {
				Await.ready(system.whenTerminated(), Duration.create(1, TimeUnit.MINUTES));
				break;
			} catch (TimeoutException | InterruptedException e) {
			}
		}
		
		System.out.println("The end!");
	}
}
