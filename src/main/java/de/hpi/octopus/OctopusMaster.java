package de.hpi.octopus;

import java.util.Arrays;
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
import de.hpi.octopus.actors.slaves.Indexer;
import de.hpi.octopus.actors.slaves.Worker;
import de.hpi.octopus.configuration.Configuration;
import de.hpi.octopus.configuration.ConfigurationSingleton;
import de.hpi.octopus.testing.TestActor;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

public class OctopusMaster extends OctopusSystem {
	
	public static final String MASTER_ROLE = "master";

	public static void start() {
		final Configuration c = ConfigurationSingleton.get();
		final Config config = createConfiguration(c.getActorSystemName(), MASTER_ROLE, c.getHost(), c.getPort(), c.getMasterHost(), c.getMasterPort());
		final ActorSystem system = createSystem(c.getActorSystemName(), config);
		
		ActorRef reaper = system.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);
		
	//	ActorRef clusterListener = system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
	//	ActorRef metricsListener = system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);
		ActorRef progressListener = system.actorOf(ProgressListener.props(), ProgressListener.DEFAULT_NAME);
		
		ActorRef preprocessor = system.actorOf(Preprocessor.props(), Preprocessor.DEFAULT_NAME);

		ActorRef profiler = system.actorOf(Profiler.props(), Profiler.DEFAULT_NAME);
		
		
		Cluster.get(system).registerOnMemberUp(new Runnable() {
			@Override
			public void run() {
				
				for (int i = 0; i < c.getNumWorkers(); i++)
					system.actorOf(Indexer.props(), Indexer.DEFAULT_NAME + i);

				if (c.getNumWorkers() > 0) {
					ActorRef storekeeper = system.actorOf(Storekeeper.props(), Storekeeper.DEFAULT_NAME);
				
					for (int i = 0; i < c.getNumWorkers(); i++)
						system.actorOf(Worker.props(storekeeper), Worker.DEFAULT_NAME + i);
				}
				
			//	ActorRef testActor1 = system.actorOf(TestActor.props(null), TestActor.DEFAULT_NAME + 1);
			//	testActor1.tell("hi", ActorRef.noSender());
			//	ActorRef testActor2 = system.actorOf(TestActor.props(testActor1), TestActor.DEFAULT_NAME + 2);
			//	int[] data = {1,2,3};
			//	testActor2.tell(new TestActor.TestMessage(data), ActorRef.noSender());
				
			//	ActorRef testActor1 = system.actorOf(TestActor.props(null), TestActor.DEFAULT_NAME + 1);
			//	ActorRef testActor2 = system.actorOf(TestActor.props(testActor1), TestActor.DEFAULT_NAME + 2);
			//	testActor2.tell("Hello", ActorRef.noSender());
				
			//	for (int i = 0; i < 12; i++) {
			//		ActorRef testActor = system.actorOf(TestActor.props(null), TestActor.DEFAULT_NAME + i);
			//		testActor.tell(new TestActor.MakeBusyMessage(), ActorRef.noSender());
			//	}
			//	ActorRef testActor = system.actorOf(TestActor.props(null), TestActor.DEFAULT_NAME);
			//	for (int i = 0; i < 100; i++) {
			//		testActor.tell(new TestActor.PingMessage(), ActorRef.noSender());
			//	}
				
				if (!c.isStartPaused())
					system.actorSelection("/user/" + Preprocessor.DEFAULT_NAME).tell(new PreprocessingTaskMessage(), ActorRef.noSender());
			}
		});
		
		if (c.isStartPaused()) {
			try (final Scanner scanner = new Scanner(System.in)) {
				String line = scanner.nextLine();
				System.out.println(line);
			}
			
			system.actorSelection("/user/" + Preprocessor.DEFAULT_NAME).tell(new PreprocessingTaskMessage(), ActorRef.noSender());
		}
		
/*		while (true) {
			try {
				Await.ready(system.whenTerminated(), Duration.create(1, TimeUnit.MINUTES));
				break;
			} catch (TimeoutException | InterruptedException e) {
			}
		}
		
		System.out.println("The end!");
*/	}
}
