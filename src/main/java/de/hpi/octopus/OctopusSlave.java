package de.hpi.octopus;

import com.typesafe.config.Config;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import de.hpi.octopus.actors.Reaper;
import de.hpi.octopus.actors.Storekeeper;
import de.hpi.octopus.actors.slaves.Indexer;
import de.hpi.octopus.actors.slaves.Validator;

public class OctopusSlave extends OctopusSystem {

	public static final String SLAVE_ROLE = "slave";
	
	public static void start(String actorSystemName, int workers, String host, int port, String masterhost, int masterport) {
		
		final Config config = createConfiguration(actorSystemName, SLAVE_ROLE, host, port, masterhost, masterport);
		
		final ActorSystem system = createSystem(actorSystemName, config);
		
		Cluster.get(system).registerOnMemberUp(new Runnable() {
			@Override
			public void run() {
				ActorRef reaper = system.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);
				
			//	ActorRef clusterListener = system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
			//	ActorRef metricsListener = system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);

				for (int i = 0; i < workers; i++)
					system.actorOf(Indexer.props(), Indexer.DEFAULT_NAME + i);

				if (workers > 0) {
					ActorRef storekeeper = system.actorOf(Storekeeper.props(), Storekeeper.DEFAULT_NAME);
					
					for (int i = 0; i < workers; i++)
						system.actorOf(Validator.props(storekeeper), Validator.DEFAULT_NAME + i);
				}
			}
		});
	}
}
