package de.hpi.octopus;

import com.typesafe.config.Config;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import de.hpi.octopus.actors.Reaper;
import de.hpi.octopus.actors.Storekeeper;
import de.hpi.octopus.actors.slaves.Indexer;
import de.hpi.octopus.actors.slaves.Validator;
import de.hpi.octopus.configuration.Configuration;
import de.hpi.octopus.configuration.ConfigurationSingleton;

public class OctopusSlave extends OctopusSystem {

	public static final String SLAVE_ROLE = "slave";
	
	public static void start() {
		final Configuration c = ConfigurationSingleton.get();
		final Config config = createConfiguration(c.getActorSystemName(), SLAVE_ROLE, c.getHost(), c.getPort(), c.getMasterHost(), c.getMasterPort());
		final ActorSystem system = createSystem(c.getActorSystemName(), config);
		
		Cluster.get(system).registerOnMemberUp(new Runnable() {
			@Override
			public void run() {
				ActorRef reaper = system.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);
				
			//	ActorRef clusterListener = system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
			//	ActorRef metricsListener = system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);

				for (int i = 0; i < c.getNumWorkers(); i++)
					system.actorOf(Indexer.props(), Indexer.DEFAULT_NAME + i);

				if (c.getNumWorkers() > 0) {
					ActorRef storekeeper = system.actorOf(Storekeeper.props(), Storekeeper.DEFAULT_NAME);
					
					for (int i = 0; i < c.getNumWorkers(); i++)
						system.actorOf(Validator.props(storekeeper), Validator.DEFAULT_NAME + i);
				}
			}
		});
	}
}
