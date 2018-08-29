package de.hpi.octopus;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSystem;
import akka.actor.Props;
import de.hpi.octopus.actors.listeners.ClusterListener;
import de.hpi.octopus.actors.listeners.MetricsListener;
import de.hpi.octopus.factorial.FactorialBackend;

public class OctopusSlave {

	public static void start(String actorSystemName, String actorSystemRole, int workers, String host, int port, String masterhost, int masterport) {
		
		final Config config = ConfigFactory
				.parseString(
						"akka.remote.netty.tcp.hostname = \"" + host + "\"\n" +
						"akka.remote.netty.tcp.port = " + port + "\n" + 
						"akka.remote.artery.canonical.hostname = \"" + host + "\"\n" +
						"akka.remote.artery.canonical.port = " + port + "\n" +
						"akka.cluster.roles = [" + actorSystemRole + "]\n" +
						"akka.cluster.seed-nodes = [\"akka://" + actorSystemName + "@" + masterhost + ":" + masterport + "\"]")
				.withFallback(ConfigFactory.load("octopus"));
		
		ActorSystem system = ActorSystem.create(actorSystemName, config);

		//system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
		system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);
		
		for (int i = 0; i < workers; i++) {
			System.out.println("Start Worker " + i);
		}
		
		
	}

}
