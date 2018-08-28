package de.hpi.octopus;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSystem;
import akka.actor.Props;
import de.hpi.octopus.factorial.FactorialBackend;
import de.hpi.octopus.factorial.MetricsListener;

public class OctopusSlave {

	public static void start(int workers, int port, int masterport, int masterhost) {
		final Config config = ConfigFactory
				.parseString("akka.remote.netty.tcp.port=" + port + "\n" + "akka.remote.artery.canonical.port=" + port)
				.withFallback(ConfigFactory.parseString("akka.cluster.roles = [slave]"))
				.withFallback(ConfigFactory.load("octopus"));

		ActorSystem system = ActorSystem.create("ClusterSystem", config);

		system.actorOf(Props.create(FactorialBackend.class), "factorialBackend");

		system.actorOf(Props.create(MetricsListener.class), "metricsListener");
	}

}
