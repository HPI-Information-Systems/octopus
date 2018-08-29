package de.hpi.octopus;

import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import de.hpi.octopus.actors.listeners.ClusterListener;
import de.hpi.octopus.actors.listeners.MetricsListener;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

public class OctopusMaster {

	public static void start(String actorSystemName, String actorSystemRole, int workers, String host, int port) {
		
		final Config config = ConfigFactory
				.parseString(
						"akka.remote.netty.tcp.hostname = \"" + host + "\"\n" +
						"akka.remote.netty.tcp.port = " + port + "\n" + 
						"akka.remote.artery.canonical.hostname = \"" + host + "\"\n" +
						"akka.remote.artery.canonical.port = " + port + "\n" +
						"akka.cluster.roles = [" + actorSystemRole + "]\n" +
						"akka.cluster.seed-nodes = [\"akka://" + actorSystemName + "@" + host + ":" + port + "\"]")
				.withFallback(ConfigFactory.load("octopus"));

		final ActorSystem system = ActorSystem.create(actorSystemName, config);
		
		system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
		//system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);
		
		for (int i = 0; i < workers; i++) {
			System.out.println("Start Worker " + i);
		}
		
		
		
		
		Cluster.get(system).registerOnMemberRemoved(new Runnable() {
			@Override
			public void run() {
				final Runnable exit = new Runnable() {
					@Override
					public void run() {
						System.exit(0);
					}
				};
				system.registerOnTermination(exit);
				system.terminate();

				new Thread() {
					@Override
					public void run() {
						try {
							Await.ready(system.whenTerminated(), Duration.create(10, TimeUnit.SECONDS));
						} catch (Exception e) {
							System.exit(-1);
						}

					}
				}.start();
			}
		});
	}
}
