package de.hpi.octopus;

import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import de.hpi.octopus.actors.OctopusClusterListener;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

public class OctopusMaster {

	public static void start(int workers, int port) {
		
		final Config config = ConfigFactory
				.parseString("akka.cluster.roles = [master]")
				.withFallback(ConfigFactory.load("octopus"));

		final ActorSystem system = ActorSystem.create("OctopusSystem", config);
		
		system.actorOf(OctopusClusterListener.props(), OctopusClusterListener.DEFAULT_NAME);
		
		
		
		
		
		
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
