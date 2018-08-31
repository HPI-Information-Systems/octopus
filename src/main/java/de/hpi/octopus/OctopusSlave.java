package de.hpi.octopus;

import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import de.hpi.octopus.actors.Worker;
import de.hpi.octopus.actors.listeners.MetricsListener;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

public class OctopusSlave {

	public static final String SLAVE_ROLE = "slave";
	
	public static void start(String actorSystemName, int workers, String host, int port, String masterhost, int masterport) {
		
		final Config config = ConfigFactory.parseString(
				"akka.remote.netty.tcp.hostname = \"" + host + "\"\n" +
				"akka.remote.netty.tcp.port = " + port + "\n" + 
				"akka.remote.artery.canonical.hostname = \"" + host + "\"\n" +
				"akka.remote.artery.canonical.port = " + port + "\n" +
				"akka.cluster.roles = [" + SLAVE_ROLE + "]\n" +
				"akka.cluster.seed-nodes = [\"akka://" + actorSystemName + "@" + masterhost + ":" + masterport + "\"]")
			.withFallback(ConfigFactory.load("octopus"));
		
		ActorSystem system = ActorSystem.create(actorSystemName, config);
		system.registerOnTermination(new Runnable() {
			@Override
			public void run() {
				System.exit(0);
			}
		});
		
		Cluster.get(system).registerOnMemberRemoved(new Runnable() {
			@Override
			public void run() {
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
		
		Cluster.get(system).registerOnMemberUp(new Runnable() {
			@Override
			public void run() {
				//system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
				system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);

				for (int i = 0; i < workers; i++)
					system.actorOf(Worker.props(), Worker.DEFAULT_NAME + i);
			}
		});
	}
}
