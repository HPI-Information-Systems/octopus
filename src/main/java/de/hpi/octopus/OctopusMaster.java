package de.hpi.octopus;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import de.hpi.octopus.actors.Profiler;
import de.hpi.octopus.actors.Worker;
import de.hpi.octopus.actors.listeners.ClusterListener;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

public class OctopusMaster {
	
	public static final String MASTER_ROLE = "master";

	public static void start(String actorSystemName, int workers, String host, int port) {

		final Config config = ConfigFactory.parseString(
				"akka.remote.netty.tcp.hostname = \"" + host + "\"\n" + 
				"akka.remote.netty.tcp.port = " + port + "\n" + 
				"akka.remote.artery.canonical.hostname = \"" + host + "\"\n" + 
				"akka.remote.artery.canonical.port = " + port + "\n" + 
				"akka.cluster.roles = [" + MASTER_ROLE + "]\n" + 
				"akka.cluster.seed-nodes = [\"akka://" + actorSystemName + "@" + host + ":" + port + "\"]")
			.withFallback(ConfigFactory.load("octopus"));

		final ActorSystem system = ActorSystem.create(actorSystemName, config);
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
				system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
			//	system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);

				system.actorOf(Profiler.props(), Profiler.DEFAULT_NAME);
				
				for (int i = 0; i < workers; i++)
					system.actorOf(Worker.props(), Worker.DEFAULT_NAME + i);
				
			//	int maxInstancesPerNode = workers; // TODO: Every node gets the same number of workers, so it cannot be a parameter for the slave nodes
			//	Set<String> useRoles = new HashSet<>(Arrays.asList("master", "slave"));
			//	ActorRef router = system.actorOf(
			//		new ClusterRouterPool(
			//			new AdaptiveLoadBalancingPool(
			//				SystemLoadAverageMetricsSelector.getInstance(), 0),
			//				new ClusterRouterPoolSettings(10000, workers, true, new HashSet<>(Arrays.asList("master", "slave"))))
			//		.props(Props.create(Worker.class)), "router");
			}
		});
		
		final Scanner scanner = new Scanner(System.in);
		String line = scanner.nextLine();
		
		system.actorSelection("/user/" + Profiler.DEFAULT_NAME).tell(new Profiler.TaskMessage(10), ActorRef.noSender());
	}
}
