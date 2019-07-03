package de.hpi.octopus.actors;

import static org.junit.Assert.assertTrue;

import java.time.Duration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.typesafe.config.Config;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.testkit.javadsl.TestKit;
import de.hpi.octopus.OctopusSystem;
import de.hpi.octopus.actors.slaves.AbstractSlave;

public class LargeMessageProxyTest {

	static ActorSystem system;

	static class TestActor extends AbstractSlave {

		public static Props props(ActorRef target) {
			return Props.create(TestActor.class, () -> new TestActor(target));
		}

		public TestActor(ActorRef target) {
			this.target = target;
		}

		ActorRef target = null;

		@Override
		public Receive createReceive() {
			return receiveBuilder()
					.match(LargeMessageProxy.LargeMessage.class, message -> this.largeMessageProxy.tell(message, this.self()))
					.match(Object.class, message -> this.target.tell(message, this.self()))
					.build();
		}

		@Override
		protected String getName() {
			return "testActor";
		}

		@Override
		protected String getMasterName() {
			return "";
		}

		@Override
		protected void handle(TerminateMessage message) {
			this.self().tell(PoisonPill.getInstance(), this.self());
		}
	}

	@Before
	public void setUp() throws Exception {
		Config config = OctopusSystem.createConfiguration("TestSystem", "master", "localhost", 7877, "localhost", 7877);
		system = OctopusSystem.createSystem("TestSystem", config);
		
		system.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);
	}

	@After
	public void tearDown() throws Exception {
		TestKit.shutdownActorSystem(system);
		system = null;
	}

/*	@Test
	public void testPoxiesCreated() {
		new TestKit(system) {
			{
				ActorRef sender = system.actorOf(TestActor.props(this.getRef()), "sender");
				ActorRef receiver = system.actorOf(TestActor.props(this.getRef()), "receiver");
				
				this.expectMsgClass(CurrentClusterState.class);
				this.expectMsgClass(CurrentClusterState.class);
				
				within(Duration.ofSeconds(1), () -> {
					// Test if both proxy actors are created and respond to messages
					ActorSelection senderProxy = system.actorSelection(sender.path().child(LargeMessageProxy.DEFAULT_NAME));
					ActorSelection receiverProxy = system.actorSelection(receiver.path().child(LargeMessageProxy.DEFAULT_NAME));
					
					senderProxy.tell(new LargeMessageProxy.RequestBytesMessage(receiver), this.getRef());
					this.expectMsg(new BytesMessage(new byte[0], sender));
					
					receiverProxy.tell(new LargeMessageProxy.RequestBytesMessage(sender), this.getRef());
					this.expectMsg(new BytesMessage(new byte[0], receiver));
					
					// Will wait for the rest of the within duration
					expectNoMessage();
					return null;
				});
			}
		};
	}
*/	
	@Test
	public void testSmallMessageSending() {
		new TestKit(system) {
			{
				ActorRef sender = system.actorOf(TestActor.props(this.getRef()), "sender");
				ActorRef receiver = system.actorOf(TestActor.props(this.getRef()), "receiver");
				
				this.expectMsgClass(CurrentClusterState.class);
				this.expectMsgClass(CurrentClusterState.class);
				
				within(Duration.ofSeconds(1), () -> {
					// Test if a small message gets passed from one proxy to the other
					String shortMessage = "Hello, this is a short message!";
					LargeMessageProxy.LargeMessage<String> shortStringMessage = new LargeMessageProxy.LargeMessage<String>(shortMessage, receiver, true);
					
					sender.tell(shortStringMessage, this.getRef()); // Tell the TestActor to send a large message via its large message proxy to the receiver
					this.expectMsg(shortMessage);
					assertTrue(this.getLastSender().equals(receiver));
					
					// Will wait for the rest of the within duration
					expectNoMessage();
					return null;
				});
			}
		};
	}
	
	@Test
	public void testLargeMessageSending() {
		new TestKit(system) {
			{
				ActorRef sender = system.actorOf(TestActor.props(this.getRef()), "sender");
				ActorRef receiver = system.actorOf(TestActor.props(this.getRef()), "receiver");
				
				this.expectMsgClass(CurrentClusterState.class);
				this.expectMsgClass(CurrentClusterState.class);
				
				within(Duration.ofSeconds(2), () -> {
					// Test if a large message gets passed from one proxy to the other
					StringBuffer longMessageBuffer = new StringBuffer("Hello, this is a String message with a very large payload!");
					for (int i = 0; i < 1000; i++)
						longMessageBuffer.append("<content>");
					String longMessage = longMessageBuffer.toString();
					LargeMessageProxy.LargeMessage<String> longStringMessage = new LargeMessageProxy.LargeMessage<String>(longMessage, receiver, true);
					
					sender.tell(longStringMessage, this.getRef()); // Tell the TestActor to send a large message via its large message proxy to the receiver
					this.expectMsg(longMessage);
					assertTrue(this.getLastSender().equals(receiver));
					
					// Will wait for the rest of the within duration
					expectNoMessage();
					return null;
				});
			}
		};
	}
}
