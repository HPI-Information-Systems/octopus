package de.hpi.octopus.transformation;

import static de.hpi.octopus.transformation.TransformationMessages.BACKEND_REGISTRATION;

import akka.actor.AbstractActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.octopus.transformation.TransformationMessages.TransformationJob;
import de.hpi.octopus.transformation.TransformationMessages.TransformationResult;

public class TransformationBackend extends AbstractActor {

  Cluster cluster = Cluster.get(getContext().system());

  //subscribe to cluster changes, MemberUp
  @Override
  public void preStart() {
    cluster.subscribe(self(), MemberUp.class);
  }

  //re-subscribe when restart
  @Override
  public void postStop() {
    cluster.unsubscribe(self());
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(TransformationJob.class, job -> {
        sender().tell(new TransformationResult(job.getText().toUpperCase()),
          self());
      })
      .match(CurrentClusterState.class, state -> {
        for (Member member : state.getMembers()) {
          if (member.status().equals(MemberStatus.up())) {
            register(member);
          }
        }
      })
      .match(MemberUp.class, mUp -> {
        register(mUp.member());
      })
      .build();
  }

  void register(Member member) {
    if (member.hasRole("frontend"))
      getContext().actorSelection(member.address() + "/user/frontend").tell(
          BACKEND_REGISTRATION, self());
  }
}
