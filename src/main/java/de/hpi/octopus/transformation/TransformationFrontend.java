package de.hpi.octopus.transformation;

import static de.hpi.octopus.transformation.TransformationMessages.BACKEND_REGISTRATION;

import java.util.ArrayList;
import java.util.List;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Terminated;
import de.hpi.octopus.transformation.TransformationMessages.JobFailed;
import de.hpi.octopus.transformation.TransformationMessages.TransformationJob;

public class TransformationFrontend extends AbstractActor {

  List<ActorRef> backends = new ArrayList<>();
  int jobCounter = 0;

  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(TransformationJob.class, job -> backends.isEmpty(), job -> {
        sender().tell(new JobFailed("Service unavailable, try again later", job),
          sender());
      })
      .match(TransformationJob.class, job -> {
        jobCounter++;
        backends.get(jobCounter % backends.size())
          .forward(job, getContext());
      })
      .matchEquals(BACKEND_REGISTRATION, message -> {
        getContext().watch(sender());
        backends.add(sender());
      })
      .match(Terminated.class, terminated -> {
        backends.remove(terminated.getActor());
      })
      .build();
  }
}
