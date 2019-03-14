package uk.gov.justice.event.tool;

import uk.gov.justice.tools.eventsourcing.transformation.EventTransformationRegistry;

import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class PassesDeterminer {

    @Inject
    private EventTransformationRegistry eventTransformationRegistry;

    private AtomicInteger passValue = new AtomicInteger(1);

    public int getNextPassValue() {
        return passValue.incrementAndGet();
    }

    public int getPassValue() {
        return passValue.get();
    }


    public boolean isLastElementInPasses() {
        return eventTransformationRegistry.getPasses().size() == passValue.get();
    }

    public int passCount(){
       return eventTransformationRegistry.getPasses().size();
    }
}


