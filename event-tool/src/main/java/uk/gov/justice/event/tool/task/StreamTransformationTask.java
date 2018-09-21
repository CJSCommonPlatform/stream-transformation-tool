package uk.gov.justice.event.tool.task;

import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.tools.eventsourcing.transformation.service.EventStreamTransformationService;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import javax.enterprise.concurrent.ManagedTask;
import javax.enterprise.concurrent.ManagedTaskListener;

public class StreamTransformationTask implements Callable<UUID>, ManagedTask {

    private final UUID streamId;

    private EventStreamTransformationService eventStreamTransformationService;

    private ManagedTaskListener transformationListener;

    private int pass;

    public StreamTransformationTask(final UUID streamId, final EventStreamTransformationService eventStreamTransformationService, final ManagedTaskListener transformationListener , final int pass) {
        this.eventStreamTransformationService = eventStreamTransformationService;
        this.transformationListener = transformationListener;
        this.streamId = streamId;
        this.pass = pass;
    }

    @Override
    public UUID call() throws EventStreamException {
        return eventStreamTransformationService.transformEventStream(this.streamId, pass);
    }

    @Override
    public Map<String, String> getExecutionProperties() {
        return null;
    }

    @Override
    public ManagedTaskListener getManagedTaskListener() {
        return transformationListener;
    }
}