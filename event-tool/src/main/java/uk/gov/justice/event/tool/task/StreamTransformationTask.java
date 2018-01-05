package uk.gov.justice.event.tool.task;

import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.service.EventStreamTransformationService;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import javax.enterprise.concurrent.ManagedTask;
import javax.enterprise.concurrent.ManagedTaskListener;

public class StreamTransformationTask implements Callable<UUID>, ManagedTask {

    Stream<JsonEnvelope> stream;

    private EventStreamTransformationService eventStreamTransformationService;

    private ManagedTaskListener transformationListener;

    public StreamTransformationTask(final Stream<JsonEnvelope> stream, final EventStreamTransformationService eventStreamTransformationService, final ManagedTaskListener transformationListener) {
        this.eventStreamTransformationService = eventStreamTransformationService;
        this.transformationListener = transformationListener;
        this.stream = stream;
    }

    @Override
    public UUID call() throws EventStreamException {
        return eventStreamTransformationService.transformEventStream(this.stream);
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