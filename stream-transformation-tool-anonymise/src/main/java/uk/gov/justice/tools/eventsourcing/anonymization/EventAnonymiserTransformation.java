package uk.gov.justice.tools.eventsourcing.anonymization;

import static java.util.stream.Stream.of;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.anonymization.service.EventAnonymiserService;
import uk.gov.justice.tools.eventsourcing.transformation.api.Action;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

import java.util.stream.Stream;

import javax.json.JsonObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class EventAnonymiserTransformation implements EventTransformation {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventAnonymiserTransformation.class);

    private Enveloper enveloper;

    private EventAnonymiserService eventAnonymiserService;


    public EventAnonymiserTransformation() {
        this(new EventAnonymiserService());
    }

    public EventAnonymiserTransformation(final EventAnonymiserService eventAnonymiserService) {
        this.eventAnonymiserService = eventAnonymiserService;
    }

    @Override
    public Action actionFor(final JsonEnvelope eventEnvelope) {
        return new Action(true, false, false);
    }

    @Override
    public Stream<JsonEnvelope> apply(final JsonEnvelope sourceJsonEnvelope) {

        final JsonObject transformedPayload = buildTransformedPayload(sourceJsonEnvelope);
        final JsonEnvelope transformedEnvelope = envelopeFrom(sourceJsonEnvelope.metadata(), transformedPayload);
        return of(transformedEnvelope);
    }

    @Override
    public void setEnveloper(final Enveloper enveloper) {
        // not used
    }

    private JsonObject buildTransformedPayload(final JsonEnvelope jsonEnvelope) {
        final String eventName = jsonEnvelope.metadata().name();
        final JsonObject payload = jsonEnvelope.payloadAsJsonObject();
        LOGGER.debug("Original payload: {}", payload);
        final JsonObject transformedPayload = eventAnonymiserService.anonymiseObjectPayload(payload, eventName);
        LOGGER.debug("Transformed payload : {}", transformedPayload);
        return transformedPayload;

    }

}
