package uk.gov.sample.event.transformation.pass;

import static java.util.UUID.fromString;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.Action;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;

import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

@Transformation(pass = 9)
public class SampleTransformationSequenceScenario3 implements EventTransformation {

    private Enveloper enveloper;

    @Override
    public Action actionFor(final JsonEnvelope event) {
        if (event.metadata().name().equalsIgnoreCase("sample.events.name.pass1.sequence3")) {
            return new Action(true, false, false);
        }
        return NO_ACTION;
    }

    @Override
    public Stream<JsonEnvelope> apply(final JsonEnvelope event) {
        final JsonEnvelope transformedEnvelope = enveloper.withMetadataFrom(event, "sample.events.name.pass1.sequence3").apply(event.payload());
        return Stream.of(transformedEnvelope);
    }

    @Override
    public void setEnveloper(final Enveloper enveloper) {
        this.enveloper = enveloper;
    }

    @Override
    public Optional<UUID> setStreamId(final JsonEnvelope event) {
        return Optional.of(fromString("80764cb1-a031-4328-b59e-6c18b0974a84"));
    }
}


