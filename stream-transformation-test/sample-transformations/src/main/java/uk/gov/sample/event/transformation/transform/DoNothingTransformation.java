package uk.gov.sample.event.transformation.transform;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;

import java.util.stream.Stream;

@Transformation
public class DoNothingTransformation implements EventTransformation {

    @Override
    public boolean isApplicable(JsonEnvelope event) {
        return event.metadata().name().equalsIgnoreCase("sample.events.check-date-not-transformed");
    }

    @Override
    public Stream<JsonEnvelope> apply(final JsonEnvelope event) {
        return Stream.of(event);
    }

    @Override
    public void setEnveloper(final Enveloper enveloper) {

    }
}
