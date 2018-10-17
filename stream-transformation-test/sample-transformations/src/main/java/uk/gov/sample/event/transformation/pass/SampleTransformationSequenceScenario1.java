package uk.gov.sample.event.transformation.pass;

import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.Action;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;

import java.util.stream.Stream;

@Transformation(pass = 7)
public class SampleTransformationSequenceScenario1 implements EventTransformation {

    private Enveloper enveloper;

    @Override
    public Action actionFor(final JsonEnvelope event) {
        if (event.metadata().name().equalsIgnoreCase("sample.events.name.pass1.sequence")) {
            return new Action(true, false, false);
        }
        return NO_ACTION;
    }

    @Override
    public Stream<JsonEnvelope> apply(final JsonEnvelope event) {
        final JsonEnvelope transformedEnvelope = enveloper.withMetadataFrom(event, "sample.events.name.pass1.sequence1").apply(event.payload());
        final JsonEnvelope transformedEnvelope2 = enveloper.withMetadataFrom(event, "sample.events.name.pass1.sequence2").apply(event.payload());
        final JsonEnvelope transformedEnvelope3 = enveloper.withMetadataFrom(event, "sample.events.name.pass1.sequence3").apply(event.payload());
        return Stream.of(transformedEnvelope, transformedEnvelope2, transformedEnvelope3);
    }

    @Override
    public void setEnveloper(final Enveloper enveloper) {
        this.enveloper = enveloper;
    }
}


