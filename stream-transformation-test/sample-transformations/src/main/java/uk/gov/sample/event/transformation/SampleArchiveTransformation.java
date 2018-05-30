package uk.gov.sample.event.transformation;

import static uk.gov.justice.tools.eventsourcing.transformation.api.TransformAction.ARCHIVE;
import static uk.gov.justice.tools.eventsourcing.transformation.api.TransformAction.NO_ACTION;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.TransformAction;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;

import java.util.stream.Stream;

@Transformation
public class SampleArchiveTransformation implements EventTransformation {

    private Enveloper enveloper;

    @Override
    public TransformAction actionFor(final JsonEnvelope event) {
        if (event.metadata().name().equalsIgnoreCase("sample.archive.events.name")) {
            return ARCHIVE;
        }
        return NO_ACTION;
    }


    @Override
    public void setEnveloper(Enveloper enveloper) {
        this.enveloper = enveloper;
    }
}
