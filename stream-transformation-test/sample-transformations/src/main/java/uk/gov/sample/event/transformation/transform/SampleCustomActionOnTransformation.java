package uk.gov.sample.event.transformation.transform;

import static com.google.common.collect.Lists.newArrayList;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.DEACTIVATE;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.Action;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;

import java.util.List;
import java.util.stream.Stream;

@Transformation
public class SampleCustomActionOnTransformation implements EventTransformation {

    private static final List<String> EVENTS_TO_DEACTIVATE = newArrayList(
            "sample.event.to.deactivate",
            "sample.event2.to.deactivate"
    );

    private static final String EVENT_NAME_ENDS_WITH = ".archived.old.release";

    private Enveloper enveloper;

    @Override
    public Stream<JsonEnvelope> apply(final JsonEnvelope event) {
        final String restoredEventName = event.metadata().name().replace(EVENT_NAME_ENDS_WITH, "");

        final JsonEnvelope transformedEnvelope = enveloper.withMetadataFrom(event, restoredEventName).apply(event.payload());
        return Stream.of(transformedEnvelope);
    }

    @Override
    public Action actionFor(final JsonEnvelope event) {
        if (EVENTS_TO_DEACTIVATE.contains(event.metadata().name().toLowerCase())) {
            return DEACTIVATE;
        } else if (event.metadata().name().toLowerCase().endsWith(EVENT_NAME_ENDS_WITH)) {
            return new Action(true, true, false, false);
        }

        return NO_ACTION;
    }

    @Override
    public void setEnveloper(final Enveloper enveloper) {
        this.enveloper = enveloper;
    }

}
