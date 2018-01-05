package uk.gov.justice.tools.eventsourcing.transformation.api;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.stream.Stream;

/**
 * Interface for the Event transformation. Responsible for identifying whether the transformation
 * should be applied to a particular event and that how event should be transformed.
 */
public interface EventTransformation {

    /**
     * Checks if a transformation is applicable to a given event.
     *
     * @param event - the event to check
     * @return TRUE if the event is eligible to have the transformation applied to it.
     */
    boolean isApplicable(final JsonEnvelope event);

    /**
     * Transforms an events into zero to many events.
     *
     * @param event - the event to be transformed
     * @return a stream of transformed events.
     */
    Stream<JsonEnvelope> apply(final JsonEnvelope event);

    void setEnveloper(Enveloper enveloper);
}
