package uk.gov.justice.tools.eventsourcing.transformation.api;

import static java.util.stream.Stream.of;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.TRANSFORM;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Interface for the Event transformation. Responsible for identifying whether the transformation
 * should be applied to a particular event and that how event should be transformed.
 */
public interface EventTransformation {

    /**
     * Checks if a transformation is applicable to a given event. The actionFor method call this
     * internally in the default implementation for backwards compatibility.
     *
     * @param event - the event to check
     * @return TRUE if the event is eligible to have the transformation applied to it.
     * @deprecated use actionFor method instead The default implementation is false
     */
    @Deprecated
    default boolean isApplicable(final JsonEnvelope event) {
        return false;
    }


    /**
     * Checks which transform action to perform for a given event.
     *
     * @param event - the event to check
     * @return Action if the event is eligible to have the transformation applied to it.
     */
    default Action actionFor(final JsonEnvelope event) {
        return isApplicable(event) ? TRANSFORM : NO_ACTION;
    }

    /**
     * Transforms an events into zero to many events.
     *
     * @param event - the event to be transformed
     * @return a stream of transformed events. The default implementation is provided for other than
     * transform action, as other actions dose not need to implement this method.
     */
    default Stream<JsonEnvelope> apply(final JsonEnvelope event) {
        return of(event);

    }

    /**
     * Checks if events need to be appended to a specified stream.
     *
     * @param event - the event to be moved or "transformed and moved"
     * @return a stream of transformed events. The default implementation is provided and returns empty
     * indication events do not need to be moved.
     */
    default Optional<UUID> setStreamId(final JsonEnvelope event){
        return Optional.empty();
    }

    void setEnveloper(Enveloper enveloper);
}
