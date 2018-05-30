package uk.gov.justice.tools.eventsourcing.transformation.api;

import static java.util.stream.Stream.of;
import static uk.gov.justice.tools.eventsourcing.transformation.api.TransformAction.NO_ACTION;
import static uk.gov.justice.tools.eventsourcing.transformation.api.TransformAction.TRANSFORM;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.stream.Stream;

/**
 * Interface for the Event transformation. Responsible for identifying whether the transformation
 * should be applied to a particular event and that how event should be transformed.
 */
public interface EventTransformation {

    /**
     * This method is deprecated and use actionFor method instead. The actionFor method call this internally.
     * Checks if a transformation is applicable to a given event.
     *
     * @param event - the event to check
     * @return TRUE if the event is eligible to have the transformation applied to it.
     * The default implementation is false
     */
    @Deprecated
    default boolean isApplicable(final JsonEnvelope event){
        return false;
    }


    /**
     * Checks which actionFor to perform for a given event.
     *
     * @param event - the event to check
     * @return TransformAction if the event is eligible to have the transformation applied to it.
     */
    default TransformAction actionFor(final JsonEnvelope event) {
        return isApplicable(event) ? TRANSFORM : NO_ACTION;
    }

    /**
     * Transforms an events into zero to many events.
     *
     * @param event - the event to be transformed
     * @return a stream of transformed events.
     * The default implementation is provided for other than transform actio, as other actions dose not need to implement this method.
     */
    default Stream<JsonEnvelope> apply(final JsonEnvelope event){
        return of(event);

    }

    void setEnveloper(Enveloper enveloper);
}
