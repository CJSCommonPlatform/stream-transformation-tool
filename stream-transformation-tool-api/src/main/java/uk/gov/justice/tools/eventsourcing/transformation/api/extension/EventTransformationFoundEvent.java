package uk.gov.justice.tools.eventsourcing.transformation.api.extension;

import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

/**
 * Event fired to signal the discovery of an {@link EventTransformation}.
 */
public class EventTransformationFoundEvent {

    private final Class<?> clazz;

    public EventTransformationFoundEvent(final Class<?> clazz) {
        this.clazz = clazz;
    }

    public Class<?> getClazz() {
        return clazz;
    }

}
