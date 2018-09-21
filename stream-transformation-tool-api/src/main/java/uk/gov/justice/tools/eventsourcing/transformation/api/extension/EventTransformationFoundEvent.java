package uk.gov.justice.tools.eventsourcing.transformation.api.extension;

import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

/**
 * Event fired to signal the discovery of an {@link EventTransformation}.
 */
public class EventTransformationFoundEvent {

    private final Class<?> clazz;
    private final int transformationPosition;

    public EventTransformationFoundEvent(final Class<?> clazz, final int transformationPosition ) {
        this.clazz = clazz;
        this.transformationPosition = transformationPosition;
    }

    public Class<?> getClazz() {
        return clazz;
    }

    public int getTransformationPosition() {
        return transformationPosition;
    }
}
