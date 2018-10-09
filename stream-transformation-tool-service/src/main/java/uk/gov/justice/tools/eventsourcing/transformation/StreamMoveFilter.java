package uk.gov.justice.tools.eventsourcing.transformation;

import static java.util.stream.Stream.empty;

import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class StreamMoveFilter {

    private final Set<EventTransformation> transformations;

    public StreamMoveFilter(final Set<EventTransformation> transformations) {
        this.transformations = transformations;
    }

    public Stream<JsonEnvelope> filterMoveEvents(final List<JsonEnvelope> events) {
        return events.stream().flatMap(this::transformMoveEvent);
    }

    public Stream<JsonEnvelope> filterOriginalEvents(final List<JsonEnvelope> events) {
        return events.stream().flatMap(this::getOriginalEvents);
    }

    private Stream<JsonEnvelope> transformMoveEvent(final JsonEnvelope event) {
        final Optional<EventTransformation> eventTransformationOptional = transformations
                .stream()
                .filter(transformation -> transformation.actionFor(event).isMoveStream())
                .findFirst();
        return eventTransformationOptional.isPresent() ? eventTransformationOptional.get().apply(event) : empty();
    }

    private Stream<JsonEnvelope> getOriginalEvents(final JsonEnvelope event) {
        final Optional<EventTransformation> eventTransformationOptional = transformations
                .stream()
                .filter(transformation -> !transformation.actionFor(event).isMoveStream())
                .findFirst();

        return eventTransformationOptional.isPresent() ? Stream.of(event) : empty();
    }

}
