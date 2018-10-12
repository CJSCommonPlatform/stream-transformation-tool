package uk.gov.justice.tools.eventsourcing.transformation;


import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.empty;

import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class StreamTransformerUtil {

    private Set<EventTransformation> transformations;

    public StreamTransformerUtil(final Set<EventTransformation> transformations) {
        this.transformations = transformations;
    }

    public List<JsonEnvelope> transform(final List<JsonEnvelope> events,
                                        final Set<EventTransformation> transformations) {

        return events.stream()
                .map(event ->
                        transformerFor(event, transformations)
                                .map(eventTransformation -> eventTransformation.apply(event))
                                .orElse(empty())
                ).flatMap(identity())
                .collect(toList());
    }


    public List<JsonEnvelope> getTransformedEvents(List<JsonEnvelope> events, Set<EventTransformation> transformations) {
        return events.stream()
                .map(event ->
                        transformerFor(event, transformations)
                                .map(eventTransformation -> eventTransformation.apply(event))
                                .orElse(Stream.of(event))
                ).flatMap(identity())
                .collect(toList());
    }

    public List<JsonEnvelope> filterOriginalEvents(final Stream<JsonEnvelope> events) {
        return events.flatMap(this::getOriginalEvents).collect(toList());
    }

    private Optional<EventTransformation> transformerFor(final JsonEnvelope event, final Set<EventTransformation> transformations) {
        return transformations
                .stream()
                .filter(transformation -> transformation.actionFor(event).isTransform())
                .findFirst();
    }

    private Stream<JsonEnvelope> getOriginalEvents(final JsonEnvelope event) {
        final Optional<EventTransformation> eventTransformationOptional = transformations
                .stream()
                .filter(transformation -> !transformation.actionFor(event).isTransform())
                .findFirst();

        return eventTransformationOptional.isPresent() ? Stream.of(event) : empty();
    }

}
