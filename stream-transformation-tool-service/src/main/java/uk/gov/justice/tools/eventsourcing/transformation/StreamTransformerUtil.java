package uk.gov.justice.tools.eventsourcing.transformation;


import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.empty;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;

import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class StreamTransformerUtil {

    public Stream<JsonEnvelope> transform(final Stream<JsonEnvelope> events,
                                          final Set<EventTransformation> transformations) {
        return events.map(event ->
                transformerFor(event, transformations)
                        .map(eventTransformation -> eventTransformation.apply(event))
                        .orElse(Stream.of(event))
        ).flatMap(identity());
    }

    public Stream<JsonEnvelope> transformAndMove(final Stream<JsonEnvelope> events,
                                                 final Set<EventTransformation> transformations) {
        return events.map(event ->
                transformerFor(event, transformations)
                        .map(eventTransformation -> eventTransformation.apply(event))
                        .orElse(empty())
        ).flatMap(identity());
    }


    public Stream<JsonEnvelope> filterOriginalEvents(final List<JsonEnvelope> events,
                                                     final Set<EventTransformation> transformations) {

        final List<JsonEnvelope> originalEvents = new ArrayList<>();

        final Set<EventTransformation> eventTransformations = events
                .stream()
                .map(event -> transformerFor(event, transformations))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toSet());

        getOriginalEvents(events, originalEvents, eventTransformations);

        return originalEvents.stream();
    }

    private void getOriginalEvents(final List<JsonEnvelope> events,
                                   final List<JsonEnvelope> originalEvents,
                                   final Set<EventTransformation> eventTransformations) {
        for (final JsonEnvelope event : events) {
            for (final EventTransformation eventTransformation : eventTransformations) {
                if (eventTransformation.actionFor(event).equals(NO_ACTION)) {
                    originalEvents.add(event);
                }
            }
        }
    }

    private Optional<EventTransformation> transformerFor(final JsonEnvelope event,
                                                         final Set<EventTransformation> transformations) {
        return transformations
                .stream()
                .filter(transformation -> transformation.actionFor(event).isTransform())
                .findFirst();
    }
}
