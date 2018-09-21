package uk.gov.justice.tools.eventsourcing.transformation;


import static java.util.function.Function.identity;

import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class StreamTransformerUtil {

    public Stream<JsonEnvelope> transform(final Stream<JsonEnvelope> events,
                                          final Set<EventTransformation> transformations) {
        return events.map(event ->
                transformerFor(event, transformations)
                        .map(eventTransformation -> eventTransformation.apply(event))
                        .orElse(Stream.of(event))
        ).flatMap(identity());
    }

    private Optional<EventTransformation> transformerFor(final JsonEnvelope event,
                                                         final Set<EventTransformation> transformations) {
        return transformations
                .stream()
                .filter(transformation -> transformation.actionFor(event).isTransform())
                .findFirst();
    }
}
