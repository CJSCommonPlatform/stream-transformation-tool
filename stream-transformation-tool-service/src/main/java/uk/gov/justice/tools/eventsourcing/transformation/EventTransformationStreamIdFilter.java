package uk.gov.justice.tools.eventsourcing.transformation;

import static java.util.Optional.empty;

import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class EventTransformationStreamIdFilter {

    public Optional<UUID> getEventTransformationStreamId(final Set<EventTransformation> transformations,
                                                         final List<JsonEnvelope> jsonEnvelopeList) {

        return jsonEnvelopeList
                .stream()
                .map(event -> streamIdFor(event, transformations))
                .filter(Optional::isPresent)
                .findFirst()
                .orElse(empty());
    }

    private Optional<UUID> streamIdFor(final JsonEnvelope event,
                                       final Set<EventTransformation> transformations) {
        final Optional<EventTransformation> eventTransformation = transformations
                .stream()
                .filter(transformation -> transformation.actionFor(event).isTransform())
                .filter(transformation -> transformation.setStreamId(event).isPresent())
                .findFirst();

        return eventTransformation.isPresent() ? eventTransformation.get().setStreamId(event) : empty();
    }
}
