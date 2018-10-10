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

    public Optional<UUID> getEventTransformationStreamId(final Set<EventTransformation> transformations, final List<JsonEnvelope> transformedEventStream) {
        final Optional<Optional<UUID>> eventTransformationStreamId = transformedEventStream.stream()
                .map(e -> filterStreamId(e, transformations))
                .filter(streamId1 -> streamId1.isPresent()).findFirst();
        return eventTransformationStreamId.isPresent()?eventTransformationStreamId.get():empty();
    }

    private Optional<UUID> filterStreamId(final JsonEnvelope event, final Set<EventTransformation> transformations) {
        final Optional<EventTransformation> eventTransformation = transformations
                .stream()
                .filter(transformation -> transformation.streamId(event).isPresent())
                .findFirst();

        return eventTransformation.isPresent() ? eventTransformation.get().streamId(event) : empty();
    }
}
