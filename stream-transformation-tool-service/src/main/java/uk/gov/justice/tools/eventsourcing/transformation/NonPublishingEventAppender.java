package uk.gov.justice.tools.eventsourcing.transformation;


import static java.lang.String.format;
import static uk.gov.justice.services.eventsourcing.source.core.PublishingEventAppender.INITIAL_STREAM_EVENT;

import uk.gov.justice.services.eventsourcing.repository.jdbc.EventRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStreamJdbcRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.StoreEventRequestFailedException;
import uk.gov.justice.services.eventsourcing.source.core.EventAppender;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.UUID;

import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Alternative;
import javax.inject.Inject;

@ApplicationScoped
@Alternative
@Priority(2)
public class NonPublishingEventAppender implements EventAppender {

    @Inject
    EventRepository eventRepository;

    @Inject
    EventStreamJdbcRepository streamRepository;

    /**
     * Stores the event in the event store and publishes it with the given streamId and version.
     *
     * @param event    - the event to be appended
     * @param streamId - id of the stream the event will be part of
     * @param version  - version id of the event in the stream
     */
    @Override
    public void append(final JsonEnvelope event, final UUID streamId, final long version) throws EventStreamException {
        try {
            if (version == INITIAL_STREAM_EVENT) {
                streamRepository.insert(streamId);
            }
            final JsonEnvelope eventWithStreamIdAndVersion = eventFrom(event, streamId, version);
            eventRepository.store(eventWithStreamIdAndVersion);
        } catch (StoreEventRequestFailedException e) {
            throw new EventStreamException(format("Failed to append event to the event store %s", event.metadata().id()), e);
        }
    }


}
