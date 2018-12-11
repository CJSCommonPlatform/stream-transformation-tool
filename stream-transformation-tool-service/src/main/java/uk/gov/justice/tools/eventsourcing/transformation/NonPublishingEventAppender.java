package uk.gov.justice.tools.eventsourcing.transformation;


import static java.lang.String.format;
import static uk.gov.justice.services.eventsourcing.source.core.EventSourceConstants.INITIAL_EVENT_VERSION;

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

    @Inject
    NewEnvelopeCreator newEnvelopeCreator;

    /**
     * Stores the event in the event store and publishes it with the given streamId and version.
     *
     * @param event    - the event to be appended
     * @param streamId - id of the stream the event will be part of
     * @param position - version id of the event in the stream
     */
    @Override
    public void append(final JsonEnvelope event, final UUID streamId, final long position, final String eventSourceName) throws EventStreamException {
        try {
            if (position == INITIAL_EVENT_VERSION) {
                streamRepository.insert(streamId);
            }
//            final JsonEnvelope eventWithStreamIdAndVersion = eventFrom(event, streamId, position, eventSourceName);
            final JsonEnvelope eventWithStreamIdAndVersion = newEnvelopeCreator.toNewEnvelope(event, streamId, version);
            eventRepository.storeEvent(eventWithStreamIdAndVersion);
        } catch (final StoreEventRequestFailedException e) {
            throw new EventStreamException(format("Failed to append event with id '%s' to the event store", event.metadata().id()), e);
        }
    }
}
