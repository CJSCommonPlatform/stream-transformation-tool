package uk.gov.justice.tools.eventsourcing.transformation;

import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.eventsourcing.source.core.EventSourceConstants.INITIAL_EVENT_VERSION;

import uk.gov.justice.services.eventsourcing.repository.jdbc.EventRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStreamJdbcRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.StoreEventRequestFailedException;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;

import java.sql.SQLException;
import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class NonPublishingEventAppenderTest {

    @Mock
    private EventRepository eventRepository;

    @Mock
    private EventStreamJdbcRepository eventStreamRepository;

    @Mock
    private NewEnvelopeCreator newEnvelopeCreator;

    @InjectMocks
    private NonPublishingEventAppender eventAppender;

    @Test
    public void shouldCreateNewVersionOfEventAndStoreInTheEventRepository() throws Exception {

        final UUID streamId = randomUUID();
        final long version = 982374L;

        final JsonEnvelope event = mock(JsonEnvelope.class);
        final JsonEnvelope eventWithStreamIdAndVersion = mock(JsonEnvelope.class);

        when(newEnvelopeCreator.toNewEnvelope(event, streamId, version)).thenReturn(eventWithStreamIdAndVersion);

        eventAppender.append(event, streamId, version);

        verify(eventRepository).store(eventWithStreamIdAndVersion);

        verifyZeroInteractions(eventStreamRepository);
    }

    @Test
    public void shouldCreateNewStreamIfTheVersionIsInitialEventVersion() throws Exception {

        final UUID streamId = randomUUID();
        final long version = INITIAL_EVENT_VERSION;

        final JsonEnvelope event = mock(JsonEnvelope.class);
        final JsonEnvelope eventWithStreamIdAndVersion = mock(JsonEnvelope.class);

        when(newEnvelopeCreator.toNewEnvelope(event, streamId, version)).thenReturn(eventWithStreamIdAndVersion);

        eventAppender.append(event, streamId, version);

        final InOrder inOrder = inOrder(eventStreamRepository, eventRepository);

        inOrder.verify(eventStreamRepository).insert(streamId);
        inOrder.verify(eventRepository).store(eventWithStreamIdAndVersion);
    }

    @Test
    public void shouldThrowEventStreamExceptionIfStoringTheEventFails() throws Exception {

        final StoreEventRequestFailedException storeEventRequestFailedException = new StoreEventRequestFailedException("Ooops", new SQLException());

        final String eventId = "89f9194a-a544-4cae-8904-a1dc78df98c3";
        final UUID streamId = randomUUID();
        final long version = 982374L;

        final JsonEnvelope event = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final JsonEnvelope eventWithStreamIdAndVersion = mock(JsonEnvelope.class);

        when(newEnvelopeCreator.toNewEnvelope(event, streamId, version)).thenReturn(eventWithStreamIdAndVersion);
        when(event.metadata()).thenReturn(metadata);
        when(metadata.id()).thenReturn(fromString(eventId));
        doThrow(storeEventRequestFailedException).when(eventRepository).store(eventWithStreamIdAndVersion);

        try {
            eventAppender.append(event, streamId, version);
            fail();
        } catch (final EventStreamException expected) {
            assertThat(expected.getCause(), is(storeEventRequestFailedException));
            assertThat(expected.getMessage(), is("Failed to append event with id '89f9194a-a544-4cae-8904-a1dc78df98c3' to the event store"));
        }
    }
}
