package uk.gov.justice.tools.eventsourcing.transformation;

import static co.unruly.matchers.OptionalMatchers.contains;
import static java.util.UUID.randomUUID;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static uk.gov.justice.services.test.utils.core.messaging.JsonEnvelopeBuilder.envelope;
import static uk.gov.justice.services.test.utils.core.messaging.MetadataBuilderFactory.metadataOf;
import static uk.gov.justice.services.test.utils.core.messaging.MetadataBuilderFactory.metadataWithDefaults;

import uk.gov.justice.services.eventsourcing.repository.jdbc.EventRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStreamJdbcRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.StoreEventRequestFailedException;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class NonPublishingEventAppenderTest {

    @Mock
    private EventRepository eventRepository;

    @Mock
    private EventStreamJdbcRepository eventStreamRepository;

    @InjectMocks
    private NonPublishingEventAppender eventAppender;

    @Test
    public void shouldStoreEventInRepo() throws Exception {

        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        eventAppender.append(
                envelope()
                        .with(metadataOf(eventId, "name123"))
                        .withPayloadOf("payloadValue123", "somePayloadField")
                        .build(),
                streamId,
                3L);

        ArgumentCaptor<JsonEnvelope> envelopeCaptor = ArgumentCaptor.forClass(JsonEnvelope.class);

        verify(eventRepository).store(envelopeCaptor.capture());

        final JsonEnvelope storedEnvelope = envelopeCaptor.getValue();
        assertThat(storedEnvelope.metadata().streamId(), contains(streamId));
        assertThat(storedEnvelope.metadata().version(), contains(3L));
        assertThat(storedEnvelope.metadata().name(), is("name123"));
        assertThat(storedEnvelope.payloadAsJsonObject().getString("somePayloadField"), is("payloadValue123"));
    }

    @Test(expected = EventStreamException.class)
    public void shouldThrowExceptionWhenStoreEventRequestFails() throws Exception {
        doThrow(StoreEventRequestFailedException.class).when(eventRepository).store(any());
        eventAppender.append(envelope().with(metadataWithDefaults()).build(), randomUUID(), 1l);
    }

    @Test
    public void shouldStoreANewEventStream() throws EventStreamException {
        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();

        final long firstStreamEvent = 1L;
        eventAppender.append(
                envelope()
                        .with(metadataOf(eventId, "name456"))
                        .withPayloadOf("payloadValue456", "someOtherPayloadField")
                        .build(),
                streamId,
                firstStreamEvent);

        final ArgumentCaptor<UUID> streamIdCapture = ArgumentCaptor.forClass(UUID.class);

        verify(eventStreamRepository).insert(streamIdCapture.capture());

        final UUID streamIdActual = streamIdCapture.getValue();
        assertThat(streamIdActual, is(streamId));
    }

    @Test
    public void shouldNotStoreANewEventStream() throws EventStreamException {
        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final long secondStreamEvent = 2L;

        eventAppender.append(
                envelope()
                        .with(metadataOf(eventId, "name456"))
                        .withPayloadOf("payloadValue456", "someOtherPayloadField")
                        .build(),
                streamId,
                secondStreamEvent);

        verifyNoMoreInteractions(eventStreamRepository);
    }
}
