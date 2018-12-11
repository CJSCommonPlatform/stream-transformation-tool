package uk.gov.justice.tools.eventsourcing.transformation;

import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StreamAppenderTest {

    @Mock
    private EventSource eventSource;

    @Mock
    private EventPositionClearer eventPositionClearer;

    @InjectMocks
    private StreamAppender streamAppender;

    @Captor
    private ArgumentCaptor<Stream<JsonEnvelope>> envelopeStreamCaptor;

    @Test
    public void shouldAppendEvents() throws Exception {

        final UUID streamId = randomUUID();

        final JsonEnvelope event_1 = mock(JsonEnvelope.class);
        final JsonEnvelope event_2 = mock(JsonEnvelope.class);

        final JsonEnvelope clearedEvent_1 = mock(JsonEnvelope.class);
        final JsonEnvelope clearedEvent_2 = mock(JsonEnvelope.class);

        final EventStream eventStream = mock(EventStream.class);

        when(eventSource.getStreamById(streamId)).thenReturn(eventStream);
        when(eventPositionClearer.clearEventPositioning(event_1)).thenReturn(clearedEvent_1);
        when(eventPositionClearer.clearEventPositioning(event_2)).thenReturn(clearedEvent_2);

        streamAppender.appendEventsToStream(streamId, Stream.of(event_1, event_2));

        verify(eventStream).append(envelopeStreamCaptor.capture());

        final List<JsonEnvelope> appendedEvents = envelopeStreamCaptor.getValue().collect(toList());

        assertThat(appendedEvents.size(), is(2));
        assertThat(appendedEvents, hasItem(clearedEvent_1));
        assertThat(appendedEvents, hasItem(clearedEvent_2));
    }

    @Test
    public void shouldAlwaysCloseTheStreamOnException() throws Exception {

        final EventStreamException eventStreamException = new EventStreamException("oops");

        final UUID streamId = randomUUID();

        final JsonEnvelope event_1 = mock(JsonEnvelope.class);
        final JsonEnvelope event_2 = mock(JsonEnvelope.class);

        final JsonEnvelope clearedEvent_1 = mock(JsonEnvelope.class);
        final JsonEnvelope clearedEvent_2 = mock(JsonEnvelope.class);

        final EventStream eventStream = mock(EventStream.class);
        final Stream<JsonEnvelope> jsonEnvelopeStream = Stream.of(event_1, event_2);

        when(eventSource.getStreamById(streamId)).thenReturn(eventStream);
        doThrow(eventStreamException).when(eventStream).append(any(Stream.class));


        final StreamCloseVerifier streamCloseVerifier = new StreamCloseVerifier();
        jsonEnvelopeStream.onClose(streamCloseVerifier);

        assertThat(streamCloseVerifier.isClosed(), is(false));


        try {
            streamAppender.appendEventsToStream(streamId, jsonEnvelopeStream);
            fail();
        } catch (final EventStreamException expected) {
            assertThat(streamCloseVerifier.isClosed(), is(true));
            assertThat(expected.getCause(), is(eventStreamException));
            assertThat(expected.getMessage(), is("Failed to append events to stream"));
        }
    }

    private class StreamCloseVerifier implements Runnable {

        private boolean closed = false;

        @Override
        public void run() {
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }
    }
}
