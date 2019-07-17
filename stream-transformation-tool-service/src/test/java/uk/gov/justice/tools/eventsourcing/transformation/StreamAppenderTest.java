package uk.gov.justice.tools.eventsourcing.transformation;

import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.of;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.service.RetryStreamOperator;
import uk.gov.justice.tools.eventsourcing.transformation.service.StreamAppender;

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
    private EnvelopeFixer envelopeFixer;

    @Mock
    private RetryStreamOperator retryStreamOperator;

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
        when(envelopeFixer.clearPositionAndGiveNewId(event_1)).thenReturn(clearedEvent_1);
        when(envelopeFixer.clearPositionAndGiveNewId(event_2)).thenReturn(clearedEvent_2);

        streamAppender.appendEventsToStream(streamId, of(event_1, event_2));

        verify(retryStreamOperator).appendWithRetry(eq(streamId), eq(eventStream), envelopeStreamCaptor.capture());

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

        final EventStream eventStream = mock(EventStream.class);
        final Stream<JsonEnvelope> jsonEnvelopeStream = of(event_1, event_2);

        when(eventSource.getStreamById(streamId)).thenReturn(eventStream);
        doThrow(eventStreamException).when(retryStreamOperator).appendWithRetry(eq(streamId), eq(eventStream), envelopeStreamCaptor.capture());

        try {
            streamAppender.appendEventsToStream(streamId, jsonEnvelopeStream);
            fail();
        } catch (final EventStreamException expected) {
            assertThat(expected, is(eventStreamException));
            assertThat(expected.getMessage(), is("oops"));
        }
    }
}
