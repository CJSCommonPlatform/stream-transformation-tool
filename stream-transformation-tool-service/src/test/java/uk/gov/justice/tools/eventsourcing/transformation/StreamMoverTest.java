package uk.gov.justice.tools.eventsourcing.transformation;

import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static javax.json.Json.createObjectBuilder;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;

import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class StreamMoverTest {

    private static final UUID STREAM_ID = randomUUID();
    private static final UUID MOVED_STREAM_ID = randomUUID();

    private static final String SOURCE_EVENT_NAME = "test.event.name";
    private static final String TRANSFORMED_EVENT_NAME = "test.event.newName";

    @Mock
    private Logger logger;

    @Mock
    private EventSource eventSource;

    @Mock
    private EventStream eventStream;

    @Mock
    private EventTransformation eventTransformation;

    @Mock
    private StreamTransformerUtil streamTransformerUtil;

    @Mock
    private StreamAppender streamAppender;

    @Mock
    private EventStreamReader eventStreamReader;

    @Captor
    private ArgumentCaptor<Stream<JsonEnvelope>> streamArgumentCaptor;

    @Captor
    private ArgumentCaptor<Set<EventTransformation>> eventTransformationArgumentCaptor;

    @Captor
    private ArgumentCaptor<List<JsonEnvelope>> listArgumentCaptor;

    @Captor
    private ArgumentCaptor<UUID> streamIdArgumentCaptor;


    @InjectMocks
    private StreamMover streamMover;


    @Test
    public void shouldMoveStream() throws EventStreamException {

        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        final Set<EventTransformation> transformations = newHashSet(eventTransformation);
        final Stream<JsonEnvelope> jsonEnvelopeStream = Stream.of(event);

        when(eventSource.getStreamById(STREAM_ID)).thenReturn(eventStream);
        when(eventStream.read()).thenReturn(jsonEnvelopeStream);
        when(eventTransformation.apply(event)).thenReturn(Stream.of(buildEnvelope(TRANSFORMED_EVENT_NAME)));

        when(eventSource.getStreamById(STREAM_ID)).thenReturn(eventStream);
        when(eventSource.getStreamById(MOVED_STREAM_ID)).thenReturn(eventStream);

        when(eventStream.append(streamArgumentCaptor.capture())).thenReturn(1L);
        when(streamTransformerUtil.transformAndMove(streamArgumentCaptor.capture(), eventTransformationArgumentCaptor.capture())).thenReturn(jsonEnvelopeStream);
        when(streamTransformerUtil.filterOriginalEvents(listArgumentCaptor.capture(), eventTransformationArgumentCaptor.capture())).thenReturn(jsonEnvelopeStream);

        streamMover.transformAndMoveStream(STREAM_ID, transformations, MOVED_STREAM_ID);

        verify(eventStreamReader).getStreamBy(STREAM_ID);

        verify(eventSource).clearStream(STREAM_ID);
        verify(streamTransformerUtil).transformAndMove(streamArgumentCaptor.capture(), eventTransformationArgumentCaptor.capture());
        verify(streamTransformerUtil).filterOriginalEvents(listArgumentCaptor.capture(), eventTransformationArgumentCaptor.capture());
        verify(streamTransformerUtil).filterOriginalEvents(listArgumentCaptor.capture(), eventTransformationArgumentCaptor.capture());
        verify(streamAppender, times(2)).appendEventsToStream(streamIdArgumentCaptor.capture(), streamArgumentCaptor.capture());
    }


    @Test
    public void shouldLogEventStreamException() throws Exception {
        final Set<EventTransformation> transformations = newHashSet(eventTransformation);
        doThrow(Exception.class).when(eventSource).clearStream(any());
        streamMover.transformAndMoveStream(STREAM_ID, transformations, randomUUID());

        verify(logger).error(format(anyString()), any(Exception.class));
    }

    private JsonEnvelope buildEnvelope(final String eventName) {
        return envelopeFrom(
                metadataBuilder().withId(randomUUID()).withStreamId(STREAM_ID).withName(eventName),
                createObjectBuilder().add("field", "value").build());
    }

}
