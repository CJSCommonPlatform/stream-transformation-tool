package uk.gov.justice.tools.eventsourcing.transformation;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static javax.json.Json.createObjectBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.MOVE_AND_TRANSFORM;

import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.repository.StreamRepository;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class StreamMoverTest {

    private static final UUID STREAM_ID = randomUUID();
    private static final UUID CLONED_STREAM_ID = randomUUID();
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
    private StreamRepository streamRepository;

    @Mock
    private EventTransformation eventTransformation;

    @Captor
    private ArgumentCaptor<Stream<JsonEnvelope>> streamArgumentCaptor;


    @InjectMocks
    private StreamMover streamMover;

    @Test
    public void shouldMoveAndBackUpStream() throws EventStreamException {

        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        final Set<EventTransformation> transformations = newHashSet(eventTransformation);

        when(eventSource.cloneStream(STREAM_ID)).thenReturn(CLONED_STREAM_ID);
        when(eventSource.getStreamById(STREAM_ID)).thenReturn(eventStream);

        final Stream<JsonEnvelope> event1 = Stream.of(event);
        when(eventStream.read()).thenReturn(event1);
        when(eventTransformation.actionFor(any(JsonEnvelope.class))).thenReturn(MOVE_AND_TRANSFORM);
        when(eventTransformation.apply(event)).thenReturn(Stream.of(buildEnvelope(TRANSFORMED_EVENT_NAME)));

        when(streamRepository.createStream()).thenReturn(MOVED_STREAM_ID);
        when(eventSource.getStreamById(MOVED_STREAM_ID)).thenReturn(eventStream);
        when(eventStream.append(streamArgumentCaptor.capture())).thenReturn(1L);

        final  Optional<UUID> clonedStreamId =  streamMover.moveAndBackupStream(STREAM_ID, transformations);

        final InOrder inOrder = inOrder(eventSource, eventStream);
        inOrder.verify(eventSource).cloneStream(STREAM_ID);
        inOrder.verify(eventSource).getStreamById(STREAM_ID);
        inOrder.verify((eventStream)).read();
        inOrder.verify(eventSource).clearStream(STREAM_ID);
        inOrder.verify(eventStream).append(streamArgumentCaptor.capture());

        assertThat(clonedStreamId, is(of(CLONED_STREAM_ID)));
    }

    @Test
    public void shouldLogErrorAndReturnEmptyStreamIdIfMoveAndBackUpStreamFailed() throws EventStreamException {

        final JsonEnvelope event = buildEnvelope(SOURCE_EVENT_NAME);
        final Set<EventTransformation> transformations = newHashSet(eventTransformation);

        given(eventSource.cloneStream(STREAM_ID)).willReturn(CLONED_STREAM_ID);
        given(eventSource.getStreamById(STREAM_ID)).willReturn(eventStream);

        when(eventStream.read()).thenReturn(Stream.of(event));
        given(eventTransformation.actionFor(any(JsonEnvelope.class))).willReturn(MOVE_AND_TRANSFORM);
        given(eventTransformation.apply(event)).willReturn(Stream.of(buildEnvelope(TRANSFORMED_EVENT_NAME)));

        given(streamRepository.createStream()).willReturn(MOVED_STREAM_ID);

        final Optional<UUID> clonedStreamId =  streamMover.moveAndBackupStream(STREAM_ID, transformations);

        assertThat(clonedStreamId, is(empty()));
    }

    @Test
    public void shouldLogEventStreamExceptionAndReturnEmptyStreamIdIfMoveAndBackUpStreamFailed() throws Exception {

        final Set<EventTransformation> transformations = newHashSet(eventTransformation);
        doThrow(EventStreamException.class).when(eventSource).cloneStream(any());

        final Optional<UUID> clonedStreamId = streamMover.moveAndBackupStream(STREAM_ID, transformations);

        assertThat(clonedStreamId, is(empty()));
    }

    private JsonEnvelope buildEnvelope(final String eventName) {
        return envelopeFrom(
                metadataBuilder().withId(randomUUID()).withStreamId(STREAM_ID).withName(eventName),
                createObjectBuilder().add("field", "value").build());
    }

}
