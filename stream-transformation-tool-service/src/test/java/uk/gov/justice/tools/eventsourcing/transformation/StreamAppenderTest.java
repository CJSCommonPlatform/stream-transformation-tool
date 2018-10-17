package uk.gov.justice.tools.eventsourcing.transformation;

import static java.util.UUID.randomUUID;
import static org.apache.activemq.artemis.utils.JsonLoader.createObjectBuilder;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.spi.DefaultJsonMetadata.metadataBuilder;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.repository.StreamRepository;

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

    private static final UUID STREAM_ID = randomUUID();

    @Mock
    private EventSource eventSource;

    @Mock
    private StreamRepository streamRepository;

    @Captor
    private ArgumentCaptor<Stream<JsonEnvelope>> streamArgumentCaptor;

    @InjectMocks
    private StreamAppender streamAppender;

    @Test
    public void shouldAppendEvents() throws Exception {
        final JsonEnvelope envelope = buildEnvelope("test");
        final Stream<JsonEnvelope> jsonEnvelopeStream = Stream.of(envelope);
        final EventStream eventStream = mock(EventStream.class);

        when(eventSource.getStreamById(STREAM_ID)).thenReturn(eventStream);

        streamAppender.appendEventsToStream(STREAM_ID, jsonEnvelopeStream);

        verify(eventStream).append(streamArgumentCaptor.capture());
    }

    private JsonEnvelope buildEnvelope(final String eventName) {
        return envelopeFrom(
                metadataBuilder().withId(randomUUID()).withStreamId(STREAM_ID).withName(eventName),
                createObjectBuilder().add("field", "value").build());
    }
}