package uk.gov.justice.tools.eventsourcing.transformation.repository;

import static java.util.UUID.randomUUID;
import static javax.json.Json.createObjectBuilder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.spi.DefaultJsonMetadata.metadataBuilder;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventJdbcRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStreamJdbcRepository;
import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.UUID;
import java.util.stream.Stream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class StreamRepositoryTest {

    private static final UUID STREAM_ID = randomUUID();

    @Mock
    private Logger logger;

    @Mock
    private EventStreamJdbcRepository eventStreamJdbcRepository;

    @Mock
    private EventJdbcRepository eventRepository;


    @InjectMocks
    private StreamRepository streamRepository;

    @Test
    public void shouldDeactivateTheStreamWhenRequested() {

        streamRepository.deactivateStream(STREAM_ID);

        verify(eventStreamJdbcRepository).markActive(STREAM_ID, false);
        verifyNoMoreInteractions(eventStreamJdbcRepository);
    }

    @Test
    public void shouldDeleteTheStreamWhenRequested() {
        streamRepository.deleteStream(STREAM_ID);

        verify(eventStreamJdbcRepository).delete(STREAM_ID);
        verify(eventRepository).clear(STREAM_ID);
        verifyNoMoreInteractions(eventStreamJdbcRepository);
    }

}
