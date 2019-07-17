package uk.gov.justice.tools.eventsourcing.transformation.service;

import static java.util.UUID.randomUUID;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.OptimisticLockingRetryException;
import uk.gov.justice.services.eventsourcing.source.core.EventSourceTransformation;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.messaging.JsonEnvelope;

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
public class RetryStreamOperatorTest {

    @Mock
    private Logger logger;

    @Mock
    private EventStream eventStream;

    @Mock
    private Stream<JsonEnvelope> envelopes;

    @Mock
    private StreamOperationRetryableExecutor streamOperationRetryableExecutor;

    @Mock
    private EventSourceTransformation eventSourceTransformation;

    @Captor
    private ArgumentCaptor<StreamOperationRetryable> streamOperationRetryableArgumentCaptor;

    @InjectMocks
    private RetryStreamOperator retryStreamOperator;

    @Test
    public void shouldAppendWithRetry() throws Exception {
        final UUID streamId = randomUUID();

        retryStreamOperator.appendWithRetry(streamId, eventStream, envelopes);

        verify(streamOperationRetryableExecutor).execute(eq(streamId), streamOperationRetryableArgumentCaptor.capture());

        streamOperationRetryableArgumentCaptor.getValue().execute();

        verify(eventStream).append(envelopes);
    }


    @Test
    public void shouldCloneWithRetry() throws Exception {
        final UUID streamId = randomUUID();

        retryStreamOperator.cloneWithRetry(streamId);

        verify(streamOperationRetryableExecutor).execute(eq(streamId), streamOperationRetryableArgumentCaptor.capture());

        streamOperationRetryableArgumentCaptor.getValue().execute();

        verify(eventSourceTransformation).cloneStream(streamId);
    }
}