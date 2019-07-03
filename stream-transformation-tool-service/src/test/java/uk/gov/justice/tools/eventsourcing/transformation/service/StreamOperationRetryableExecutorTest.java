package uk.gov.justice.tools.eventsourcing.transformation.service;

import static java.util.UUID.randomUUID;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.setField;

import uk.gov.justice.services.common.util.Sleeper;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.OptimisticLockingRetryException;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;


@RunWith(MockitoJUnitRunner.class)
public class StreamOperationRetryableExecutorTest {

    @InjectMocks
    private StreamOperationRetryableExecutor streamOperationRetryableExecutor;

    @Mock
    private StreamOperationRetryable retryable;

    @Mock
    private Sleeper sleeper;

    @Mock
    private Logger logger;

    @Before
    public void setup() throws Exception {
        setField(streamOperationRetryableExecutor, "maxRetry", 2l);
    }

    @Test
    public void shouldExecuteWithRetryAsMaxAttemptNotExceeded() throws Exception {
        final UUID streamId = randomUUID();

        doThrow(new OptimisticLockingRetryException("oops")).doNothing().when(retryable).execute();

        streamOperationRetryableExecutor.execute(streamId, retryable);

        verify(retryable, times(2)).execute();
        verify(sleeper).sleepFor(2000);
    }

    @Test
    public void shouldNotExecuteWithRetryAsMaxAttemptExceeded() throws Exception {
        final UUID streamId = randomUUID();

        doThrow(new OptimisticLockingRetryException("oops")).doThrow(new OptimisticLockingRetryException("oops")).when(retryable).execute();

        try {
            streamOperationRetryableExecutor.execute(streamId, retryable);
            fail("Should throw exception");
        } catch (OptimisticLockingRetryException e) {

            verify(retryable, times(2)).execute();
            verify(sleeper).sleepFor(2000);
        }
    }


}