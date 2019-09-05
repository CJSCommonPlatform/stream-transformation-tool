package uk.gov.justice.tools.eventsourcing.transformation.service;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.DatabaseTableTruncator;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class PrePublishedQueueTruncatorServiceTest {

    @Mock
    private Logger logger;

    @Mock
    private DatabaseTableTruncator databaseTableTruncator;

    @Mock
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @InjectMocks
    private PrePublishedQueueTruncatorService prePublishedQueueTruncatorService;

    @Test
    public void shouldTruncatePrePublishQueueTable() throws Exception {

        final DataSource datasource = mock(DataSource.class);
        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(datasource);

        prePublishedQueueTruncatorService.truncate();

        verify(databaseTableTruncator).truncate("pre_publish_queue", datasource);
    }

    @Test
    public void shouldLogException() throws Exception {

        final DataSource datasource = mock(DataSource.class);
        final SQLException sqlException = new SQLException();

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(datasource);
        doThrow(sqlException).when(databaseTableTruncator).truncate("pre_publish_queue", datasource);

        prePublishedQueueTruncatorService.truncate();

        verify(logger).error("Failed to truncate pre_publish_queue", sqlException);
    }
}