package uk.gov.justice.tools.eventsourcing.transformation.service;

import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.DatabaseTableTruncator;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.transaction.Transactional;

import org.slf4j.Logger;

@ApplicationScoped
public class PrePublishedQueueTruncatorService {

    @Inject
    private Logger logger;

    @Inject
    private DatabaseTableTruncator databaseTableTruncator;

    @Inject
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @Transactional(REQUIRES_NEW)
    public void truncate() {
        try {
            databaseTableTruncator.truncate("pre_publish_queue", eventStoreDataSourceProvider.getDefaultDataSource());
        } catch (final Exception e) {
            logger.error("Failed to truncate pre_publish_queue", e);
        }
    }
}
