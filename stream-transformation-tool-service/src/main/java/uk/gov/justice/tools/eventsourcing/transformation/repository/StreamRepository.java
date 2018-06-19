package uk.gov.justice.tools.eventsourcing.transformation.repository;

import static java.lang.String.format;

import uk.gov.justice.services.eventsourcing.repository.jdbc.eventstream.EventStreamJdbcRepository;

import java.util.UUID;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.slf4j.Logger;

@ApplicationScoped
public class StreamRepository {

    @Inject
    private Logger logger;

    @Inject
    private EventStreamJdbcRepository eventStreamJdbcRepository;

    public void deleteStream(final UUID streamId) {
        eventStreamJdbcRepository.delete(streamId);
        if (logger.isDebugEnabled()) {
            logger.debug(format("deleted stream '%s'", streamId));
        }
    }

    public void deactivateStream(final UUID streamId) {
        eventStreamJdbcRepository.markActive(streamId, false);
        if (logger.isDebugEnabled()) {
            logger.debug(format("deactivated/archived stream '%s'", streamId));
        }
    }
}
