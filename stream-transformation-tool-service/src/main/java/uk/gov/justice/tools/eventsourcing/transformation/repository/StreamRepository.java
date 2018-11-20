package uk.gov.justice.tools.eventsourcing.transformation.repository;

import static java.lang.String.format;

import uk.gov.justice.services.eventsourcing.repository.jdbc.EventRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventJdbcRepository;
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
    private EventRepository eventRepository;

    @Inject
    private EventStreamJdbcRepository eventStreamJdbcRepository;


    @SuppressWarnings({"squid:S2629"})
    public void deleteStream(final UUID streamId) {
        eventStreamJdbcRepository.delete(streamId);
        eventRepository.clearEventsForStream(streamId);
        logger.info(format("deleted stream '%s'", streamId));
    }

    @SuppressWarnings({"squid:S2629"})
    public void deactivateStream(final UUID streamId) {
        eventStreamJdbcRepository.markActive(streamId, false);
        logger.info(format("deactivated/archived stream '%s'", streamId));
    }
}
