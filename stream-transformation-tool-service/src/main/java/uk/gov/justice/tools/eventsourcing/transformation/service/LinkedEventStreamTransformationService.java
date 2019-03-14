package uk.gov.justice.tools.eventsourcing.transformation.service;

import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.eventsourcing.source.core.LinkedEventSourceTransformation;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.transaction.Transactional;

import org.slf4j.Logger;

/**
 * Service to transform events on an event-stream.
 */
@ApplicationScoped
public class LinkedEventStreamTransformationService {

    @Inject
    private Logger logger;

    @Inject
    private LinkedEventSourceTransformation linkedEventSourceTransformation;

    @Transactional(REQUIRES_NEW)
    public void truncateLinkedEvents() {
        try {
            linkedEventSourceTransformation.truncate();
        } catch (final Exception e) {
            logger.error("Failed to truncate linked events log", e);
        }
    }

    @Transactional(REQUIRES_NEW)
    public void populateLinkedEvents() {
        try {
            linkedEventSourceTransformation.populate();
        } catch (final Exception e) {
            logger.error("Failed to populate linked events log", e);
        }
    }
}
