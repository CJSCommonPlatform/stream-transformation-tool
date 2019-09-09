package uk.gov.justice.tools.eventsourcing.transformation.service;

import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.eventsourcing.publishedevent.rebuild.PublishedEventRebuilder;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.transaction.Transactional;

import org.slf4j.Logger;

/**
 * Service to transform events on an event-stream.
 */
@ApplicationScoped
public class PublishedEventsRebuilderService {

    @Inject
    private Logger logger;

    @Inject
    private PublishedEventRebuilder publishedEventRebuilder;

    @Transactional(REQUIRES_NEW)
    public void rebuild() {
        try {
            publishedEventRebuilder.rebuild();
        } catch (final Exception e) {
            logger.error("Failed to rebuild published events", e);
        }
    }
}
