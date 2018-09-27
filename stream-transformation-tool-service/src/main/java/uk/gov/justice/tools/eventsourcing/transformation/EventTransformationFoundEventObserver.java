package uk.gov.justice.tools.eventsourcing.transformation;

import static java.lang.String.format;

import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.extension.EventTransformationFoundEvent;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.slf4j.Logger;

@ApplicationScoped
public class EventTransformationFoundEventObserver {

    @Inject
    private Logger logger;

    @Inject
    private EventTransformationRegistry eventTransformationRegistry;


    /**
     * Register method, invoked automatically to register all {@link EventTransformation} classes
     * into the transformations set.
     *
     * @param event identified by the framework to be registered into the event map.
     */
    public void register(@Observes final EventTransformationFoundEvent event) throws IllegalAccessException, InstantiationException {
        if (logger.isDebugEnabled()) {
            logger.debug(format("Loading Event Transformation %s", event.getClazz().getSimpleName()));
        }
        eventTransformationRegistry.createTransformations(event);
    }

}
