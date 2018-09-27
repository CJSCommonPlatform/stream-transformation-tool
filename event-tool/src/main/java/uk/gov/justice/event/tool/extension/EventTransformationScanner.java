package uk.gov.justice.event.tool.extension;

import static java.util.Collections.synchronizedList;

import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.annotation.Transformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.extension.EventTransformationFoundEvent;

import java.util.ArrayList;
import java.util.List;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.AfterDeploymentValidation;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.ProcessAnnotatedType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scans all beans and processes {@link EventTransformation} classes.
 */
public class EventTransformationScanner implements Extension {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventTransformationScanner.class);

    private final List<Object> events = synchronizedList(new ArrayList<>());

    @SuppressWarnings("unused")
    <T> void processAnnotatedType(@Observes final ProcessAnnotatedType<T> pat) {
        final AnnotatedType<T> annotatedType = pat.getAnnotatedType();

        if (annotatedType.isAnnotationPresent(Transformation.class)) {

            final int pass = annotatedType.getAnnotation(Transformation.class).pass();

            LOGGER.info("Found 'EventTransformation' class");
            events.add(new EventTransformationFoundEvent(annotatedType.getJavaClass(), pass));
        }
    }

    @SuppressWarnings("unused")
    void afterDeploymentValidation(@Observes final AfterDeploymentValidation event, final BeanManager beanManager) {
        events.forEach(beanManager::fireEvent);
    }
}
