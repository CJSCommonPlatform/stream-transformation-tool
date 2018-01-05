package uk.gov.justice.tools.eventsourcing.transformation.api.annotation;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Identifies POJOs that are defining {@link EventTransformation}.
 */

@Retention(RUNTIME)
@Target(TYPE)
public @interface Transformation {

}
