package uk.gov.justice.tools.eventsourcing.transformation;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.tools.eventsourcing.TransformingEnveloper;

import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Alternative;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

@ApplicationScoped
@Alternative
@Priority(1)
public class EnveloperProducer {

    @Inject
    private EnvelopeFactory envelopeFactory;

    @Produces
    public Enveloper transformingEnveloper() {
        return new TransformingEnveloper(envelopeFactory);
    }
}
