package uk.gov.justice.tools.eventsourcing.transformation;

import static java.util.stream.Collectors.toList;
import static uk.gov.justice.tools.eventsourcing.transformation.api.Action.NO_ACTION;

import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.tools.eventsourcing.transformation.api.Action;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.slf4j.Logger;

@ApplicationScoped
public class TransformationChecker {

    @Inject
    private Logger logger;

    @Inject
    private EventTransformationRegistry eventTransformationRegistry;

    public Action requiresTransformation(final Stream<JsonEnvelope> eventStream, final UUID streamId, int pass) {
        final List<Action> eventTransformationList = eventStream
                .map(jsonEnvelope -> checkTransformations(jsonEnvelope, pass))
                .flatMap(List::stream)
                .distinct()
                .collect(toList());

        if (eventTransformationList.isEmpty()) {
            return noAction(streamId, "Stream {} did not require transformation stream ", eventTransformationList);
        }
        if (eventTransformationList.size() > 1) {
            return noAction(streamId, "Stream {} can not have multiple actions {} ", eventTransformationList);
        }
        return eventTransformationList.get(0);
    }

    @SuppressWarnings({"squid:S2629"})
    private Action noAction(final UUID streamId,
                            final String errorMessage,
                            final List<Action> eventTransformationList) {
        logger.error(errorMessage, streamId, eventTransformationList.toString());

        return NO_ACTION;
    }

    private List<Action> checkTransformations(final JsonEnvelope event, final int pass) {

        final Set<EventTransformation> eventTransformations = getEventTransformations(pass);
        return eventTransformations
                .stream()
                .map(t -> t.actionFor(event))
                .filter(Objects::nonNull)
                .filter(t -> !t.equals(NO_ACTION))
                .collect(toList());
    }

    private Set<EventTransformation> getEventTransformations(final int pass) {
        return eventTransformationRegistry.getEventTransformationBy(pass);
    }
}
