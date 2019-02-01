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

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.slf4j.Logger;

@ApplicationScoped
public class TransformationChecker {

    @Inject
    private Logger logger;

    @Inject
    private EventTransformationRegistry eventTransformationRegistry;

    public Action requiresTransformation(final List<JsonEnvelope> eventStream, final UUID streamId, int pass) {
        final List<Action> eventTransformationList = eventStream.stream()
                .map(jsonEnvelope -> getAllActions(jsonEnvelope, pass))
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
                            final String message,
                            final List<Action> eventTransformationList) {
        logger.debug(message, streamId, eventTransformationList.toString());

        return NO_ACTION;
    }

    private List<Action> getAllActions(final JsonEnvelope event, final int pass) {
        final Set<EventTransformation> eventTransformations = getEventTransformations(pass);

        return eventTransformations
                .stream()
                .map(t -> t.actionFor(event))
                .filter(Objects::nonNull)
                .filter(action -> !action.equals(NO_ACTION))
                .collect(toList());
    }


    private Set<EventTransformation> getEventTransformations(final int pass) {
        return eventTransformationRegistry.getEventTransformationBy(pass);
    }
}
