package uk.gov.justice.tools.eventsourcing.transformation;

import static java.lang.String.format;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.tools.eventsourcing.transformation.api.EventTransformation;
import uk.gov.justice.tools.eventsourcing.transformation.api.extension.EventTransformationFoundEvent;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class EventTransformationRegistry {

    @Inject
    private Enveloper enveloper;

    private final Map<Integer, Set<EventTransformation>> eventTransformationMap = new TreeMap<>();

    public void createTransformations(final EventTransformationFoundEvent event) throws IllegalAccessException, InstantiationException {
        eventTransformationMap.computeIfAbsent(event.getTransformationPosition(), key -> new HashSet<>()).add(getEventTransformation(event));
    }

    public Set<Integer> getPasses() {
        checkSequence();
        return eventTransformationMap.keySet();
    }

    public Set<EventTransformation> getEventTransformationBy(final int pass) {
        checkSequence();
        return eventTransformationMap.get(pass);
    }

    private EventTransformation getEventTransformation(final EventTransformationFoundEvent event) throws IllegalAccessException, InstantiationException {
        final EventTransformation eventTransformation = (EventTransformation) event.getClazz().newInstance();
        eventTransformation.setEnveloper(enveloper);

        return eventTransformation;
    }

    private void checkSequence() {
        final List<Integer> transformationPassList = new ArrayList<>(eventTransformationMap.keySet());
        for (int i = 0; i < transformationPassList.size(); i++) {
            if (transformationPassList.get(i) != i + 1) {
                throw new SequenceValidationException(format("Transformation passes are not in sequence %s", transformationPassList.toString()));
            }
        }
    }
}

