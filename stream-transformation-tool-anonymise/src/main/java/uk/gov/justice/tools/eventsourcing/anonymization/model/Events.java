package uk.gov.justice.tools.eventsourcing.anonymization.model;


import java.util.List;

public class Events {

    private List<Event> events;

    public Events() {
    }

    public Events(final List<Event> events) {
        this.events = events;
    }

    public List<Event> getEvents() {
        return events;
    }
}
