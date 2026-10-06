package com.github.lechandonga.localeventbrokerjava.broker;

import java.util.ArrayList;
import java.util.List;

public class TopicStatus {
    private String topic;
    private int totalEvents;
    private int capacity;
    private List<GroupStatus> groups = new ArrayList<>();

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public int getTotalEvents() { return totalEvents; }
    public void setTotalEvents(int totalEvents) { this.totalEvents = totalEvents; }
    public int getCapacity() { return capacity; }
    public void setCapacity(int capacity) { this.capacity = capacity; }
    public List<GroupStatus> getGroups() { return groups; }
    public void setGroups(List<GroupStatus> groups) { this.groups = groups; }
}
