package Database;

import Model.Subscription;

import java.util.List;

public class RoutingTableEntry {
    String sourceId;
    List<String> subscriptions;

    RoutingTableEntry(){}

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public List<String> getSubscriptions() {
        return subscriptions;
    }

    public void setSubscriptions(List<String> subscriptions) {
        this.subscriptions = subscriptions;
    }
}
