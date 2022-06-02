package Model;

import java.util.ArrayList;
import java.util.List;

public class Subscription {
    List<SubscriptionEntry> subscriptionConstraints;

    public Subscription() {
        subscriptionConstraints = new ArrayList<>();
    }

    public Subscription(List<SubscriptionEntry> subscriptionConstraints) {
        this.subscriptionConstraints = subscriptionConstraints;
    }

    public List<SubscriptionEntry> getSubscriptionConstraints() {
        return subscriptionConstraints;
    }

    public void setSubscriptionConstraints(List<SubscriptionEntry> subscriptionConstraints) {
        this.subscriptionConstraints = subscriptionConstraints;
    }

    public void addSubscriptionEntry(SubscriptionEntry subscriptionEntry){
        subscriptionConstraints.add(subscriptionEntry);
    }

    @Override
    public String toString() {
        return "Subscription{" +
                "subscriptionConstraints=" + subscriptionConstraints +
                '}';
    }
}
