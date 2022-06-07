package Deserializer;

import Model.Subscription;
import com.google.gson.*;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MySubscriptionListDeserializer implements JsonDeserializer {
    @Override
    public List<Subscription> deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        List<Subscription> subscriptionList = new ArrayList<>();
        JsonArray array = jsonElement.getAsJsonArray();
        for (int i = 0; i < array.size(); i++) {
            JsonArray subArr = (JsonArray) array.get(i);
            Subscription tmpSubscription = new Subscription();
            for (int j = 0; j < subArr.size(); j++) {
                String field = ((JsonArray) subArr.get(j)).get(0).getAsString();
                switch (field) {
                    case "Company":
                        String valueCompany = ((JsonArray) subArr.get(j)).get(2).getAsString();
                        tmpSubscription.setCompany(valueCompany);
                        break;
                    case "Date":
                        try {
                            Date valueDate = new SimpleDateFormat("yyyy-MM-dd").parse(((JsonArray) subArr.get(j)).get(2).getAsString());
                            tmpSubscription.setDate(valueDate);
                            break;
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                    case "Value":
                        Double valueDbl = ((JsonArray) subArr.get(j)).get(2).getAsDouble();
                        tmpSubscription.setValue(valueDbl);
                        break;
                    case "Drop":
                        Double dropDbl = ((JsonArray) subArr.get(j)).get(2).getAsDouble();
                        tmpSubscription.setDrop(dropDbl);
                        break;
                    case "Variation":
                        Double variationDbl = ((JsonArray) subArr.get(j)).get(2).getAsDouble();
                        tmpSubscription.setVariation(variationDbl);
                        break;
                }
                String operator = ((JsonArray) subArr.get(j)).get(1).getAsString();
                tmpSubscription.addFieldOp(field, operator);
            }
            subscriptionList.add(tmpSubscription);
        }
        return subscriptionList;
    }
}
