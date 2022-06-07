package Deserializer;

import Model.Publication;
import com.google.gson.*;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MyPublicationListDeserializer implements JsonDeserializer {
    @Override
    public List<Publication> deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        List<Publication> publicationList = new ArrayList<>();
        JsonArray array = jsonElement.getAsJsonArray();
        for (int i = 0; i < array.size(); i++) {
            JsonArray pubArr = (JsonArray) array.get(i);
            Publication tmpPublication = new Publication();
            for (int j = 0; j < pubArr.size(); j++) {
                String field = ((JsonArray) pubArr.get(j)).get(0).getAsString();
                switch(field){
                    case "Company":
                        String valueCompany = ((JsonArray) pubArr.get(j)).get(1).getAsString();
                        tmpPublication.setCompany(valueCompany);
                        break;
                    case "Date":
                        try {
                            Date valueDate = new SimpleDateFormat("yyyy-MM-dd").parse(((JsonArray) pubArr.get(j)).get(1).getAsString());
                            tmpPublication.setDate(valueDate);
                            break;
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                    case "Value":
                        Double valueDbl = ((JsonArray) pubArr.get(j)).get(1).getAsDouble();
                        tmpPublication.setValue(valueDbl);
                        break;
                    case "Drop":
                        Double dropDbl = ((JsonArray) pubArr.get(j)).get(1).getAsDouble();
                        tmpPublication.setDrop(dropDbl);
                        break;
                    case "Variation":
                        Double variationDbl = ((JsonArray) pubArr.get(j)).get(1).getAsDouble();
                        tmpPublication.setVariation(variationDbl);
                        break;
                }
            }
            publicationList.add(tmpPublication);
        }
        return publicationList;
    }
}
