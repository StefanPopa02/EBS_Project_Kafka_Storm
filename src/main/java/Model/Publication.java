package Model;

import java.util.HashMap;
import java.util.Map;

public class Publication {
    Map<String, String> publicationFields;

    public Publication(Map<String, String> publicationFields) {
        this.publicationFields = publicationFields;
    }

    public Publication() {
        publicationFields = new HashMap<>();
    }

    public Map<String, String> getPublicationFields() {
        return publicationFields;
    }

    public void setPublicationFields(Map<String, String> publicationFields) {
        this.publicationFields = publicationFields;
    }

    public void addPublicationField(String key, String value){
        publicationFields.put(key, value);
    }

    @Override
    public String toString() {
        return "Publication{" +
                "publicationFields=" + publicationFields +
                '}';
    }
}
