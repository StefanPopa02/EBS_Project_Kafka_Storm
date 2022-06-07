package Model;

import java.util.*;

public class Subscription {
    private String company;
    private Double value;
    private Date date;
    private Double drop;
    private Double variation;
    private Map<String, String> fieldOperator;

    public Subscription() {
        fieldOperator = new HashMap<>();
    }

    public Subscription(String company, Double value, Date date, Double drop, Double variation, Map<String, String> fieldOperator) {
        this.company = company;
        this.value = value;
        this.date = date;
        this.drop = drop;
        this.variation = variation;
        this.fieldOperator = fieldOperator;
    }

    public void addFieldOp(String field, String operator){
        fieldOperator.put(field, operator);
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Double getDrop() {
        return drop;
    }

    public void setDrop(Double drop) {
        this.drop = drop;
    }

    public Double getVariation() {
        return variation;
    }

    public void setVariation(Double variation) {
        this.variation = variation;
    }

    public Map<String, String> getFieldOperator() {
        return fieldOperator;
    }

    public void setFieldOperator(Map<String, String> fieldOperator) {
        this.fieldOperator = fieldOperator;
    }

    @Override
    public String toString() {
        return "Subscription{" +
                "company='" + company + '\'' +
                ", value=" + value +
                ", date=" + date +
                ", drop=" + drop +
                ", variation=" + variation +
                ", fieldOperator=" + fieldOperator +
                '}';
    }
}
