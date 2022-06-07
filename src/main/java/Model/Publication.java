package Model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Publication {
    private String company;
    private Double value;
    private Date date;
    private Double drop;
    private Double variation;

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

    public Publication() {
    }

    public Publication(String company, Double value, Date date, Double drop, Double variation) {
        this.company = company;
        this.value = value;
        this.date = date;
        this.drop = drop;
        this.variation = variation;
    }

    @Override
    public String toString() {
        return "Publication{" +
                "company='" + company + '\'' +
                ", value=" + value +
                ", date=" + date +
                ", drop=" + drop +
                ", variation=" + variation +
                '}';
    }
}
