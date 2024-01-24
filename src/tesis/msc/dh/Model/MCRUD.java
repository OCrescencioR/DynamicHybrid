package tesis.msc.dh.Model;

import java.io.Serializable;

/**
 *
 * @author Abraham Castillo
 */
public class MCRUD implements Serializable {

    private long cost;
    private String predicate;
    private String attribute;
    private String site;
    private long frequency;
    private long tuples;
    private long performance;

    public MCRUD(long cost, String predicate, String attribute, String site) {
        this.cost = cost;
        this.predicate = predicate;
        this.attribute = attribute;
        this.site = site;
    }

    @Override
    public boolean equals(Object obj) {
        MCRUD temp = (MCRUD) obj;
        return this.attribute.equals(temp.attribute);
    }

    public long getCost() {
        return cost;
    }

    public void setCost(long cost) {
        this.cost = cost;
    }

    public String getPredicate() {
        return predicate;
    }

    public void setPredicate(String predicate) {
        this.predicate = predicate;
    }

    public String getAtribute() {
        return attribute;
    }

    public void setAtribute(String atribute) {
        this.attribute = atribute;
    }

    public String getSite() {
        return site;
    }

    public void setSite(String site) {
        this.site = site;
    }

    public long getFrequency() {
        return frequency;
    }

    public void setFrequency(long frequency) {
        this.frequency = frequency;
    }

    public long getTuples() {
        return tuples;
    }

    public void setTuples(long tuples) {
        this.tuples = tuples;
    }

    public long getPerformance() {
        return performance;
    }

    public void setPerformance(long performance) {
        this.performance = performance;
    }

    @Override
    public String toString() {
        return "{predicate:" + this.predicate
                + ",cost:" + this.cost + ",site:" + this.site + "}";
    }

}
