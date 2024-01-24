package tesis.msc.dh.Model;

/**
 *
 * @author Abraham Castillo
 */
public class Operation {

    private String predicate;
    private int valueOperation;
    private boolean isRemote;
    private long tuples;
    private String site;
    private long cost;
    private long frequency;
    private String typeOperation;
    private long performance;

    public Operation(String typeOperation, String predicate, int valueOperation, long tuples, String site, boolean isRemote) {
        this.predicate = predicate;
        this.valueOperation = valueOperation;
        this.tuples = tuples;
        this.site = site;
        this.isRemote = isRemote;
        this.frequency = 1;
        this.typeOperation = typeOperation;
        calculateCost();

    }

    @Override
    public boolean equals(Object obj) {
        Operation temp = (Operation) obj;

        return this.site.equals(temp.site)
                && this.valueOperation == temp.valueOperation
                && this.predicate.equals(temp.predicate);

    }

    private void calculateCost() {

        if (isRemote) {
            this.cost = this.frequency * this.valueOperation * this.tuples * 2;
            this.performance = this.frequency * 2;
        } else {
            this.cost = this.frequency * this.valueOperation * this.tuples;
            this.performance = this.frequency;
        }

    }

    public String getPredicate() {
        return predicate;
    }

    public int getValueOperation() {
        return valueOperation;
    }

    public boolean isIsRemote() {
        return isRemote;
    }

    public void setIsRemote(boolean isRemote) {
        this.isRemote = isRemote;
    }

    public long getTuples() {
        return tuples;
    }

    public long getCost() {
        return this.cost;
    }

    public void setCost(long cost) {
        this.cost = cost;
    }

    public String getAttribute() {
        return this.predicate.split(":|=|!=|>=|<=|<|>")[0];
    }

    public String getSite() {
        return this.site;
    }

    public long getFrequency() {
        return frequency;
    }

    public void setFrequency(long frequency) {
        this.frequency = frequency;
        calculateCost();
    }

    public void setTuples(long tuples) {
        this.tuples = tuples;
        calculateCost();
    }

    public String getTypeOperation() {
        return this.typeOperation;
    }

    public long getPerformance() {
        if (isRemote) {

            this.performance = this.frequency * 2;
        } else {

            this.performance = this.frequency;
        }
        return performance;
    }

    @Override
    public String toString() {

        return "{predicate:" + predicate + ",tuples:" + tuples + ",cost:"
                + cost + ",attribute:" + getAttribute()
                + ",site:" + site + "}";

    }

}
