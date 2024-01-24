package tesis.msc.dh.FHorizontal;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import tesis.msc.dh.Model.MCRUD;
import tesis.msc.dh.Model.Operation;

/**
 *
 * @author Abraham Castillo
 */
public class BuilderMCRUD {

    private Map<String, List<MCRUD>> MCRUDF;
    private Map<String, Long> sites;
    private List<MCRUD> mcrud;
    private long operationTuples;
    private long totalFrecuency;
    private long totalPerformance;

    public void buildMCRUD(List<Operation> operations) {
        MCRUDF = new HashMap<>();
        this.sites = new HashMap<>();
        mcrud = new ArrayList<>();
        operations.stream().map(operation -> {
            if (MCRUDF.containsKey(operation.getPredicate())) {
                int index = this.findBySite(MCRUDF.get(operation.getPredicate()), operation.getSite());
                if (index >= 0) {
                    MCRUD item = MCRUDF.get(operation.getPredicate()).get(index);
                    item.setFrequency(item.getFrequency() + 1);
                    item.setCost(item.getCost() + operation.getCost());
                    item.setTuples(item.getTuples() + operation.getTuples());
                    item.setPerformance(item.getPerformance() + operation.getPerformance());

                } else {

                    MCRUDF.get(operation.getPredicate())
                            .add(this.addItem(operation));
                }

            } else {
                List<MCRUD> list = new ArrayList<>();
                list.add(this.addItem(operation));
                MCRUDF.put(operation.getPredicate(), list);

            }
            return operation;
        }).forEachOrdered(operation -> {
            this.sites.put(operation.getSite(), 0L);
            this.operationTuples += operation.getTuples();
            this.totalFrecuency += operation.getFrequency();
            this.totalPerformance += operation.getPerformance();
        });

        MCRUDF.entrySet().stream().map(row -> {
            MCRUD item = row.getValue().stream()
                    .max(Comparator.comparingLong(MCRUD::getFrequency)).get();
            MCRUD tempItem = new MCRUD(item.getCost(), item.getPredicate(), item.getAtribute(), item.getSite());
            long cost = row.getValue().stream()
                    .map(MCRUD::getCost).reduce(0L, Long::sum);
            long frecuency = operations.stream()
                    .filter(o -> o.getPredicate().contains(item.getPredicate()))
                    .map(o -> o.getFrequency()).reduce(0l, Long::sum);
            long performance = operations.stream()
                    .filter(o -> o.getPredicate().contains(item.getPredicate()))
                    .map(o -> o.getPerformance()).reduce(0l, Long::sum);

            tempItem.setCost(cost);
            tempItem.setFrequency(frecuency);
            tempItem.setPerformance(performance);
            return tempItem;
        }).forEachOrdered(item -> {
            mcrud.add(item);
        });

    }

    private MCRUD addItem(Operation operation) {

        MCRUD item = new MCRUD(operation.getCost(),
                operation.getPredicate(),
                operation.getAttribute(),
                operation.getSite());
        item.setFrequency(operation.getFrequency());
        item.setTuples(operation.getTuples());
        item.setPerformance(operation.getPerformance());
        return item;
    }

    private int findBySite(List<MCRUD> items, String site) {
        int count = 0;
        int index = -1;
        for (MCRUD tempMcrud : items) {
            if (tempMcrud.getSite().equals(site)) {
                index = count;
            }
            count++;
        }
        return index;
    }

    private MCRUD findElement(String predicate) {
        return this.mcrud.stream()
                .filter(v -> v.getPredicate().equals(predicate))
                .collect(Collectors.toList()).get(0);
    }

    public List<String> getSites() {
        return new ArrayList<>(this.sites.keySet());
    }

    public List<String> getPredicatesByfrecuency() {
        return new ArrayList<>(this.MCRUDF.keySet());
    }

    public List<Long> getFrecuencySite(String currentPredicate) {
        List<MCRUD> items = this.MCRUDF.get(currentPredicate);
        this.sites.replaceAll((k, v) -> 0L);
        items.forEach(item -> {
            this.sites.put(item.getSite(), item.getFrequency());
        });
        return new ArrayList<>(this.sites.values());
    }

    public String getSiteMax(String predicate) {
        return findElement(predicate).getSite();
    }

    public Long getGeneralCost(String predicate) {
        return findElement(predicate).getCost();
    }

    public List<MCRUD> getMCRUD() {
        return mcrud;
    }

    public long getTotalTuples() {
        return this.operationTuples;
    }

    public long getTotalFrecuency() {
        return this.totalFrecuency;
    }

    public long getTotalPerformance() {
        return totalPerformance;
    }

}
