package tesis.msc.dh.Filter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import tesis.msc.dh.FOperations.FragmentationOperationsMongo;
import tesis.msc.dh.Model.Operation;

/**
 *
 * @author OCrescencioR
 */
public class FilterMongoDB extends Filter implements Serializable {

    private List<String> attributes;

    public FilterMongoDB(String site) {
        super(site);
    }

    @Override
    public List<Operation> determineOperations(List<String> lines) {
        this.operations = new ArrayList<>();
        this.attributes = ((FragmentationOperationsMongo) this.FO).getAtributes();
        for (String line : lines) {

            if (line.contains("find")) {
                joinOperations(select(line));
            }

            if (line.contains("update")) {
                joinOperations(update(line));
            }

            if (line.contains("remove")) {
                joinOperations(delete(line));
            }

            if (line.contains("insert")) {
                joinOperations(insert(line));
            }
        }
        return operations;
    }

    @Override
    protected void joinOperations(List<Operation> tempOp) {
        int index;
        for (Operation op : tempOp) {
            index = this.operations.indexOf(op);
            if (index >= 0) {
                Operation cop = this.operations.get(index);
                if (cop.getTuples() == op.getTuples()) {
                    cop.setFrequency(cop.getFrequency() + 1);
                } else {
                    this.operations.add(op);
                }

            } else {
                this.operations.add(op);
            }
        }
    }

    @Override
    protected List<Operation> select(String row) {
        List<Operation> items = new ArrayList<>();
        List<String> predicates = new ArrayList<>();
        pattern = Pattern.compile("filter:\\{(.*?)\\},lsid:");
        match = pattern.matcher(row);
        if (match.find() && !match.group(1).equals("")) {
            predicates.addAll(this.buildPredicates(match.group(1)));

            pattern = Pattern.compile("nreturned:(.*?),");
            match = pattern.matcher(row);
            match.find();
            tuples = Integer.parseInt(match.group(1));
            if (tuples > 0) {
                pattern = Pattern.compile(regexSite);
                match = pattern.matcher(row);
                match.find();
                predicates.forEach(p -> {
                    Operation item = new Operation("READ", p, 1, tuples, match.group(1), !site.contains(match.group(1)));
                    items.add(item);
                });
            }
        }
        return items;
    }

    @Override
    protected List<Operation> update(String row) {
        System.out.println(row);
        List<Operation> items = new ArrayList<>();
        List<String> predicates;
        pattern = Pattern.compile("q:\\{(.*?)\\},u:");
        match = pattern.matcher(row);
        if (match.find() && !match.group(1).equals("")) {
            predicates = this.buildPredicates(match.group(1));
            pattern = Pattern.compile("nModified:(.*?),");
            match = pattern.matcher(row);
            try {
                match.find();
                tuples = Integer.parseInt(match.group(1));
            } catch (Exception e) {
                tuples = 0;
            }
            if (tuples > 0) {
                pattern = Pattern.compile(regexSite);
                match = pattern.matcher(row);
                match.find();
                predicates.forEach(p -> {
                    Operation item = new Operation("UPDATE", p, 3, tuples, match.group(1), !site.contains(match.group(1)));
                    items.add(item);
                });
            }
        }
        return items;
    }

    @Override
    protected List<Operation> delete(String row) {
        List<Operation> items = new ArrayList<>();
        List<String> predicates;
        pattern = Pattern.compile("q:\\{(.*?)\\},limit");
        match = pattern.matcher(row);
        if (match.find() && !match.group(1).equals("")) {
            predicates = this.buildPredicates(match.group(1));
            pattern = Pattern.compile("ndeleted:(.*?),");
            match = pattern.matcher(row);
            match.find();
            tuples = Integer.parseInt(match.group(1));
            if (tuples > 0) {
                pattern = Pattern.compile(regexSite);
                match = pattern.matcher(row);
                match.find();
                predicates.forEach(p -> {
                    Operation item = new Operation("DELETE", p, 2, tuples, match.group(1), !site.contains(match.group(1)));
                    items.add(item);
                });
            }
        }
        return items;
    }

    @Override
    protected List<Operation> insert(String row) {
        List<Operation> items = new ArrayList<>();

        pattern = Pattern.compile("ninserted:(.*?),");
        match = pattern.matcher(row);
        match.find();
        tuples = Integer.parseInt(match.group(1));
        if (tuples > 0) {
            pattern = Pattern.compile(regexSite);
            match = pattern.matcher(row);
            match.find();
            this.attributes.forEach(p -> {
                Operation item = new Operation("CREATE", p, 2, tuples, match.group(1), !site.contains(match.group(1)));
                items.add(item);
            });
        }

        return items;
    }

    protected List<String> buildPredicates(String predicates) {
        return Arrays.asList(predicates.replaceAll("[{}]", "").replaceAll("\\$eq:", "").split(","));
    }

}
