package tesis.msc.dh.Scheme;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import tesis.msc.dh.FHorizontal.Register;
import tesis.msc.dh.Model.Fragment;
import tesis.msc.dh.Model.MCRUD;
import tesis.msc.dh.Model.BD;

/**
 *
 * @author Abraham Castillo
 */
public class Schema implements Serializable {

    private List<Fragment> schema;
    private MCRUD attribute;
    private BuilderFraments BF;

    public Schema(BD bd) {
      this.BF = new BuilderFragmentsCreator().createBuilderFragments(bd);
    }

    public void buildDesignSchema(List<MCRUD> ALP, List<MCRUD> mcrud,
            long currentTuples, long totalFrecuency, long totalPerformace) throws Exception {
        DecimalFormat formatter = new DecimalFormat("##########.##");
        this.attribute = ALP.stream().max(
                Comparator.comparingLong((i) -> i.getCost())
        ).get();

        List<MCRUD> currentItems;

        currentItems = mcrud.stream()
                .filter(p -> p.getPredicate().contains(this.attribute.getAtribute()))
                .sorted(Comparator.comparingLong(MCRUD::getCost).reversed())
                .collect(Collectors.toList());
        this.schema = this.BF.buildDesignSchema(currentItems, attribute);
        Double currentFrecuency = this.schema.stream().map(f -> f.getApproximateOperations()).reduce(0d, Double::sum);
        Double currentPerformace = this.schema.stream().map(f -> f.getPerformance()).reduce(0d, Double::sum);
        this.schema.forEach(fragment -> {
            String percent = formatter.format(((double) fragment.getTuples() * 100) / currentTuples);
            fragment.setPercent(percent);
            if (fragment.getPredicate().contains("Sin predicado")) {
                fragment.setApproximateOperations(totalFrecuency - currentFrecuency);
                fragment.setPerformance(totalPerformace - currentPerformace);
            }

        });

    }

    public void buildSchema(Register register) throws Exception {
        this.BF.buildSchema(this.schema, register);
    }

    public List<Fragment> getSchema() {
        return schema;
    }

}
