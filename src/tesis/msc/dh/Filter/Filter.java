package tesis.msc.dh.Filter;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import tesis.msc.dh.FOperations.FragmentationOperations;
import tesis.msc.dh.Model.Operation;

/**
 * Clase que encapsula el proceso de filtrar las operaciones de un archivo log
 *
 * @author Abraham Castillo
 */
public abstract class Filter implements Serializable {

    protected Pattern pattern;
    protected Matcher match;
    protected int tuples;
    protected String site;
    protected String regexSite = "remote:(.*?):";
    protected FragmentationOperations FO;
    protected List<Operation> operations;

    public Filter(String site) {
        this.site = site;
    }

    protected void joinOperations(List<Operation> tempOp) {
        int index;
        for (Operation op : tempOp) {
            index = this.operations.indexOf(op);
            if (index >= 0) {
                Operation cop = this.operations.get(index);
                cop.setFrequency(cop.getFrequency() + 1);

            } else {
                this.operations.add(op);
            }
        }
    }

    public abstract List<Operation> determineOperations(List<String> operations) throws Exception;

    protected abstract List<Operation> select(String row);

    protected abstract List<Operation> update(String row);

    protected abstract List<Operation> delete(String row);

    protected abstract List<Operation> insert(String row);

    public List<Operation> getOperations() {
        return operations;
    }

    public void setFO(FragmentationOperations FO) {
        this.FO = FO;
    }

}
