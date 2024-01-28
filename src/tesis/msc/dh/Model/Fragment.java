package tesis.msc.dh.Model;

import java.util.ArrayList;

/**
 * Clase que encapsula toda la informacion relacionada a un fragmento de un
 * esquema propuesto obtenido a partir de un log
 *
 * @author Abraham Castillo García
 */
public class Fragment {

    private String id_fragment;
    private String name;
    private double numberofOperations; //nOperaciones
    private double performance; //ndesempenio
    private boolean cbir;
    private double nperformanceLineLog;
    private double noperationsLineLog;
    private ArrayList<Atributo> attributes;
    private String predicate;
    private String site;
    private long tuples; //número de tuplas
    private String percent;
    private String predicadoTraslape;  //overlapPredicate
    private boolean fragmenTable;
    private String fragmentIP;
    private double costH; //costH
    private double costV;  //costV
    private double cost;
    private ArrayList<Integer> id_operation = new ArrayList<Integer>();
    private String siteh;
    private String sitev;
    private double approximateOperations;
    private boolean flag;

    public Fragment() {

    }

    public String getId_fragment() {
        return id_fragment;
    }

    /**
     * @param id_fragmento the id_fragment to set
     */
    public void setId_fragment(String id_fragmento) {
        this.id_fragment = id_fragmento;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getNumberofOperations() {
        return numberofOperations;
    }

    public void setNumberofOperations(double numberofOperations) {
        this.numberofOperations = numberofOperations;
    }

    public double getPerformance() {
        return performance;
    }

    public void setPerformance(double performance) {
        this.performance = performance;
    }

    /**
     * @return the cbir
     */
    public boolean isCbir() {
        return cbir;
    }

    /**
     * @param cbir the cbir to set
     */
    public void setCbir(boolean cbir) {
        this.cbir = cbir;
    }

    public double getNperformanceLineLog() {
        return nperformanceLineLog;
    }

    public void setNperformanceLineLog(double nperformanceLineLog) {
        this.nperformanceLineLog = nperformanceLineLog;
    }

    public double getNoperationsLineLog() {
        return noperationsLineLog;
    }

    public void setNoperationsLineLog(double noperationsLineLog) {
        this.noperationsLineLog = noperationsLineLog;
    }

    public void setPredicate(String predicate) {
        this.predicate = predicate;
    }

    public String getPredicate() {
        return predicate;
    }

    public ArrayList<Atributo> getAttributes() {
        return attributes;
    }

    public void setAttributes(ArrayList<Atributo> attributes) {
        this.attributes = attributes;
    }

    public String getSite() {
        return site;
    }

    public void setSite(String site) {
        this.site = site;
    }

    public long getTuples() {
        return tuples;
    }

    public void setTuples(long tuples) {
        this.tuples = tuples;
    }

    public String getPercent() {
        return percent;
    }

    public void setPercent(String percent) {
        this.percent = percent;
    }

    @Override
    public String toString() {
        return "{name:" + this.name + ",predicate:" + this.predicate + ""
                + ",site:" + this.site + ", tuples" + this.tuples + "}";
    }

    /**
     * @return the predicadoTraslape
     */
    public String getPredicadoTraslape() {
        return predicadoTraslape;
    }

    /**
     * @param predicadoTraslape the predicadoTraslape to set
     */
    public void setPredicadoTraslape(String predicadoTraslape) {
        this.predicadoTraslape = predicadoTraslape;
    }

    public String getFragmentIP() {
        return fragmentIP;
    }

    public void setFragmentIP(String fragmentIP) {
        this.fragmentIP = fragmentIP;
    }

    public boolean isFragmenTable() {
        return fragmenTable;
    }

    public void setFragmenTable(boolean fragmenTable) {
        this.fragmenTable = fragmenTable;
    }

    /**
     * @return the costH
     */
    public double getCostH() {
        return costH;
    }

    /**
     * @param costH the costH to set
     */
    public void setCostH(double costH) {
        this.costH = costH;
    }

    /**
     * @return the costV
     */
    public double getCostV() {
        return costV;
    }

    /**
     * @param costV the costV to set
     */
    public void setCostV(double costV) {
        this.costV = costV;
    }

    /**
     * @return the cost
     */
    public double getCost() {
        return cost;
    }

    /**
     * @param cost the cost to set
     */
    public void setCost(double cost) {
        this.cost = cost;
    }

    /**
     * @return the siteh
     */
    public String getSiteh() {
        return siteh;
    }

    /**
     * @param siteh the siteh to set
     */
    public void setSiteh(String siteh) {
        this.siteh = siteh;
    }

    /**
     * @return the sitev
     */
    public String getSitev() {
        return sitev;
    }

    /**
     * @param sitev the sitev to set
     */
    public void setSitev(String sitev) {
        this.sitev = sitev;
    }

    public double getApproximateOperations() {
        return approximateOperations;
    }

    public void setApproximateOperations(double approximateOperations) {
        this.approximateOperations = approximateOperations;
    }

    public ArrayList<Integer> getId_operation() {
        return id_operation;
    }

    public void setId_operation(ArrayList<Integer> id_operation) {
        this.id_operation = id_operation;
    }

    public boolean isFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }

}
