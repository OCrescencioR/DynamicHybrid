package tesis.msc.dh.FHorizontal;

import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import tesis.msc.dh.FOperations.FragmentationOperations;
import tesis.msc.dh.FOperations.FragmentationOperationsCreator;
import tesis.msc.dh.Filter.Filter;
import tesis.msc.dh.Filter.FilterCreator;
import tesis.msc.dh.Log.LogReader;
import tesis.msc.dh.Log.LogReaderCreator;
import tesis.msc.dh.Model.MCRUD;
import tesis.msc.dh.Model.BD;
import tesis.msc.dh.Scheme.Schema;

/**
 * Clase que encapsula todo el comportamiento para realizar una fragmentacion
 * horizontal dinamica
 *
 * @author Abraham Castillo
 */
public class DHFragmentation implements Serializable {

    private LogReader logReader;
    private Filter filter;
    private FragmentationOperations FO;
    private BuilderMCRUD MCRUD;
    private List<MCRUD> alp;
    private Schema schema;
    //AccessDataXamana xamana;
    private int percent;
    private long token;
     private String dirLogs;

    public DHFragmentation(BD bd) throws Exception {
        this.logReader = new LogReaderCreator().createLogReader(bd.getTipoBD(), bd.getDirbd());
        this.filter = new FilterCreator().createFilter(bd.getTipoBD(), bd.getDirbd());
        this.FO = FragmentationOperationsCreator.createFragmentationOperations(bd);
        this.MCRUD = new BuilderMCRUD();
        this.schema = new Schema(bd);
        //this.xamana = new AccessDataXamana(BD);
        //Se invoca a AccessXamana en el método de registerInfo() para almacenar la información en Xamana

    }

    public void implementMethod(String dirLogs, String database, String table) throws Exception {
        List<String> operations = this.logReader.determineOperations02(dirLogs, database, table);
        this.FO.initialize();
        this.filter.setFO(FO);
        this.filter.determineOperations(operations);
        this.MCRUD.buildMCRUD(this.filter.getOperations());
        this.builderALP(this.MCRUD.getMCRUD());
        this.schema.buildDesignSchema(alp, this.MCRUD.getMCRUD(), this.FO.getCurrentTuples(), this.MCRUD.getTotalFrecuency(), this.MCRUD.getTotalPerformance());

    }

    private void builderALP(List<MCRUD> mcrud) {
        this.alp = new ArrayList<>();
        int index = -1;
        for (MCRUD item : mcrud) {
            index = this.alp.indexOf(item);
            if (index >= 0) {
                MCRUD currentItem = this.alp.get(index);
                currentItem.setCost(currentItem.getCost() + item.getCost());
            } else {
                MCRUD tempElement = new MCRUD(item.getCost(),
                        item.getPredicate(),
                        item.getAtribute(),
                        item.getSite());

                this.alp.add(tempElement);
            }

        }

    }

    public Filter getFilterInfo() {
        return filter;
    }

    public BuilderMCRUD getMCRUD() {
        return MCRUD;
    }

    public List<MCRUD> getAlp() {
        return this.alp;
    }

    public Schema getSchema() {
        return this.schema;
    }

    public int getPercent() {
        return this.percent;
    }

    public long getToken() {
        return this.token;
    }

}
