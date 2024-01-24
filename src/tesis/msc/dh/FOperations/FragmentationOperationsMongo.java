package tesis.msc.dh.FOperations;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.List;
import java.util.stream.Collectors;
import org.bson.Document;
import tesis.msc.dh.Data.DataAccessMongoDB;
import tesis.msc.dh.Model.BD;

/**
 *
 * @author Abraham Castillo
 */
public class FragmentationOperationsMongo implements FragmentationOperations {

    DataAccessMongoDB ACD;
    Long currentTuples;
    private List<String> atributes;
    String database;
    String table;

    public FragmentationOperationsMongo(BD bd) {
        ACD = bd.getAccexternomongo();
        this.database = bd.getNombd();
        this.table = bd.getTabla().getNombre();
    }

    @Override
    public void initialize() {
        this.atributes = this.ACD.attributes(database, table).stream()
                .filter(v -> !v.contains("_id"))
                .collect(Collectors.toList());
        MongoDatabase db = this.ACD.getMongoClient().getDatabase(this.database);
        MongoCollection<Document> colec = db.getCollection(this.table);
        this.currentTuples = (Long) colec.countDocuments();
        System.out.println(this.currentTuples);
    }

    public List<String> getAtributes() {
        return atributes;
    }

    @Override
    public long getCurrentTuples() {
        return this.currentTuples;
    }

}
