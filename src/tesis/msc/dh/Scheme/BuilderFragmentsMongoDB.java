package tesis.msc.dh.Scheme;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;
import tesis.msc.dh.Data.DataAccessMongoDB;
import tesis.msc.dh.FHorizontal.Register;
import tesis.msc.dh.Model.Fragment;
import tesis.msc.dh.Model.MCRUD;
import tesis.msc.dh.Model.BD;

/**
 *
 * @author Abragam Castillo
 */
public class BuilderFragmentsMongoDB implements BuilderFraments {

    DataAccessMongoDB ACD;
    MongoDatabase db;
    List<Bson> operations;
    String table;
    String address;
    String password;
    String user;
    String port;
    String nameDatabase;
    String regexOperations = ":\\$eq:|:\\$gt:|:\\$gte:"
            + "|:\\$lt:|:\\$lte:|:\\$ne:|:";
    String type;
    MCRUD attribute;

    public BuilderFragmentsMongoDB(BD bd) {
        db = bd.getAccexternomongo()
                .getMongoClient().getDatabase(bd.getNombd());
        this.operations = new ArrayList<>();
        this.table = bd.getTabla().getNombre();
        this.address = bd.getDirbd();
        this.password = bd.getPassbd();
        this.user = bd.getUsubd();
        this.port = bd.getPortbd();
        this.nameDatabase = bd.getNombd();

    }

    @Override
    public List<Fragment> buildDesignSchema(List<MCRUD> mcrud, MCRUD attribute) throws Exception {
        int id = 1;
        Fragment fragment = null;

        List<Fragment> schema = new ArrayList<>();
        long tuples = 0L;
        this.attribute = attribute;
        this.determineType(attribute);

        for (MCRUD item : mcrud) {
            tuples = countQuery(item.getPredicate());
            if (tuples > 0) {
                fragment = new Fragment();
                fragment.setName(this.table + "_" + id);
                fragment.setPredicate(item.getPredicate());
                fragment.setSite(item.getSite());
                fragment.setTuples(tuples);
                fragment.setApproximateOperations(item.getFrequency());
                fragment.setPerformance(item.getPerformance());
                schema.add(fragment);
                id++;

            }

        }
        tuples = countQuery("");
        if (tuples > 0) {
            fragment = new Fragment();
            fragment.setName(this.table + "_" + id);
            fragment.setPredicate("Sin predicado");
            fragment.setSite(this.address);
            fragment.setTuples(tuples);
            fragment.setApproximateOperations(0L);
            schema.add(fragment);
        }
        this.operations.clear();
        return schema;
    }

    @Override
    public void buildSchema(List<Fragment> schema, Register register) throws Exception {
        DataAccessMongoDB tempACD = null;
        int i = 1;
        for (Fragment item : schema) {
            tempACD = new DataAccessMongoDB(item.getSite() + ":" + this.port,
                    this.nameDatabase, this.table, this.user, this.password);
            if (tempACD.connect()) {
                item.setName(determineNameFragment(item.getName(), i, item.getSite()));
                MongoDatabase mdb = tempACD.getMongoClient().getDatabase(this.nameDatabase);
                mdb.createCollection(item.getName());
                mdb.getCollection(item.getName()).insertMany(getDocuments(item.getPredicate()));
                register.fragment(item, this.attribute, this.type);
            }

        }
    }

    private long countQuery(String predicate) {
        long count = 0;
        Bson query = (predicate.isEmpty()) ? null : selectOperation(predicate);
        if (query == null && this.operations.size() > 0) {

            count = db.getCollection(table)
                    .find(
                            Filters.and(Filters.nor(this.operations))
                    ).into(new ArrayList<>()).size();
        }

        if (this.operations.size() > 0 && query != null) {

            count = db.getCollection(table)
                    .find(Filters.and(
                            query,
                            Filters.nor(this.operations)
                    )).into(new ArrayList<>()).size();
        } else if (query != null) {

            count = db.getCollection(table).find(query).into(new ArrayList<>()).size();
        }

        this.operations.add(query);
        return count;
    }

    private List<Document> getDocuments(String predicate) {
        List<Document> documents = null;
        Bson query = null;

        if (!predicate.contains("Sin predicado")) {
            query = selectOperation(predicate);
        }

        if (predicate.contains("Sin predicado")) {
            documents = db.getCollection(table)
                    .find(
                            Filters.and(Filters.nor(this.operations))
                    ).into(new ArrayList<>());
        }

        if (this.operations.size() > 0 && query != null && !predicate.contains("Sin predicado")) {

            documents = db.getCollection(table)
                    .find(Filters.and(
                            query,
                            Filters.nor(this.operations)
                    )).into(new ArrayList<>());
        }
        if (this.operations.isEmpty() && query != null && !predicate.contains("Sin predicado")) {
            documents = db.getCollection(table).find(query).into(new ArrayList<>());
        }
        if (!predicate.contains("Sin predicado")) {
            this.operations.add(query);
        }
        return documents;
    }

    private Bson selectOperation(String predicate) {

        String[] values = predicate.split(regexOperations);

        if (predicate.contains(":$eq:")) {
            return Filters.eq(values[0], buildValue(values[1]));
        }
        if (predicate.contains(":$gt:")) {
            return Filters.gt(values[0], buildValue(values[1]));
        }
        if (predicate.contains(":$gte:")) {
            return Filters.gte(values[0], buildValue(values[1]));
        }
        if (predicate.contains(":$lt:")) {
            return Filters.lt(values[0], buildValue(values[1]));
        }
        if (predicate.contains(":$lte:")) {
            return Filters.lte(values[0], buildValue(values[1]));
        }
        if (predicate.contains(":$ne:")) {
            return Filters.ne(values[0], buildValue(values[1]));
        }
        return new Document(values[0], buildValue(values[1]));
    }

    private void determineType(MCRUD attribute) {
        String field = attribute.getPredicate().split(regexOperations)[0];
        Document doc = db.getCollection(table).find(Filters.exists(field)).first();
        type = doc.get(field).getClass().getSimpleName();
    }

    private Object buildValue(String value) {
        Object tempValue = value;
        switch (type) {
            case "Double":
                return new Double(value);
            case "Integer":
                return new Integer(value);
            case "Boolean":
                return Boolean.valueOf(value);
            case "Long":
                return new Long(value);
            case "Byte":
                return new Byte(value);
            case "Short":
                return new Short(value);
            case "Float":
                return new Float(value);

        }
        return tempValue;
    }

    private String determineNameFragment(String fragment, int id, String site) throws Exception {

        String name = fragment;

        DataAccessMongoDB tempACD = new DataAccessMongoDB(site + ":" + this.port,
                this.nameDatabase, this.table, this.user, this.password);
        if (tempACD.connect()) {
            List<String> collections = tempACD.getMongoClient().getDatabase(this.nameDatabase).listCollectionNames().into(new ArrayList<>());
            while (collections.contains(name)) {
                name += "_" + id;
            }
        }
        return name;
    }

}
