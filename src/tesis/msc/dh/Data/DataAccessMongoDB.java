package tesis.msc.dh.Data;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Aggregates.project;
import com.mongodb.client.model.Projections;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Clase que hereda los métodos abstractos de la clase "AccesoDatos", para el
 * acceso a la base de datos MongoDB. Driver, sUrl, sUsr, sPwd
 *
 * @author Felipe Castro Medina
 */
public class DataAccessMongoDB extends DataAccess implements Serializable {

    public String sUrl = null; //sURL de conexión
    public String ip = null;
    public String sUsr = null; //Usuario de la base de datos
    public String sPwd = null; //Contraseña de la base de datos
    public String tabla = null;
    private MongoClient mongoClient;  // La conexión

    public DataAccessMongoDB(String url, String nombd, String tabla, String usr, String pwd) throws Exception {
        ip = url.split(":")[0];
        sUrl = url;//"jdbc:mysql://localhost/Master";
        sUsr = usr;//"root";
        sPwd = pwd;//"sunny_4teamo";
        this.setTabla(tabla);

    }

    @Override
    public boolean connect() throws Exception {

        String mongoClientURI = "";
        //System.out.println("Url:" + sUrl +" "+ "Usuario:" + sUsr +" "+"Contraseña:" +sPwd );
        if (sUsr.isEmpty()) {
            mongoClientURI = "mongodb://" + sUrl + "/";
        } else {
            mongoClientURI = "mongodb://" + sUsr + ":" + sPwd + "@" + sUrl + "/";
        }
        //System.out.println("Desde connect" + mongoClientURI);
        setMongoClient(MongoClients.create(mongoClientURI));

        return getMongoClient() != null;

    }

    @Override
    public void disconnect() throws Exception {
        this.mongoClient.close();
    }

    @Override
    public synchronized ArrayList<String> attributes(String nombreBase, String nombre) {
        ArrayList<String> res = new ArrayList<>();
        MongoDatabase db = this.getMongoClient().getDatabase(nombreBase);
        MongoCollection<Document> collection = db.getCollection(nombre);
        Document doc = collection.find().first();
        try {
            System.out.println(nombreBase + " " + nombre + " " + sUrl);
            Iterator js = new JSONObject(doc.toJson()).keys();
            while (js.hasNext()) {
                String key = js.next().toString();
                Bson group = project(Projections.computed(key, Document.parse("{$type: '$" + key + "'}")));
                List<Document> r = collection.aggregate(Arrays.asList(group)).into(new ArrayList<>());
                String tipo = "" + (new JSONObject(r.get(0).toJson()).get(key));
                System.out.println(key + ":" + tipo);
                res.add(key + ":" + tipo);
            }
        } catch (JSONException ex) {
            ex.printStackTrace();
        }
        return res;
    }

    /**
     * @return the tabla
     */
    public String getTabla() {
        return tabla;
    }

    /**
     * @param tabla the tabla to set
     */
    public void setTabla(String tabla) {
        this.tabla = tabla;
    }

    /**
     * @return the mongoClient
     */
    public MongoClient getMongoClient() {
        return mongoClient;
    }

    /**
     * @param mongoClient the mongoClient to set
     */
    public void setMongoClient(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

}
