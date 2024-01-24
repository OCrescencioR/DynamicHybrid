package tesis.msc.dh.Dynamic.Hybrid;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import static com.mongodb.client.model.Updates.unset;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;
import tesis.msc.dh.Data.DataAccess;
import tesis.msc.dh.Data.DataAccessMongoDB;
import tesis.msc.dh.Data.DataAccessXamana;
import tesis.msc.dh.Model.Atributo;
import tesis.msc.dh.Model.BD;
import tesis.msc.tools.Tools;

/**
 *
 * @author Felipe Castro Medina
 */
public class Fragmentador {

    private DataAccess origen;
    private DataAccess destino;
    private DataAccessXamana xamanaAcc;
    private ArrayList<Atributo> atts;
    private BD datosBD;
    private int nFragmento;
    private boolean fragmentacionCBIR;
    private double noperaciones;
    private double ndesempenio;
    private String sTablaOrigen;

    public Fragmentador(DataAccess origen, DataAccess destino, ArrayList<Atributo> atts, BD datosBD, int nFragmento, boolean fragmentacionCBIR, String sTablaOrigen) {
        this.setOrigen(origen);
        this.setDestino(destino);
        this.setAtts(atts);
        this.datosBD = datosBD;
        this.nFragmento = nFragmento;
        this.fragmentacionCBIR = fragmentacionCBIR;
        this.sTablaOrigen = sTablaOrigen;
    }

    //Cuando no es CBIR tiene valores en ndesempenio y noperaciones
    public Fragmentador(DataAccess origen, DataAccess destino, ArrayList<Atributo> atts, BD datosBD, int nFragmento, boolean fragmentacionCBIR, double noperaciones, double ndesempenio, String sTablaOrigen) {
        this.setOrigen(origen);
        this.setDestino(destino);
        this.setAtts(atts);
        this.datosBD = datosBD;
        this.nFragmento = nFragmento;
        this.fragmentacionCBIR = fragmentacionCBIR;
        this.noperaciones = noperaciones;
        this.ndesempenio = ndesempenio;
        this.sTablaOrigen = sTablaOrigen;
    }

    public boolean generarFragmentos() {
        if (this.datosBD.getTipoBD().compareTo("a") == 0) {
            //Aquí se debe de llamar al método para generar los fragmentos en MySQL generarFragmentosMySQL();
        } else if (this.datosBD.getTipoBD().compareTo("b") == 0 || this.datosBD.getTipoBD().compareTo("c") == 0) {
            //Aquí se debe de llamar al método para generar los fragmentos en PostgreSQL generarFragmentoPost();
        } else if (this.datosBD.getTipoBD().compareTo("d") == 0) {
            generarFragmentoMongo();
        } else {
            System.out.println("Error,no es una base de datos válida");
        }
        return true;
    }

    /**
     * @return the origen
     */
    public DataAccess getOrigen() {
        return origen;
    }

    /**
     * @param origen the origen to set
     */
    public void setOrigen(DataAccess origen) {
        this.origen = origen;
    }

    /**
     * @return the destino
     */
    public DataAccess getDestino() {
        return destino;
    }

    /**
     * @param destino the destino to set
     */
    public void setDestino(DataAccess destino) {
        this.destino = destino;
    }

    /**
     * @return the atts
     */
    public ArrayList<Atributo> getAtts() {
        return atts;
    }

    /**
     * @param atts the atts to set
     */
    public void setAtts(ArrayList<Atributo> atts) {
        this.atts = atts;
    }

    private void generarFragmentoMongo() {
        DataAccessMongoDB accOrigen = (DataAccessMongoDB) this.origen;
        DataAccessMongoDB accDestino = (DataAccessMongoDB) this.destino;
        try {
            if (accOrigen.connect() && accDestino.connect()) {
                //Destino
                MongoDatabase dbDestino = accDestino.getMongoClient().getDatabase(this.datosBD.getNombd());
                MongoCollection<Document> docDestino = dbDestino.getCollection(this.sTablaOrigen + "_f" + this.nFragmento);
                dbDestino.createCollection(this.sTablaOrigen + "_f" + this.nFragmento);

                //Origen
                MongoDatabase dbOrigen = accOrigen.getMongoClient().getDatabase(this.datosBD.getNombd());
                MongoCollection<Document> docOrigen = dbOrigen.getCollection(this.sTablaOrigen);

                //Para la vertical
                List<String> nombresAtt = new ArrayList<>();
                this.atts.forEach(x -> nombresAtt.add(x.getNombre()));
                Bson proyeccion = Projections.fields(Projections.include(nombresAtt));
                MongoCursor<Document> resultado = docOrigen.find().projection(proyeccion).iterator();
                while (resultado.hasNext()) {
                    docDestino.insertOne(resultado.next());
                }

            }

            //Registrar datos en xamana
            System.out.println("Registrar datos en Xamana");
            xamanaAcc = new DataAccessXamana();
            String idFragmento = "";
            String idSitio = "";
            if (xamanaAcc.connect()) {
                //id del sitio
                ArrayList arr2 = null;
                arr2 = xamanaAcc.ejecutarConsulta("select id_sitio from Sitio where direccion='" + accDestino.ip + "'");
                if (Tools.isNullorEmpty(arr2)) {
                    xamanaAcc.ejecutarComando("insert into Sitio (direccion) values('" + accDestino.ip + "')");
                    idSitio = "" + ((ArrayList) xamanaAcc.ejecutarConsulta("Select LAST_INSERT_ID()").get(0)).get(0);
                } else {
                    idSitio = "" + ((ArrayList) arr2.get(0)).get(0);
                }

                if (this.fragmentacionCBIR) {
                    xamanaAcc.ejecutarComando("insert into Fragmento(nombre, id_tabla, id_sitio, noperaciones, ndesempenio, isCBIR) value ('"
                            + this.sTablaOrigen + "_f" + this.nFragmento + "', " + this.datosBD.getTabla().getIdTabla()
                            + ", " + idSitio + ",0,0,1)");
                    idFragmento = "" + ((ArrayList) xamanaAcc.ejecutarConsulta("Select LAST_INSERT_ID()").get(0)).get(0);

                } else {
                    xamanaAcc.ejecutarComando("insert into Fragmento(nombre, id_tabla, id_sitio, noperaciones, ndesempenio, isCBIR) value ('"
                            + this.sTablaOrigen + "_f" + this.nFragmento + "', " + this.datosBD.getTabla().getIdTabla()
                            + ", " + idSitio + "," + this.noperaciones + "," + this.ndesempenio + ",0)");
                    idFragmento = "" + ((ArrayList) xamanaAcc.ejecutarConsulta("Select LAST_INSERT_ID()").get(0)).get(0);
                }
                //Modificar atributos en Xamana
                if (!Tools.isNullorEmpty(idFragmento)) {
                    for (Atributo a : this.getAtts()) {
                        if (!a.isPk()) {
                            String sql = "update Atributo set id_tabla=NULL, id_fragmento=" + idFragmento + " where id_atributo=" + a.getIdAtributo();
                            System.out.println(sql);
                            xamanaAcc.ejecutarComando(sql);
                            System.out.println("Atributo " + a.getNombre() + " actualizado a nuevo fragmento");
                        }
                    }
                    xamanaAcc.ejecutarComando("insert into Atributo (tipo,size,nom,id_fragmento,descriptor,multimedia,pk,id_tabla) values ('"
                            + this.datosBD.getTabla().getAtributoLlave().getTipo() + "'," + this.datosBD.getTabla().getAtributoLlave().getTamanio() + ",'" + this.datosBD.getTabla().getAtributoLlave().getNombre() + "',"
                            + idFragmento + ",0,0,1,null)");
                }
                xamanaAcc.disconnect();

            }

            //Eliminar atributos en tabla original
            //Pendiente: eliminar relaciones de la tabla (foráneas)
            if (accOrigen.connect()) {
                MongoDatabase dbOrigen = accOrigen.getMongoClient().getDatabase(this.datosBD.getNombd());
                MongoCollection<Document> docOrigen = dbOrigen.getCollection(this.sTablaOrigen);
                for (Atributo b : this.getAtts()) {
                    //No eliminar PK
                    if (!b.isPk()) {
                        docOrigen.updateMany(new Document(), unset(b.getNombre()));
                        System.out.println(b.getNombre() + ": Atributo eliminado de la tabla original");
                    }
                }
                accOrigen.disconnect();
            }
            //solicitar actualización de fragmentos en el servidor destino (socket)
            //TEMPORAL: COMENTADO DESTINO PORQUE NO ESTA EN EJECUCIÓN VIGILANTE FRAGMENTADOR EN DESTINO (CONNECTION REFUSED)

            //Socket socket = new Socket(accDestino.ip, 5000);
            Socket socket2 = new Socket(accOrigen.ip, 5000);
            //new DataOutputStream(socket.getOutputStream()).writeBoolean(true);
            new DataOutputStream(socket2.getOutputStream()).writeBoolean(true);
            //socket.close();
            socket2.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    void eliminarOrigenMongo(String idFragmentoOriginal) {
        DataAccessMongoDB accOrigen = (DataAccessMongoDB) this.origen;
        try {
            if (accOrigen.connect()) {
                accOrigen.getMongoClient().getDatabase(this.datosBD.getNombd()).getCollection(sTablaOrigen).drop();
            }
            xamanaAcc = new DataAccessXamana();
            //eliminar el fragmento en xamana
            if (this.xamanaAcc.connect()) {
                xamanaAcc.ejecutarComando("delete from Atributo where id_fragmento=" + idFragmentoOriginal);//solo deberia ser PK
                xamanaAcc.ejecutarComando("delete from Fragmento where id_fragmento=" + idFragmentoOriginal);
                xamanaAcc.disconnect();
            }

            //Recargar fragmentos en origen
            Socket socket2 = new Socket(accOrigen.ip, 5000);
            new DataOutputStream(socket2.getOutputStream()).writeBoolean(true);
            socket2.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
