package tesis.msc.dh.Data;
import java.util.ArrayList;

/**
 * Clase abstracta que tiene los métodos para connect/disconnect para establecer
 * el vínculo de conexión hacia la base de datos
 *
 * @author Felipe Castro Medina
 */
public abstract class DataAccess {

    public abstract boolean connect() throws Exception;

    public abstract void disconnect() throws Exception;

    public abstract ArrayList<String> attributes(String databasename, String name);

}
