package tesis.msc.dh.Log;

import java.io.File;
import java.io.Serializable;
import java.util.List;

/**
 * Clase que define el comportamiento para obtener las operaciones de un archivo
 * log
 *
 * @author Abraham Castillo
 */
public abstract class LogReader implements Serializable {

    protected String addrees;
    File archivos[];

    public LogReader(String address) {
        this.addrees = address;
    }

    //Otra Alternativa de solución, basada en la clase de LoadReader, método readSingleLogFile() del proyecto FragmentationWatch
    public abstract List<String> determineOperations02(String path, String database, String table) throws Exception;

    public String getAddrees() {
        return addrees;
    }
}
