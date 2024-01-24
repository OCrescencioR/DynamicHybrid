package tesis.msc.dh.Data;

import java.io.Serializable;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author OCrescencioR
 */
public class DataAccessMySQL extends DataAccess implements Serializable {

    public static String sUrl = null;
    public String ip = null;
    public static String sUsr = null;
    public static String sPwd = null;
    private java.sql.Connection oConexion;
    

    public DataAccessMySQL(String url, String nombd, String usr, String pwd) throws Exception {
        ip = url.split(":")[0];
        sUrl = "jdbc:mysql://" + url + "/" + nombd;//"jdbc:mysql://localhost/Master";
        sUsr = usr;//"root";
        sPwd = pwd;//"sunny_4teamo";
    }

    @Override
    public boolean connect() throws Exception {
        boolean bRet = false;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            //System.out.println(sUrl +" " +sUsr+" "+ sPwd);
            oConexion = DriverManager.getConnection(sUrl, sUsr, sPwd);
            bRet = true;
        } catch (SQLException e) {
            throw e;

        }
        return bRet;
    }

    @Override
    public void disconnect() throws Exception {
        oConexion.close();
    }

    @Override
    public ArrayList<String> attributes(String databasename, String name) {
        ArrayList<String> ret = new ArrayList<String>();
        try {
            DatabaseMetaData metadata = oConexion.getMetaData();
            ResultSet atributos = metadata.getColumns(databasename, null, name, null);
            while (atributos.next()) {
                ret.add(atributos.getString("COLUMN_NAME") + ":" + atributos.getString("TYPE_NAME") + ":" + atributos.getString("COLUMN_SIZE"));
            }
        } catch (SQLException ex) {
            Logger.getLogger(DataAccessXamana.class.getName()).log(Level.SEVERE, null, ex);
        }
        return ret;
    }

}
