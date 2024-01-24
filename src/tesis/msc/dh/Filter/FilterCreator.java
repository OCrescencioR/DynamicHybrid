package tesis.msc.dh.Filter;

import java.io.Serializable;

/**
 *
 * @author Abraham Castillo
 */
public class FilterCreator implements Serializable {

    public Filter createFilter(String optionManager, String site) {
        Filter filter = null;

        switch (optionManager) {
            case "a": //MySQL
                //filter = new FilterMySQL(site);
                break;
            case "b": //Postgres-XL
                break;
            case "c": //PostgresSQL
                //filter = new FilterPostgreSQL(site);
                break;
            case "d": //MongoDB
                filter = new FilterMongoDB(site);
                break;
        }

        return filter;

    }

}
