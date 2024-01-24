package tesis.msc.dh.Log;

import java.io.Serializable;

/**
 *
 * @author OCrescencioR
 */
public class LogReaderCreator implements Serializable {

    public LogReader createLogReader(String optionManager, String addrees) {
        LogReader logReader = null;

        switch (optionManager) {
            case "a": //MySQL
                //logReader = new LogReaderMySQL(addrees);
                break;
            case "b": //Postgres-XL
                break;
            case "c": //PostgresSQL
                //logReader = new LogReaderPostgreSQL(addrees);
                break;
            case "d": //MongoDB
                logReader = new LogReaderMongoDB(addrees);
                break;
        }

        return logReader;
    }

}
