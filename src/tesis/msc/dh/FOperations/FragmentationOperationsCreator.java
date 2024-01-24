package tesis.msc.dh.FOperations;

import tesis.msc.dh.Model.BD;

/**
 *
 * @author Abraham Castillo
 */
public class FragmentationOperationsCreator {

    public static FragmentationOperations createFragmentationOperations(BD bd) {
        FragmentationOperations FO = null;
        switch (bd.getTipoBD()) {
            case "a": //MySQL
                //FO = new FragmentationOperationsMySQL(BD);
                break;
            case "b": //Postgres-XL
                break;
            case "c": //PostgresSQL
                //FO = new FragmentationOperationsPostgreSQL(BD);
                break;
            case "d": //MongoDB
                FO = new FragmentationOperationsMongo(bd);
                break;
        }
        return FO;

    }

}
