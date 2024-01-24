package tesis.msc.dh.Scheme;

import tesis.msc.dh.Model.BD;

/**
 *
 * @author Abraham Castillo
 */
public class BuilderFragmentsCreator {

    public BuilderFraments createBuilderFragments(BD bd) {
        BuilderFraments BF = null;

        switch (bd.getTipoBD()) {
            case "a": //MySQL
                //BF = new BuilderFragmentsMySQL(BD);
                break;
            case "b": //Postgres-XL
                break;
            case "c": //PostgresSQL
                //BF = new BuilderFragmentsPostgreSQL(BD);
                break;
            case "d": //MongoDB
                BF = new BuilderFragmentsMongoDB(bd);
                break;
        }

        return BF;
    }

}
