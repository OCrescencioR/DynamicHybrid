package tesis.msc.dh.Scheme;

import java.util.List;
import tesis.msc.dh.FHorizontal.Register;
import tesis.msc.dh.Model.Fragment;
import tesis.msc.dh.Model.MCRUD;

/**
 *
 * @author Abraham Castillo
 */
public interface BuilderFraments {

    List<Fragment> buildDesignSchema(List<MCRUD> mcrud, MCRUD item) throws Exception;

    void buildSchema(List<Fragment> schema, Register register) throws Exception;

}
