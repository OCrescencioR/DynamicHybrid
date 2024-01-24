package tesis.msc.dh.FHorizontal;

import java.util.List;
import tesis.msc.dh.Model.Fragment;
import tesis.msc.dh.Model.MCRUD;

/**
 *
 * @author abryn
 */
public interface BuilderFraments {

    List<Fragment> buildDesignSchema(List<MCRUD> mcrud, MCRUD item) throws Exception;

    void buildSchema(List<Fragment> schema, Register register) throws Exception;

}
