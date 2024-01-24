package tesis.msc.dh.Model;

import java.util.ArrayList;
import tesis.msc.dh.Data.DataAccessXamana;
import tesis.msc.tools.Tools;

/**
 *
 * @author Felipe Castro Medina
 */
public class Tabla {

    private String tipoFragment;
    private boolean CBIR = false;
    private String nombre;
    private Double umbralOP;
    private Double umbralCO;
    private double ndesempenio;
    private double noperaciones;
    private String atributoMultimedia;
    private Atributo atributoLlave;
    private ArrayList<Fragment> fragmentos = null;
    private ArrayList<Fragment> fragmentos_remotos = null;
    private ArrayList<Atributo> Atributos;
    private String idTabla; //mi tabla que se llama tabla, no la remota
    private Fragment fragmentTable;
    DataAccessXamana accessXamana;
    private static final String ANSI_RED = "\u001B[31m";
    private static final String ANSI_BLUE = "\u001B[34m";
    private static final String ANSI_RESET = "\u001B[0m";

    public void generarTablaFragmento() {
        fragmentTable = new Fragment();
        fragmentTable.setName(nombre);
        fragmentTable.setAttributes(new ArrayList<Atributo>());
        fragmentTable.setCbir(false);
        fragmentTable.setPerformance(getNdesempenio());
        fragmentTable.setFragmenTable(true);

    }

    /**
     * @return the nombre
     */
    public String getNombre() {
        return nombre;
    }

    /**
     * @param nombre the nombre to set
     */
    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    /**
     * @return the tipoFragment
     */
    public String getTipoFragment() {
        return tipoFragment;
    }

    /**
     * @param tipoFragment the tipoFragment to set
     */
    public void setTipoFragment(String tipoFragment) {
        this.tipoFragment = tipoFragment;
    }

    /**
     * @return the CBIR
     */
    public boolean isCBIR() {
        return CBIR;
    }

    /**
     * @param CBIR the CBIR to set
     */
    public void setCBIR(boolean CBIR) {
        this.CBIR = CBIR;
    }

    /**
     * @return the umbralOP
     */
    public Double getUmbralOP() {
        return umbralOP;
    }

    /**
     * @param umbralOP the umbralOP to set
     */
    public void setUmbralOP(Double umbralOP) {
        this.umbralOP = umbralOP;
    }

    /**
     * @return the umbralCO
     */
    public Double getUmbralCO() {
        return umbralCO;
    }

    /**
     * @param umbralCO the umbralCO to set
     */
    public void setUmbralCO(Double umbralCO) {
        this.umbralCO = umbralCO;
    }

    /**
     * @return the ndesempenio
     */
    public double getNdesempenio() {
        return ndesempenio;
    }

    /**
     * @param ndesempenio the ndesempenio to set
     */
    public void setNdesempenio(double ndesempenio) {
        this.ndesempenio = ndesempenio;
    }

    /**
     * @return the atributoMultimedia
     */
    public String getAtributoMultimedia() {
        return atributoMultimedia;
    }

    /**
     * @param atributoMultimedia the atributoMultimedia to set
     */
    public void setAtributoMultimedia(String atributoMultimedia) {
        this.atributoMultimedia = atributoMultimedia;
    }

    /**
     * @return the Atributos
     */
    public ArrayList<Atributo> getAtributos() {
        return Atributos;
    }

    /**
     * @param Atributos the Atributos to set
     */
    public void setAtributos(ArrayList<Atributo> Atributos) {
        this.Atributos = Atributos;
    }

    /**
     * @return the atributoLlave
     */
    public Atributo getAtributoLlave() {
        return atributoLlave;
    }

    /**
     * @param atributoLlave the atributoLlave to set
     */
    public void setAtributoLlave(Atributo atributoLlave) {
        this.atributoLlave = atributoLlave;
    }

    /**
     * @return the idTabla
     */
    public String getIdTabla() {
        return idTabla;
    }

    /**
     * @param idTabla the idTabla to set
     */
    public void setIdTabla(String idTabla) {
        this.idTabla = idTabla;
    }

    /**
     * @return the fragmentos
     */
    public ArrayList<Fragment> getFragmentos() {
        return fragmentos;
    }

    /**
     * @param fragmentos the fragmentos to set
     */
    public void setFragmentos(ArrayList<Fragment> fragmentos) {
        this.fragmentos = fragmentos;
    }

    public Fragment getFragmentTable() {
        return fragmentTable;
    }

    public void setFragmentTable(Fragment fragmentTable) {
        this.fragmentTable = fragmentTable;
    }

    public ArrayList<Fragment> getFragmentosRemotos() {
        return fragmentos_remotos;
    }

    /**
     * @param fragmentos_remotos the fragmentos to set
     */
    public void setFragmentosRemotos(ArrayList<Fragment> fragmentos_remotos) {
        this.fragmentos_remotos = fragmentos_remotos;
    }

    public double getNoperaciones() {
        return noperaciones;
    }

    public void setNoperaciones(double noperaciones) {
        this.noperaciones = noperaciones;
    }

    public void llenaFragmentosxSitio(String ip) {
        try {
            accessXamana = new DataAccessXamana();
            ArrayList res = null, res2 = null;
            //ArrayList 
            if (accessXamana.connect()) {
                //¿Que fragmentos estan en el sitio a donde el vigilante esta?
                res = accessXamana.ejecutarConsulta("select fragmento.id_fragmento, nombre, noperaciones, ndesempenio, "
                        + "isCBIR, direccion from Fragmento join Sitio on Sitio.id_sitio=Fragmento.id_sitio "
                        + "where id_tabla= '" + this.idTabla + "'" + " and direccion='" + ip + "'");

                if (!Tools.isNullorEmpty(res)) {
                    System.out.println("\n" + ANSI_BLUE + "Fragments sites finds" + ANSI_RESET);
                    setFragmentos(new ArrayList<>());
                    for (Object tupla : res) {
                        Fragment f = new Fragment();
                        f.setId_fragment("" + ((ArrayList) tupla).get(0));
                        f.setName("" + ((ArrayList) tupla).get(1));
                        f.setNumberofOperations(Double.parseDouble("" + ((ArrayList) tupla).get(2)));
                        f.setPerformance(Double.parseDouble("" + ((ArrayList) tupla).get(3)));
                        f.setCbir(Boolean.parseBoolean("" + ((ArrayList) tupla).get(4)));
                        f.setSite("" + ((ArrayList) tupla).get(5));

                        //buscar atributos de fragmento y agregarlos al fragmento
                        res2 = accessXamana.ejecutarConsulta("select id_atributo, tipo, size,nom, descriptor, multimedia, pk "
                                + "from Atributo "
                                + "where id_fragmento=" + f.getId_fragment() + "");
                        ArrayList<Atributo> attsFragmento = new ArrayList<>();
                        if (!Tools.isNullorEmpty(res2)) {
                            for (int k = 0; k < res2.size(); k++) {
                                Atributo a = new Atributo();
                                a.setIdAtributo("" + ((ArrayList) res2.get(k)).get(0));
                                a.setTipo("" + ((ArrayList) res2.get(k)).get(1));
                                a.setTamanio("" + ((ArrayList) res2.get(k)).get(2));
                                a.setNombre("" + ((ArrayList) res2.get(k)).get(3));
                                a.setDescriptor(("" + ((ArrayList) res2.get(k)).get(4)).compareTo("0") != 0);
                                a.setMultimedia(("" + ((ArrayList) res2.get(k)).get(5)).compareTo("0") != 0);
                                a.setPk(("" + ((ArrayList) res2.get(k)).get(6)).compareTo("0") != 0);
                                attsFragmento.add(a);
                            }
                        }
                        f.setAttributes(attsFragmento);
                        getFragmentos().add(f);
                    }
                    getFragmentos().forEach(lr -> System.out.println("Fragment name: " + lr.getName()));
                } else {
                    //No hay fragmentos
                    setFragmentos(new ArrayList<>());//SE BORRAN ANTERIORES
                }
                accessXamana.disconnect();
            } else {
                System.out.println(ANSI_RED + "No connection in llenarFragmentosxSitio()" + ANSI_RESET);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void llenaFragmentosxSitioRemoto(String ip) {
        try {
            accessXamana = new DataAccessXamana();
            ArrayList res = null, res2 = null;
            //ArrayList 
            if (accessXamana.connect()) {
                //¿Que fragmentos estan en el sitio remoto?
                res = accessXamana.ejecutarConsulta("select fragmento.id_fragmento, nombre, noperaciones, ndesempenio, "
                        + "isCBIR, direccion from Fragmento join Sitio on Sitio.id_sitio=Fragmento.id_sitio "
                        + "where id_tabla= '" + this.idTabla + "'" + " and direccion !='" + ip + "'");
                if (!Tools.isNullorEmpty(res)) {
                    System.out.println(ANSI_BLUE + "Fragments remote sites finds" + ANSI_RESET);
                    setFragmentosRemotos(new ArrayList<>());
                    for (Object tupla : res) {
                        Fragment f = new Fragment();
                        f.setId_fragment("" + ((ArrayList) tupla).get(0));
                        f.setName("" + ((ArrayList) tupla).get(1));
                        f.setNumberofOperations(Double.parseDouble("" + ((ArrayList) tupla).get(2)));
                        f.setPerformance(Double.parseDouble("" + ((ArrayList) tupla).get(3)));
                        f.setCbir(Boolean.parseBoolean("" + ((ArrayList) tupla).get(4)));
                        f.setSite("" + ((ArrayList) tupla).get(5));
                        //buscar atributos de fragmento y agregarlos al fragmento
                        res2 = accessXamana.ejecutarConsulta("select id_atributo, tipo, size,nom, descriptor, multimedia, pk "
                                + "from Atributo "
                                + "where id_fragmento=" + f.getId_fragment() + "");
                        ArrayList<Atributo> attsFragmento = new ArrayList<>();
                        if (!Tools.isNullorEmpty(res2)) {
                            for (int k = 0; k < res2.size(); k++) {
                                Atributo a = new Atributo();
                                a.setIdAtributo("" + ((ArrayList) res2.get(k)).get(0));
                                a.setTipo("" + ((ArrayList) res2.get(k)).get(1));
                                a.setTamanio("" + ((ArrayList) res2.get(k)).get(2));
                                a.setNombre("" + ((ArrayList) res2.get(k)).get(3));
                                a.setDescriptor(("" + ((ArrayList) res2.get(k)).get(4)).compareTo("0") != 0);
                                a.setMultimedia(("" + ((ArrayList) res2.get(k)).get(5)).compareTo("0") != 0);
                                a.setPk(("" + ((ArrayList) res2.get(k)).get(6)).compareTo("0") != 0);
                                attsFragmento.add(a);
                            }
                        }
                        f.setAttributes(attsFragmento);
                        getFragmentosRemotos().add(f);
                    }
                    getFragmentosRemotos().forEach(rf -> System.out.println("Fragment name: " + rf.getName()));
                } else {
                    //No hay fragmentos
                    setFragmentosRemotos(new ArrayList<>());//SE BORRAN ANTERIORES
                }
                accessXamana.disconnect();
            } else {
                System.out.println(ANSI_RED + "No connection in llenarFragmentosxSitioRemotos()" + ANSI_RESET);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public ArrayList<Fragment> buscaEsquema() {
        ArrayList<Fragment> ret = new ArrayList<>();
        try {
            accessXamana = new DataAccessXamana();
            ArrayList res = null;
            ArrayList res3 = null;
            if (accessXamana.connect()) {
                //¿Que fragmentos estan en el sitio a donde el vigilante esta?
                res = accessXamana.ejecutarConsulta("select id_fragmento, nombre, noperaciones, ndesempenio, isCBIR, Sitio.direccion "
                        + "from Fragmento "
                        + "join Sitio on Sitio.id_sitio=Fragmento.id_sitio "
                        + "where id_tabla=" + this.idTabla);
                if (!Tools.isNullorEmpty(res)) {
                    System.out.println(ANSI_BLUE + "Fragments scheme found" + ANSI_RESET);
                    ret = new ArrayList<>();
                    for (Object tupla : res) {
                        Fragment f = new Fragment();
                        f.setId_fragment("" + ((ArrayList) tupla).get(0));
                        f.setName("" + ((ArrayList) tupla).get(1));
                        f.setNumberofOperations(Double.parseDouble("" + ((ArrayList) tupla).get(2)));
                        f.setPerformance(Double.parseDouble("" + ((ArrayList) tupla).get(3)));
                        f.setCbir(Boolean.parseBoolean("" + ((ArrayList) tupla).get(4)));
                        f.setFragmentIP("" + ((ArrayList) tupla).get(5));
                        //buscar atributos de fragmento y agregarlos al fragmento
                        res3 = accessXamana.ejecutarConsulta("select id_atributo, tipo, size,nom, descriptor, multimedia, pk "
                                + "from Atributo "
                                + "where id_fragmento=" + f.getId_fragment() + "");
                        ArrayList<Atributo> attsFragmento = new ArrayList<>();
                        if (!Tools.isNullorEmpty(res3)) {
                            for (int k = 0; k < res3.size(); k++) {
                                Atributo a = new Atributo();
                                a.setIdAtributo("" + ((ArrayList) res3.get(k)).get(0));
                                a.setTipo("" + ((ArrayList) res3.get(k)).get(1));
                                a.setTamanio("" + ((ArrayList) res3.get(k)).get(2));
                                a.setNombre("" + ((ArrayList) res3.get(k)).get(3));
                                a.setDescriptor(("" + ((ArrayList) res3.get(k)).get(4)).compareTo("0") != 0);
                                a.setMultimedia(("" + ((ArrayList) res3.get(k)).get(5)).compareTo("0") != 0);
                                a.setPk(("" + ((ArrayList) res3.get(k)).get(6)).compareTo("0") != 0);
                                attsFragmento.add(a);
                            }
                        }
                        f.setAttributes(attsFragmento);
                        ret.add(f);
                    }
                } else {
                    //No hay fragmentos
                    ret = new ArrayList<>();//SE BORRAN ANTERIORES
                }
                accessXamana.disconnect();
            } else {
                System.out.println(ANSI_RED + "No connection in buscaEsquema()" + ANSI_RESET);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return ret;

    }

    public int buscaIndiceAttxNombre(String nombre) {
        int r = -1;
        for (int i = 0; i < this.getAtributos().size(); i++) {
            if (this.getAtributos().get(i).getNombre().compareTo(nombre) == 0) {
                r = i;
            }
        }
        return r;
    }

}
