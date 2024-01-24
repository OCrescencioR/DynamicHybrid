package tesis.msc.dh.Model;

import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import tesis.msc.dh.Data.DataAccessMongoDB;
import tesis.msc.dh.Data.DataAccessMySQL;
import tesis.msc.dh.FHorizontal.DHFragmentation;
import tesis.msc.dh.Fragmentations.HMongo;
import tesis.msc.dh.Fragmentations.Hybrid;
import tesis.msc.dh.Fragmentations.Vertical;

/**
 *
 * @author Felipe Castro Medina y Oscar Crescencio Rico
 */
public class BD implements Serializable {

    private DataAccessMySQL accexternomysql;
    private DataAccessMongoDB accexternomongo;
    private String dirbd;
    private String portbd;
    private String nombd;
    private String usubd;
    private String passbd;
    private String TipoBD = "";
    private Tabla tabla = new Tabla();
    private HashMap<String, String> NombreIp;
    private ArrayList<HashMap<String, Double>> costoAtributoxSitio;
    private ArrayList<String> sitios;
    private ArrayList<TablaCosto> esquemaVertical;
    private Hybrid hybrid;
    private String sentencia = "";
    private Integer progress1;
    private static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    private static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_YELLOW_BACKGROUND = "\u001B[43m";
    private static final String ANSI_RESET = "\u001B[0m";
    private String fragmentar;

    public boolean conexionexterna() {
        boolean res = false;

        if (TipoBD.compareTo("a") == 0) {
            try {
                accexternomysql = new DataAccessMySQL(dirbd + ":" + this.portbd, nombd, usubd, passbd);

                if (accexternomysql.connect()) {
                    res = true;
                    accexternomysql.connect();
                }
            } catch (Exception ex) {
                System.out.println(ANSI_RED + "Mistake in conexionexterna() in MySQL " + ex.getMessage() + ANSI_RESET);
            }
        } /*/
        Se comentaron las líneas 55 a 67 porque es para el gestor de PostgreSQL
        else if (TipoBD.compareTo("b") == 0 || TipoBD.compareTo("c") == 0) {
            try {

                accexternopost = new AccesoDatosPG(dirbd + ":" + this.portbd, nombd, usubd, passbd);
                if (accexternopost.conectar()) {
                    res = true;
                    accexternopost.desconectar();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        /*/ else if (TipoBD.compareTo("d") == 0) {
            try {

                System.out.println(this.dirbd + ":" + this.portbd + nombd + this.tabla.getNombre());

                setAccexternomongo(new DataAccessMongoDB(this.dirbd + ":" + this.portbd, nombd, this.tabla.getNombre(), usubd, passbd));
                if (getAccexternomongo().connect()) {
                    res = true;
                } else {
                    System.out.println("Connection refused");
                }
            } catch (Exception ex) {
                //ex.printStackTrace();
                System.out.println(ANSI_RED + "Mistake in conexionexterna() in MongoDB " + ex.getMessage() + ANSI_RESET);
            }
        }

        return res;

    }

    public String crearEsquemaHybrid(String dirLogs, RandomAccessFile raf, Fragment f, ArrayList<Fragment> allFragments, String rutaContexto) {

        /*usar la fragmentación vertical para obtener su esquema
        usar la fragmentación horizontal para obtener su esquema
        mezclar esquemas
        comparar esquemas
        elegir el mejor
        /*/
        try {
            //vertical
            System.out.println(ANSI_YELLOW_BACKGROUND + " -----------CREATE HYBRID SCHEME-------------" + ANSI_RESET);
            System.out.println(" ");
            crearEsquemaVertical(raf, f, rutaContexto);
            setHybrid(new Hybrid(this, f));
            if (this.getTipoBD().compareTo("d") != 0) {
                //Horizontal
                DHFragmentation DHF = new DHFragmentation(this);
                DHF.implementMethod(dirLogs, this.getNombd(), this.getTabla().getNombre());
                getHybrid().mix(DHF.getSchema(), this.esquemaVertical);
            } else {
                HMongo horizontal = new HMongo(this, raf, f, allFragments);
                horizontal.crearEsquema();
                getHybrid().mixMongo(horizontal.getEsquema(), this.esquemaVertical);

                /*Se colocaron las siguientes líneas (122-130), con el objetivo de visualizar el resultado
                del esquema vertical y horizontal antes de aplicar la fragmentación híbrida dinámica, por
                tal motivo, sí se requiere hacer pruebas para observar los esquemas por separado se reco-
                mienda descomentarlas para su uso
                /*/
                for (int i = 0; i < this.esquemaVertical.size(); i++) {
                    System.out.println("\nVertical scheme \nFragment name: " + this.esquemaVertical.get(i).getNombre() + "\nVertical cost: " + this.esquemaVertical.get(i).getCosto()
                            + "\nSite: " + this.esquemaVertical.get(i).getSitio());
                    for (int a = 0; a < this.esquemaVertical.get(i).getArrAtri().size(); a++) {
                        System.out.println("Attributes: " + this.esquemaVertical.get(i).getArrAtri().get(a).getNombre());

                    }
                }
                horizontal.getEsquema().forEach(x -> System.out.println("\nHorizontal scheme \nFragment name: " + x.getName() + "\nPredicate: " + x.getPredicate() + "\nOverlap: " + x.getPredicadoTraslape() + "\nSite: " + x.getSite() + "\nCost: " + x.getCostH()));
            }

            if (this.getTipoBD().compareTo("a") == 0) {
                //En un futuro se debe de complementar para MySQL, Postgres-xl y PostreSQL
            } else if (this.getTipoBD().compareTo("d") == 0) {
                //COSTO SIN CONTEMPLAR TRASLAPE
                getHybrid().costsMongoDB(raf);
                int opc = (this.getHybrid().getHVCost() > this.getHybrid().getVHCost()) ? 1 : 2;
                this.hybrid.hybridOverlapMongoDB(opc);
                //COSTO CONTEMPLANDO TRASLAPE
                getHybrid().costsMongoDB(raf);
            }
            System.out.println(ANSI_BLUE + "------------------------------------------" + ANSI_RESET);
            System.out.println("Hybrid Scheme");
            System.out.println(ANSI_BLUE + "------------------------------------------" + ANSI_RESET);
            fragmentar = this.hybrid.nombrar();
            System.out.format("%5s %10s\n", "H-V cost: ", getHybrid().getHVCost());
            System.out.format("%5s %10s\n", "V-H cost: ", getHybrid().getVHCost());
            System.out.println(ANSI_BLUE + "------------------------------------------" + ANSI_RESET);
            System.out.println(" ");

            if (getHybrid().getHVCost() > getHybrid().getVHCost()) {
                System.out.println("The vertical-horizontal scheme respecting vertical assignments it is less cost and this will be applied");
                setSentencia("El esquema vertical-horizontal respetando asignaciones verticales es menos costoso y este será aplicado.");
            } else if (getHybrid().getHVCost() < getHybrid().getVHCost()) {
                System.out.println("The horizontal-vertical scheme respecting horizontal assignments it is less cost and this will be applied");
                setSentencia("El esquema horizontal-vertical respetando asignaciones horizontales es menos costoso y este será aplicado.");
            } else {
                System.out.println("Both schemes have equals cost but this will be applied vertical-horizontal scheme");
                setSentencia("Ambos esquemas poseen el mismo costo, se aplicará el esquema vertical-horizontal");
            }
            System.out.println(" ");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return fragmentar;

    }

    public void crearEsquemaVertical(RandomAccessFile raf, Fragment f, String ruta) {
        Vertical atributos = new Vertical(this, raf, f);
        ArrayList<TablaCosto> juntos = new ArrayList();
        try {
            if (this.getTipoBD().compareTo("a") == 0) {
                //Si el gestor es MySQL, en un futuro se debe complementar aquí
            } else if (this.getTipoBD().compareTo("d") == 0) {
                juntos = atributos.costoxSitioMongoV3(juntos, ruta);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        juntos = atributos.nombrar(juntos);
        atributos.setEsquemaVertical(juntos);
        this.setCostoAtributoxSitio(atributos.getCostoAtributoxSitio());
        this.setSitios(atributos.getSitios());
        this.setEsquemaVertical(atributos.getEsquemaVertical());

    }

    public String getDirbd() {
        return dirbd;
    }

    /**
     * @param dirbd the dirbd to set
     */
    public void setDirbd(String dirbd) {
        this.dirbd = dirbd;
    }

    /**
     * @return the portbd
     */
    public String getPortbd() {
        return portbd;
    }

    /**
     * @param portbd the portbd to set
     */
    public void setPortbd(String portbd) {
        this.portbd = portbd;
    }

    /**
     * @return the usubd
     */
    public String getUsubd() {
        return usubd;
    }

    /**
     * @param usubd the usubd to set
     */
    public void setUsubd(String usubd) {
        this.usubd = usubd;
    }

    /**
     * @return the passbd
     */
    public String getPassbd() {
        return passbd;
    }

    /**
     * @param passbd the passbd to set
     */
    public void setPassbd(String passbd) {
        this.passbd = passbd;
    }

    /**
     * @return the TipoBD
     */
    public String getTipoBD() {
        return TipoBD;
    }

    /**
     * @param TipoBD the TipoBD to set
     */
    public void setTipoBD(String TipoBD) {
        this.TipoBD = TipoBD;
    }

    /**
     * @return the tabla
     */
    public Tabla getTabla() {
        return tabla;
    }

    /**
     * @param tabla the tabla to set
     */
    public void setTabla(Tabla tabla) {
        this.tabla = tabla;
    }

    /**
     * @return the nombd
     */
    public String getNombd() {
        return nombd;
    }

    /**
     * @param nombd the nombd to set
     */
    public void setNombd(String nombd) {
        this.nombd = nombd;
    }

    /**
     * @param accexternomongo the accexternomongo to set
     */
    public void setAccexternomongo(DataAccessMongoDB accexternomongo) {
        this.accexternomongo = accexternomongo;
    }

    public DataAccessMongoDB getAccexternomongo() {
        return accexternomongo;
    }

    /**
     * @return the NombreIp
     */
    public HashMap<String, String> getNombreIp() {
        return NombreIp;
    }

    /**
     * @param NombreIp the NombreIp to set
     */
    public void setNombreIp(HashMap<String, String> NombreIp) {
        this.NombreIp = NombreIp;
    }

    /**
     * @return the costoAtributoxSitio
     */
    public ArrayList<HashMap<String, Double>> getCostoAtributoxSitio() {
        return costoAtributoxSitio;
    }

    /**
     * @param costoAtributoxSitio the costoAtributoxSitio to set
     */
    public void setCostoAtributoxSitio(ArrayList<HashMap<String, Double>> costoAtributoxSitio) {
        this.costoAtributoxSitio = costoAtributoxSitio;
    }

    /**
     * @return the sitios
     */
    public ArrayList<String> getSitios() {
        return sitios;
    }

    /**
     * @param sitios the sitios to set
     */
    public void setSitios(ArrayList<String> sitios) {
        this.sitios = sitios;
    }

    /**
     * @return the esquemaVertical
     */
    public ArrayList<TablaCosto> getEsquemaVertical() {
        return esquemaVertical;
    }

    /**
     * @param esquemaVertical the esquemaVertical to set
     */
    public void setEsquemaVertical(ArrayList<TablaCosto> esquemaVertical) {
        this.esquemaVertical = esquemaVertical;
    }

    /**
     * @return the hybrid
     */
    public Hybrid getHybrid() {
        return hybrid;
    }

    /**
     * @param hybrid the hybrid to set
     */
    public void setHybrid(Hybrid hybrid) {
        this.hybrid = hybrid;
    }

    /**
     * @return the sentencia
     */
    public String getSentencia() {
        return sentencia;
    }

    /**
     * @param sentencia the sentencia to set
     */
    public void setSentencia(String sentencia) {
        this.sentencia = sentencia;
    }

    /**
     * @return the progress1
     */
    public Integer getProgress1() {
        return progress1;
    }

    /**
     * @param progress1 the progress1 to set
     */
    public void setProgress1(Integer progress1) {
        this.progress1 = progress1;
    }

}
