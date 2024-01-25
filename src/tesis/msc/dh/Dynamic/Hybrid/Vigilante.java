package tesis.msc.dh.Dynamic.Hybrid;

import com.mongodb.client.MongoCollection;
import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import tesis.msc.dh.Data.DataAccessMongoDB;
import tesis.msc.dh.Data.DataAccessXamana;
import tesis.msc.dh.Model.Atributo;
import tesis.msc.dh.Model.Fragment;
import tesis.msc.dh.Model.Tabla;
import tesis.msc.dh.Model.BD;
import tesis.msc.tools.Tools;

/**
 *
 * @author OCrescencioR
 */
public class Vigilante {

    DataAccessXamana accessXamana;
    private String token;
    private String dirLogs;
    EstadoHilo hilo;
    String miIP; //IP del nodo a donde esté el vigilante
    BD datosBD;
    private static final String ANSI_RED = "\u001B[31m";
    private static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_YELLOW_BACKGROUND = "\u001B[43m";
    private static final String ANSI_RESET = "\u001B[0m";
    ArrayList<Fragment> fragment = new ArrayList<>();
    ArrayList<Fragment> fragment_remotos = new ArrayList<>();
    String archivoActualLog;
    long caracterLog;
    HashMap<String, String> NombreIp = new HashMap<>();
    private double Cost;
    private List<Fragment> esquema;
    File archivos[];
    private String fragmentar;
    private int nLinea = 0;

    public Vigilante() {

        token = Tools.readToken();
        dirLogs = Tools.readLog();
        hilo = new EstadoHilo();
        miIP = Tools.getIP();

        try {
            ArrayList res = null, res2 = null, res3 = null;
            accessXamana = new DataAccessXamana();

            if (accessXamana.connect()) {

                String sql = "select BD.nombre, direccion, puerto, gestor, usr, psw, "
                        + "Tabla.nombre, Tabla.umbral_op, Tabla.umbral_des, Tabla.noperaciones, Tabla.ndesempenio  from BD "
                        + "join Tabla on Tabla.id_BD=BD.id_BD "
                        + "join Fragmento on Fragmento.id_tabla=Tabla.id_tabla "
                        + "where Fragmento.id_tabla=" + token;
                res = accessXamana.ejecutarConsulta(sql);

                sql = "select id_atributo, tipo, size, nom, descriptor, multimedia "
                        + "from Atributo "
                        + "join Fragmento on Fragmento.id_fragmento=Atributo.id_fragmento "
                        + "where Fragmento.id_tabla=" + token + " and Atributo.pk=0";
                res3 = accessXamana.ejecutarConsulta(sql);

                sql = "select id_atributo, tipo, size, nom, descriptor, multimedia "
                        + "from Atributo "
                        + "join Fragmento on Fragmento.id_fragmento=Atributo.id_fragmento "
                        + "where Fragmento.id_tabla=" + token + " and Atributo.pk=1 limit 1";
                res2 = accessXamana.ejecutarConsulta(sql);

                accessXamana.disconnect();
            } else {
                System.out.println(ANSI_RED + "No connection" + ANSI_RESET);
            }

            if (!Tools.isNullorEmpty(res) && !Tools.isNullorEmpty(res2) && !Tools.isNullorEmpty(res3)) {
                System.out.println(" ");
                this.datosBD = new BD();
                this.datosBD.setNombd("" + ((ArrayList) res.get(0)).get(0));
                this.datosBD.setDirbd("" + ((ArrayList) res.get(0)).get(1));
                this.datosBD.setPortbd("" + ((ArrayList) res.get(0)).get(2));
                this.datosBD.setTipoBD("" + ((ArrayList) res.get(0)).get(3));

                /*
                En caso que no se tenga usuario y contraseña de la BD,las siguientes
                líneas, permiten que no se considere como nullo.
                 */
                String varUsubd = ((ArrayList) res.get(0)).get(4).toString();
                String varPassbd = ((ArrayList) res.get(0)).get(5).toString();

                if (Tools.isNullorEmpty(varUsubd)) {
                    this.datosBD.setUsubd("");
                } else {
                    this.datosBD.setUsubd("" + ((ArrayList) res.get(0)).get(4));
                }

                if (Tools.isNullorEmpty(varPassbd)) {
                    this.datosBD.setPassbd("");
                } else {
                    this.datosBD.setPassbd("" + ((ArrayList) res.get(0)).get(5));
                }
                System.out.println(ANSI_BLUE + "------------------------------------------" + ANSI_RESET);
                System.out.println("Data found and information obtained");
                System.out.println(ANSI_BLUE + "------------------------------------------" + ANSI_RESET);
                System.out.format("%5s %16s\n", "Database name", datosBD.getNombd());
                System.out.format("%5s %26s\n", "Database IP", datosBD.getDirbd());

                Tabla tabla = new Tabla();
                tabla.setIdTabla(token);
                tabla.setNombre("" + ((ArrayList) res.get(0)).get(6));
                tabla.setUmbralOP(Double.parseDouble("" + ((ArrayList) res.get(0)).get(7)));
                tabla.setUmbralCO(Double.parseDouble("" + ((ArrayList) res.get(0)).get(8)));
                tabla.setNoperaciones(Double.parseDouble("" + ((ArrayList) res.get(0)).get(9)));
                tabla.setNdesempenio(Double.parseDouble("" + ((ArrayList) res.get(0)).get(10)));

                System.out.format("%5s %27s\n", "Fragmented table", tabla.getNombre());
                System.out.format("%5s %9s\n", "Operation threshold", tabla.getUmbralOP());
                System.out.format("%5s %7s\n", "Performance threshold", tabla.getUmbralCO());
                System.out.format("%5s %11s\n", "Total operation log", tabla.getNoperaciones());
                System.out.println(ANSI_BLUE + "------------------------------------------" + ANSI_RESET);

                //buscar Atributos de tabla y agregarlos a la tabla
                ArrayList<Atributo> attTabla = new ArrayList();
                Atributo pk = new Atributo();
                pk.setIdAtributo("" + ((ArrayList) res2.get(0)).get(0));
                pk.setTipo("" + ((ArrayList) res2.get(0)).get(1));
                pk.setTamanio("" + ((ArrayList) res2.get(0)).get(2));
                pk.setNombre("" + ((ArrayList) res2.get(0)).get(3));
                pk.setDescriptor(false);
                pk.setMultimedia(false);
                pk.setPk(true);

                ArrayList<Atributo> atr = new ArrayList<>();
                boolean bandera = false;
                for (int k = 0; k < res3.size(); k++) {
                    Atributo a = new Atributo();
                    a.setIdAtributo("" + ((ArrayList) res3.get(k)).get(0));
                    a.setTipo("" + ((ArrayList) res3.get(k)).get(1));
                    a.setTamanio("" + ((ArrayList) res3.get(k)).get(2));
                    a.setNombre("" + ((ArrayList) res3.get(k)).get(3));
                    a.setDescriptor((("" + ((ArrayList) res3.get(k)).get(4)).compareTo("0") != 0));
                    a.setMultimedia((("" + ((ArrayList) res3.get(k)).get(5)).compareTo("0") != 0));

                    //Para evitar tener atributos repetidos
                    for (int l = 0; l < atr.size(); l++) {
                        if (a.getNombre().equals(atr.get(l).getNombre())) {
                            bandera = true;
                            l = atr.size();
                        }
                    }
                    if (bandera != true) {
                        attTabla.add(a);
                        atr.add(a);
                    } else {
                        bandera = false;
                    }
                }

                tabla.setAtributoLlave(pk);
                attTabla.add(pk);
                tabla.setAtributos(attTabla);
                tabla.generarTablaFragmento();//crear fragmento de la tabla original, para que sea tratado posteriormente como un fragmento más
                tabla.llenaFragmentosxSitio(miIP);
                this.fragment = tabla.buscaEsquema();
                //this.fragment.forEach(x -> System.out.println(x.getName()));
                this.datosBD.setTabla(tabla);

                if (this.datosBD.getDirbd().compareTo(miIP) == 0) {
                    this.datosBD.getTabla().getFragmentos().add(this.datosBD.getTabla().getFragmentTable());
                    tabla.llenaFragmentosxSitioRemoto(miIP);
                    this.fragment_remotos = tabla.buscaEsquema();
                }

                //System.out.println(this.datosBD.getTabla().getAtributoLlave().getNombre() + ":" + this.datosBD.getTabla().getAtributoLlave().getTipo() + ":" + this.datosBD.getTabla().getAtributoLlave().getTamanio());
                /*Análisis de logs para detectar nuevos atributos (alter table add column), comparar atributos de base de xamana
                para detectar el atributo nuevo, preguntar si es descriptor o multimedia, agregarlo a la base de xamana
                si es descriptor o multimedia, hasta que esté el duo de atributos separar en un nuevo fragmento.
                Si no es ninguno de esos dos, entonces dejarlo en la tabla original.
                Analizar si algun fragmento superó umbrales (incluida la tabla inicial)) mediante el analisis del log
                /*/
                if (this.datosBD.getTipoBD().compareTo("d") == 0) {
                    //Abro socket
                    hilo.start();
                    while (true) {
                        /*
                        La bandera de refragmentación indica cuando los fragmentos a analizar en los logs ya no son los mismos y es necesario
                        llenar nuevamente con los nuevos fragmentos que se analizaran.
                         */
                        if (hilo.banderaRefragmentacion) {
                            this.datosBD.getTabla().llenaFragmentosxSitio(miIP);
                            this.datosBD.getTabla().llenaFragmentosxSitioRemoto(miIP);
                            if (this.datosBD.getDirbd().compareTo(miIP) == 0) {
                                this.datosBD.getTabla().getFragmentos().add(this.datosBD.getTabla().getFragmentTable());
                            }
                            hilo.banderaRefragmentacion = false;
                        }
                        //No se puede revisar atributos  nuevos, ya que es NoSQL
                        this.eliminar_Archivos(dirLogs);
                        logMongoDB();
                        Thread.sleep(5000);
                    }
                }
            } else {
                System.out.println(ANSI_RED + "No database or table for the given token" + ANSI_RESET);
            }

        } catch (Exception e) {
            e.printStackTrace();
            //System.out.println(ANSI_RED + "Mistake in Vigilante class: " + e.getMessage() + ANSI_RESET);
        }

    }

    private void logMongoDB() {
        try {

            File carpeta = new File(dirLogs);
            archivos = carpeta.listFiles();
            File ultimoModificado = null;

            for (int i = 0; i < archivos.length; i++) {
                if (archivos[i].isFile()) {
                    if (ultimoModificado == null) {
                        ultimoModificado = archivos[i];
                    }
                    if (archivos[i].lastModified() > ultimoModificado.lastModified()) {
                        ultimoModificado = archivos[i];
                    }
                }
            }
            if (ultimoModificado != null) {
                if (archivoActualLog != null) {
                    //se debe de revisar el último modificado
                    if (ultimoModificado.length() < this.caracterLog) {
                        this.caracterLog = 0;
                    }
                    if (ultimoModificado.toString().compareTo(archivoActualLog) != 0) {
                        archivoActualLog = ultimoModificado.toString();
                    }
                    RandomAccessFile raf = new RandomAccessFile(ultimoModificado, "r");
                    raf.seek(this.caracterLog);
                    if (this.datosBD.getTipoBD().compareTo("a") == 0) {
                        //en un futuro se debe de complementar con mysql  
                    } else {
                        nLinea = (int) this.caracterLog;
                        //System.out.println("NLINEA " + nLinea);
                        nLinea = this.nOperaciones(raf, nLinea);
                        ArrayList<String> remover = new ArrayList<String>();
                        for (Fragment f : this.datosBD.getTabla().getFragmentos()) {
                            for (Fragment frag : this.fragment) {
                                if (f.getName().equals(frag.getName())) {
                                    frag.setId_operation(f.getId_operation());
                                    double op = this.datosBD.getTabla().getNoperaciones() * this.datosBD.getTabla().getUmbralOP() * .01;
                                    System.out.println("--------------------------------------------------------------------");
                                    System.out.println("Current operation information for hybrid fragmentation (first analysis):");
                                    System.out.println("--------------------------------------------------------------------");
                                    System.out.println(ANSI_BLUE + "Fragment name" + ANSI_RESET + ":\t\t" + f.getName());
                                    System.out.println(ANSI_BLUE + "Achieve Operation" + ANSI_RESET + ":\t\t" + op);
                                    int tama = 0;
                                    tama = f.getId_operation().size();
                                    System.out.println(ANSI_BLUE + "Number of Operations" + ANSI_RESET + ":\t\t" + tama);
                                    System.out.println("--------------------------------------------------------------------");
                                    if (f.getId_operation().size() < op) {
                                        remover.add(frag.getName());
                                    }
                                    break;
                                }
                            }
                        }
                        for (int i = 0; i < remover.size(); i++) {
                            System.out.println(" ");
                            System.out.println("Remove fragment: " + remover.get(i));
                            for (Fragment f : this.datosBD.getTabla().getFragmentos()) {
                                if (f.getName().equals(remover.get(i))) {
                                    this.datosBD.getTabla().getFragmentos().remove(f);
                                    break;
                                }
                            }
                            for (Fragment frag : this.fragment) {
                                if (frag.getName().equals(remover.get(i))) {
                                    this.fragment.remove(frag);
                                    break;
                                }
                            }
                        }

                        costslogMongoDBextencionHibrida(raf, nLinea);
                    }
                } else {
                    nLinea = 0;
                    for (File archivo : archivos) {
                        RandomAccessFile raf = new RandomAccessFile(archivo, "r");
                        if (this.datosBD.getTipoBD().compareTo("a") == 0) {
                            //en un futuro se debe de complementar con mysql  
                        } else {
                            nLinea = this.nOperaciones(raf, nLinea);
                        }
                    }
                    ArrayList<String> remover = new ArrayList<String>();
                    for (Fragment f : this.datosBD.getTabla().getFragmentos()) {
                        for (Fragment frag : this.fragment) {
                            if (f.getName().equals(frag.getName())) {
                                frag.setId_operation(f.getId_operation());
                                double op = this.datosBD.getTabla().getNoperaciones() * this.datosBD.getTabla().getUmbralOP() * .01;
                                System.out.println("--------------------------------------------------------------------");
                                System.out.println("Current operation information for hybrid fragmentation (first analysis):");
                                System.out.println("--------------------------------------------------------------------");
                                System.out.println(ANSI_BLUE + "Fragment name" + ANSI_RESET + ":\t\t" + f.getName());
                                System.out.println(ANSI_BLUE + "Achieve Operation" + ANSI_RESET + ":\t\t" + op);
                                int tama = 0;
                                tama = f.getId_operation().size();
                                System.out.println(ANSI_BLUE + "Number of Operations" + ANSI_RESET + ":\t\t" + tama);
                                System.out.println("--------------------------------------------------------------------");
                                if (f.getId_operation().size() < op) {
                                    remover.add(frag.getName());
                                }
                                break;
                            }
                        }
                    }

                    for (int i = 0; i < remover.size(); i++) {
                        System.out.println(" ");
                        System.out.println("Remove fragment: " + remover.get(i));
                        for (Fragment f : this.datosBD.getTabla().getFragmentos()) {
                            if (f.getName().equals(remover.get(i))) {
                                this.datosBD.getTabla().getFragmentos().remove(f);
                                break;
                            }
                        }
                        for (Fragment frag : this.fragment) {
                            if (frag.getName().equals(remover.get(i))) {
                                this.fragment.remove(frag);
                                break;
                            }
                        }
                    }

                    nLinea = 0;
                    for (File archivo : archivos) {
                        RandomAccessFile raf = new RandomAccessFile(archivo, "r");
                        if (this.datosBD.getTipoBD().compareTo("a") == 0) {
                            //en un futuro se debe de complementar con mysql  
                        } else {
                            nLinea = costslogMongoDBextencionHibrida(raf, nLinea);
                        }
                    }
                    archivoActualLog = ultimoModificado.toString();
                }
                //revisar si algún fragmento superó umbrales
                double costoXFragmento = 0.0;
                for (Fragment f : this.fragment) {
                    if (f.getFragmentIP().equals(miIP)) {
                        costoXFragmento = f.getCost();
                        System.out.println(" ");
                        System.out.println("--------------------------------------------------------------------");
                        System.out.println("Current cost information for hybrid fragmentation (second analysis):");
                        System.out.println("--------------------------------------------------------------------");
                        System.out.println(ANSI_BLUE + "Cost in " + f.getName() + ANSI_RESET + ":\t" + costoXFragmento);
                        System.out.println(ANSI_BLUE + "Performance threshold" + ANSI_RESET + ":\t\t" + this.datosBD.getTabla().getUmbralCO());
                        System.out.println(ANSI_BLUE + "Initial performance" + ANSI_RESET + ":\t\t" + f.getPerformance());
                        System.out.println(ANSI_BLUE + "Achieve performance" + ANSI_RESET + ":\t\t" + f.getPerformance() * (this.datosBD.getTabla().getUmbralCO() * .01));
                        System.out.println("--------------------------------------------------------------------");

                        if (costoXFragmento >= (f.getPerformance() * (this.datosBD.getTabla().getUmbralCO() * .01))) {
                            //Alcanzó Umbral
                            if (!f.isCbir()) {
                                System.out.println("This fragment is not CBIR");
                                System.out.println(" ");
                                //Nuevo:
                                RandomAccessFile ran = new RandomAccessFile(dirLogs + "/mix.log", "rw");
                                int nLineas = 0;
                                for (File archivo : archivos) {
                                    if (!archivo.getName().equals("mix.log") && !archivo.getName().equals("vertical.log")) {
                                        RandomAccessFile ra = new RandomAccessFile(archivo, "r");
                                        while (ra.getFilePointer() < ra.length()) {
                                            nLineas++;
                                            if (f.getId_operation().contains(nLineas)) {
                                                String escribirMix = ra.readLine();
                                                ran.writeBytes(escribirMix + "\n");
                                            } else {
                                                String escribirMix = ra.readLine();

                                            }
                                        }
                                    }
                                }
                                fragmentar = this.datosBD.crearEsquemaHybrid(this.dirLogs, ran, f, this.todos_Fragmentos(), this.dirLogs);
                                System.out.println(ANSI_BLUE + "------------------------------------------" + ANSI_RESET);
                                System.out.println("Apply new Scheme Hybrid");
                                System.out.println(ANSI_BLUE + "------------------------------------------" + ANSI_RESET);
                                this.datosBD.getHybrid().hybridFragmentation(fragmentar);
                                ran.close();
                                this.eliminar_Archivos(dirLogs);
                                System.out.println(ANSI_BLUE + "------------------------------------------" + ANSI_RESET);
                            } else {
                                System.out.println("Do not fragment because is CBIR fragment");
                            }
                        }//cierre del if del costoxFragmento
                    }//cierre del if que compara la ip del fragmento con la ip del sitio actual
                }//cierre del for 
                this.eliminar_Predicado_XAMANA(token);
            } else {
                System.out.println("There are not files in the folder");
            }
        } catch (Exception e) {
            e.printStackTrace();
            //System.out.println(ANSI_RED + "Mistake in logMongoDB() method  within Vigilante classs: " + e.getMessage() + ANSI_RESET);
        }
    }

    public int costslogMongoDBextencionHibrida(RandomAccessFile raf, int lNumeroLineas) {
        try {
            System.out.println(" ");
            while (raf.getFilePointer() < raf.length()) {
                String actual = raf.readLine();
                for (Fragment f : this.datosBD.getTabla().getFragmentos()) {
                    f.setNperformanceLineLog(0.0);
                    f.setNoperationsLineLog(0.0);
                    f.setCostH(0.0);
                    f.setCostV(0.0);

                }
                System.out.println(ANSI_BLUE + "----------------HYBRID COST----------------" + ANSI_RESET);
                String ipLinea = "";
                lNumeroLineas++;
                boolean band = false;
                for (Fragment fr : this.datosBD.getTabla().getFragmentos()) {
                    if (fr.getId_operation().contains(lNumeroLineas)) {
                        band = true;
                        break;
                    }
                }

                if (actual.toLowerCase().contains("command") && contieneNombreEsquema(actual.toLowerCase()) && band == true) {
                    JSONObject j = new JSONObject(actual);
                    String c = j.get("c").toString().toLowerCase().trim();
                    JSONObject j2 = new JSONObject(j.get("attr").toString());
                    JSONObject j3 = new JSONObject(j2.get("command").toString());
                    String ns = "";

                    int ValorAsignadoOperacion = Tools.VAO(j2.get("command").toString());
                    if (ValorAsignadoOperacion == 2) {
                        if (j3.has("delete")) {
                            ns = j3.get("delete").toString().toLowerCase();
                        } else if (j3.has("insert")) {
                            ns = j3.get("insert").toString().toLowerCase();
                        }
                    } else {
                        ns = (ValorAsignadoOperacion == 1) ? j3.get("find").toString().toLowerCase() : j3.get("update").toString().toLowerCase();
                    }

                    if (ValorAsignadoOperacion != 0 && c.compareTo("command") == 0 && contieneNombreEsquema(ns.toLowerCase())) {
                        j2 = new JSONObject(j.get("attr").toString());
                        String remote = j2.get("remote").toString().toLowerCase().trim();
                        ipLinea = remote.substring(0, remote.indexOf(":"));
                        System.out.println("Number of operation: " + ANSI_BLUE + lNumeroLineas + ANSI_RESET);
                        System.out.println(actual);
                        NombreIp.put(ipLinea, ipLinea);
                        //System.out.println("SITE: " + ipLinea);

                        /*calcular el número de sitios que necesita visitar para llevar
                        a cabo la operación (será el número de reuniones necesarias)
                        primero sitiov y luego sitioh
                        /*/
                        int nJoins = 0;
                        ArrayList<String> atributosOpe = buscarAtributos(actual, this.datosBD.getTabla().getAtributos());
                        //this.datosBD.getTabla().getFragmentos().forEach(x-> System.out.println(x.getName()));
                        ArrayList<Fragment> fResuelvenAtributos = new ArrayList<>();

                        for (Fragment fragmento : this.datosBD.getTabla().getFragmentos()) {
                            boolean seUsa = false;
                            for (Atributo l : fragmento.getAttributes()) {
                                if (atributosOpe.contains(l.getNombre().toLowerCase())) {
                                    seUsa = true;
                                }
                            }
                            if (seUsa) {
                                fResuelvenAtributos.add(fragmento);
                            }
                        }
                        if (this.datosBD.getTabla().getFragmentosRemotos() != null) {
                            //fResuelvenAtributos.forEach(x -> System.out.println("fResuelvenAtributos " + x.getName()));
                            for (Fragment fragmento : this.datosBD.getTabla().getFragmentosRemotos()) {
                                boolean seUsa = false;
                                for (Atributo l : fragmento.getAttributes()) {
                                    if (atributosOpe.contains(l.getNombre().toLowerCase())) {
                                        seUsa = true;
                                    }
                                }
                                if (seUsa) {
                                    fResuelvenAtributos.add(fragmento);
                                }
                            }

                        }
                        //fResuelvenAtributos.forEach(x -> System.out.println("fResuelvenAtributos_02" + " " + x.getName()));
                        String where_statement = Tools.where_statement(actual);
                        double countp = 0;
                        double tuplasDelWhere = 0;
                        ArrayList<Fragment> fResuelvenPredicados = new ArrayList<>();
                        for (Fragment fragmento : fResuelvenAtributos) {
                            countp = this.nTuplas02(fragmento.getName(), where_statement.toLowerCase(), fragmento.getSite());
                            tuplasDelWhere = tuplasDelWhere + countp;
                            //Filtrar por predicado
                            if (countp > 0) {
                                fResuelvenPredicados.add(fragmento);
                            }
                        }
                        //System.out.println("Number of tuples by predicate:" + " " + tuplasDelWhere);
                        this.insertar_Predicado(where_statement, tuplasDelWhere);

                        nJoins = (fResuelvenPredicados.size() < 1) ? 1 : fResuelvenPredicados.size();
                        /*
                         buscar en esquema si la operación se resuelve en parte o entera de manera remota 
                         primero sitiov y luego sitioh
                         */
                        boolean siHay = false;

                        for (Fragment fragmento : fResuelvenPredicados) {
                            if (fragmento.getSite().compareTo(ipLinea) != 0) {
                                siHay = true;
                            }
                        }

                        if (siHay) {
                            ValorAsignadoOperacion = ValorAsignadoOperacion * 2;
                        }
                        System.out.println("VAO: " + ValorAsignadoOperacion); //valor remoto
                        System.out.println("CR: " + nJoins);
                        double costoOperacionVertical = 0.0, costoOperacionHorizontal = 0.0, costoOperacionHibrida = 0.0;
                        for (Atributo a : this.datosBD.getTabla().getAtributos()) {
                            if (actual.toLowerCase().contains(a.getNombre().toLowerCase())) {
                                //System.out.println("\t Size of Attribute  " + a.getNombre() + ": " + Double.parseDouble(a.getTamanio()));
                                costoOperacionVertical += ValorAsignadoOperacion * nJoins * Double.parseDouble(a.getTamanio());
                            }
                        }
                        costoOperacionHorizontal = tuplasDelWhere * ValorAsignadoOperacion;
                        costoOperacionHibrida = (costoOperacionVertical + costoOperacionHorizontal) / 2;
                        System.out.println(" ");
                        System.out.println("Cost of vertical operation: " + costoOperacionVertical);
                        System.out.println("Cost of horizontal operation: " + costoOperacionHorizontal);
                        System.out.println("Cost of hybrid operation: " + costoOperacionHibrida);
                        this.setCost(this.getCost() + costoOperacionHibrida);
                        //Agregar el costo a los fragmentos que resuelven la operación
                        for (int f = 0; f < this.fragment.size(); f++) {
                            boolean b = false;
                            for (int r = 0; r < fResuelvenPredicados.size(); r++) {
                                if (fResuelvenPredicados.get(r).getName().compareTo(fragment.get(f).getName()) == 0) {
                                    b = true;
                                }
                            }
                            if (b) {
                                System.out.println("The cost of operation is allocated to the fragment: " + ANSI_BLUE + fragment.get(f).getName() + ANSI_RESET);
                                fragment.get(f).setCost(fragment.get(f).getCost() + costoOperacionHibrida);

                            }
                        }
                    }
                    System.out.println(" ");
                }
            }
            System.out.println(ANSI_BLUE + "----------------The file has finished analyze ----------------" + ANSI_RESET);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println(ANSI_RED + "Mistake in costslogMongoDBextencionHibrida(RandomAccessFile raf) method within Vigilante class: " + ex.getMessage() + ANSI_RESET);
        }
        return lNumeroLineas;
    }

    public int nOperaciones(RandomAccessFile raf, int lNumeroLineas) {
        try {
            ArrayList<Integer> id_operacion = new ArrayList<Integer>();
            System.out.println(" ");
            System.out.println(ANSI_BLUE + "---------------Operation threshold----------------" + ANSI_RESET);
            while (raf.getFilePointer() < raf.length()) {
                String actual = raf.readLine();
                lNumeroLineas++;
                System.out.println("Line: " + lNumeroLineas);
                for (Fragment f : this.datosBD.getTabla().getFragmentos()) {
                    if (actual.toLowerCase().contains("command") && actual.toLowerCase().contains(f.getName())) {
                        //System.out.println(actual);
                        id_operacion.add(lNumeroLineas);
                        f.setId_operation(id_operacion);
                        break;
                    }
                }
            }//cierre de while
        } catch (Exception ex) {
            //ex.printStackTrace();
            System.out.println(ANSI_RED + "Mistake in int nOperaciones(RandomAccessFile raf, int lNumeroLineas) method within Vigilante class: " + ex.getMessage() + ANSI_RESET);
        }
        return lNumeroLineas;
    }

    /*
    Una vez aplicada la fragmentación estática,para el caso de la fragmentación dinámica,
    se accece a la base de datos (Xamana) y se obtiene por cada fragmento su nombre.
    Por consiguiente, contieneNombreEsquema, es utilizado en las extension híbrida.
    /*/
    private boolean contieneNombreEsquema(String x) {
        boolean bandera = false;
        for (Fragment f : this.fragment) {
            if (x.contains(f.getName().toLowerCase())) {
                bandera = true;
            }
        }
        if (!bandera) {
            if (x.contains(this.datosBD.getTabla().getNombre().toLowerCase())) {
                bandera = true;
            }
        }
        return bandera;
    }

    private ArrayList<String> buscarAtributos(String actual, ArrayList<Atributo> atts) {
        JSONObject j;
        ArrayList<String> ret = new ArrayList<>();
        //QUE ATRIBUTOS SON LOS DEL ESQUEMA?
        try {
            j = new JSONObject(actual);
            String c = j.get("c").toString().toLowerCase().trim();
            JSONObject j2 = new JSONObject(j.get("attr").toString());
            JSONObject j3 = new JSONObject(j2.get("command").toString());
            if (j3.has("find")) {
                if (j3.has("projection")) {
                    JSONObject j4 = new JSONObject(j3.get("projection").toString());
                    for (Atributo a : atts) {
                        if (j4.has(a.getNombre())) {
                            ret.add(a.getNombre().toLowerCase());
                        }
                    }
                } else {
                    for (Atributo a : atts) {
                        ret.add(a.getNombre().toLowerCase());
                    }
                }
            } else if (j3.has("delete")) {
                for (Atributo a : atts) {
                    ret.add(a.getNombre().toLowerCase());
                }
            } else if (j3.has("insert")) {
                //insert many
                if (j3.has("documents")) {
                    JSONArray arr = j3.getJSONArray("documents");
                    for (int i = 0; i < arr.length(); i++) {
                        for (Atributo a : atts) {
                            JSONObject j4 = new JSONObject(arr.get(i).toString());
                            if (j4.has(a.getNombre())) {
                                if (!ret.contains(a.getNombre())) {
                                    ret.add(a.getNombre());
                                }
                            }
                        }

                    }
                }
            } else if (j3.has("update")) {
                JSONArray arr = j3.getJSONArray("updates");
                JSONObject j4 = new JSONObject(arr.get(0).toString());
                JSONObject j5 = new JSONObject(j4.get("u").toString());
                JSONObject j6 = new JSONObject(j5.get("$set").toString());
                for (Atributo a : atts) {
                    if (j6.has(a.getNombre())) {
                        ret.add(a.getNombre().toLowerCase());
                    }
                }
            }
        } catch (Exception ex) {
            //ex.printStackTrace();
            System.out.println(ANSI_RED + "Mistake in buscarAtributos(String actual, ArrayList<Atributo> atts) method within Vigilante class: " + ex.getMessage());
        }
        return ret;
    }

    private double nTuplas02(String nombre, String predicado, String direccion) {
        System.out.println("Count of tuples: " + predicado);
        double count1 = 0.0;
        try {
            //mongo
            DataAccessMongoDB accOrigen = new DataAccessMongoDB(direccion + ":" + this.datosBD.getPortbd(), this.datosBD.getNombd(), this.datosBD.getTabla().getNombre(), this.datosBD.getUsubd(), this.datosBD.getPassbd());
            if (accOrigen.connect()) {
                MongoCollection<Document> doc = accOrigen.getMongoClient().getDatabase(this.datosBD.getNombd()).getCollection(nombre);
                Document d = Document.parse((predicado.isEmpty()) ? "{}" : predicado);
                count1 = doc.countDocuments(d);
                accOrigen.getMongoClient().close();
            }
        } catch (Exception ex) {
            //ex.printStackTrace();
            System.out.println(ANSI_RED + "Mistake in nTuplas02(String nombre, String predicado, String direccion) method within Vigilante class: " + ex.getMessage() + ANSI_RESET);
        }
        System.out.println("Number of tuples:" + " " + count1);
        System.out.println("");
        return count1;
    }

    private ArrayList<Fragment> todos_Fragmentos() {

        ArrayList<Fragment> all = new ArrayList<>();

        for (Fragment f : this.datosBD.getTabla().getFragmentos()) {
            all.add(f);
        }

        if (this.datosBD.getTabla().getFragmentosRemotos() != null) {
            for (Fragment f : this.datosBD.getTabla().getFragmentosRemotos()) {
                all.add(f);
            }
        }
        return all;
    }

    public void insertar_Predicado(String where_statement, double tuplasDelwhere) {

        String sql, id_predicado = "", id_sitio = "";
        try {
            accessXamana = new DataAccessXamana();
            if (accessXamana.connect()) {
                sql = "Insert Into Predicado(descripcion,nTuplas) " + "values ('" + where_statement + "'," + tuplasDelwhere + ")";
                accessXamana.ejecutarComando(sql);
                //Insertar en la relación fragmento_predicado:
                id_predicado = "" + ((ArrayList) accessXamana.ejecutarConsulta("Select LAST_INSERT_ID()").get(0)).get(0);
                id_sitio = "" + ((ArrayList) accessXamana.ejecutarConsulta("SELECT id_sitio FROM SITIO WHERE direccion = '" + miIP + "'").get(0)).get(0);
                accessXamana.ejecutarComando("Insert into fragmento_predicado(id_tabla, id_predicado, id_sitio) values ('"
                        + token + "'," + id_predicado + "," + id_sitio + ")");
            }

        } catch (Exception ex) {
            //ex.printStackTrace();
            System.out.println(ANSI_RED + "Mistake in insertar_Predicado(String where_statement, double tuplasDelwhere) method within Vigilante class: " + ex.getMessage());
        }

    }

    public void eliminar_Archivos(String filePath) {
        File fileMix = new File(filePath, "mix.log");
        File fileVertical = new File(filePath, "vertical.log");
        fileMix.delete();
        fileVertical.delete();
    }

    public void eliminar_Predicado_XAMANA(String token) {
        try {
            accessXamana = new DataAccessXamana();
            if (accessXamana.connect()) {
                String id_sitio = "" + ((ArrayList) accessXamana.ejecutarConsulta("SELECT id_sitio FROM SITIO WHERE direccion = '" + miIP + "'").get(0)).get(0);
                accessXamana.ejecutarComando("Delete predicado FROM predicado "
                        + "JOIN fragmento_predicado"
                        + " ON predicado.id_predicado = fragmento_predicado.id_predicado"
                        + " WHERE  id_tabla=" + token + " and id_sitio=" + id_sitio);
            }

        } catch (Exception ex) {

            System.out.println(ANSI_RED + "Mistake in eliminar_Predicado_XAMANA (String id_sitio) method within Vigilante class: " + ex.getMessage());
        }

    }

    /**
     * @return the accesXamana
     */
    public DataAccessXamana getAccesXamana() {
        return accessXamana;
    }

    /**
     * @param accesXamana the accesXamana to set
     */
    public void setAccesXamana(DataAccessXamana accesXamana) {
        this.accessXamana = accesXamana;
    }

    /**
     * @return the dirLogs
     */
    public String getDirLogs() {
        return dirLogs;
    }

    /**
     * @param dirLogs the dirLogs to set
     */
    public void setDirLogs(String dirLogs) {
        this.dirLogs = dirLogs;
    }

    /**
     * @return the esquema
     */
    public List<Fragment> getEsquema() {
        return esquema;
    }

    /**
     * @param esquema the esquema to set
     */
    public void setEsquema(List<Fragment> esquema) {
        this.esquema = esquema;
    }

    public double getCost() {
        return Cost;
    }

    public void setCost(double Cost) {
        this.Cost = Cost;
    }
}
