package tesis.msc.dh.Fragmentations;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.json.JSONObject;
import tesis.msc.dh.Model.Atributo;
import tesis.msc.dh.Model.Fragment;
import tesis.msc.dh.Model.TablaCosto;
import tesis.msc.dh.Model.BD;

/**
 *
 * @authors Aldo, Felipe y OCrescencioR
 */
public class Vertical {

    BD datosBD;
    RandomAccessFile raf;
    private ArrayList<HashMap<String, Double>> costoAtributoxSitio;
    private ArrayList<String> sitios;
    private ArrayList<TablaCosto> esquemaVertical;
    private HashMap<ArrayList<Atributo>, HashMap<String, Double>> atributosSitioCostos = new HashMap<>();//afinidad
    Fragment f;
    private static final String ANSI_RED = "\u001B[31m";
    private static final String ANSI_BLUE = "\u001B[34m";
    private static final String ANSI_RESET = "\u001B[0m";

    public Vertical(BD datosBD, RandomAccessFile raf, Fragment f) {
        this.datosBD = datosBD;
        this.raf = raf;
        this.f = f;
    }

    public ArrayList<TablaCosto> costoxSitioMongoV3(ArrayList<TablaCosto> preEsquema, String rutaLogXAMANA) {
        ArrayList<TablaCosto> ret = new ArrayList<>();
        int contador = 0;
        double costoTotal = 0.0;
        ArrayList<String> sitios = new ArrayList<>();
        System.out.println(ANSI_BLUE + "-----------VERTICAL-------------" + ANSI_RESET);
        try {
            raf.seek(0);
            System.out.println("\tWriting in file txt.........");
            System.out.println(" ");
            RandomAccessFile ran = new RandomAccessFile(rutaLogXAMANA + "/vertical.log", "rw");
            while (this.raf.getFilePointer() < this.raf.length()) {
                String actual = this.raf.readLine();
                contador++;
                String ipLinea = "";
                ran.writeBytes("Number of operation: " + contador + "\n");
                System.out.println("Number of operation: " + ANSI_BLUE + contador + ANSI_RESET);
                ran.writeBytes(actual + "\n");
                System.out.println(actual);

                if (actual.toLowerCase().contains("command") && actual.toLowerCase().contains(f.getName().toLowerCase())) {
                    JSONObject j = new JSONObject(actual);
                    String c = j.get("c").toString().toLowerCase().trim();
                    JSONObject j2 = new JSONObject(j.get("attr").toString());
                    JSONObject j3 = new JSONObject(j2.get("command").toString());
                    String ns = "";
                    int ValorAsignadoOperacion = 0;
                    if (j3.has("find")) {
                        ValorAsignadoOperacion = 1;
                        ns = j3.get("find").toString().toLowerCase();
                    } else if (j3.has("delete")) {
                        ValorAsignadoOperacion = 2;
                        ns = j3.get("delete").toString().toLowerCase();
                    } else if (j3.has("insert")) {
                        ValorAsignadoOperacion = 2;
                        ns = j3.get("insert").toString().toLowerCase();
                    } else if (j3.has("update")) {
                        ValorAsignadoOperacion = 3;
                        ns = j3.get("update").toString().toLowerCase();
                    }
                    if (ValorAsignadoOperacion != 0 && c.compareTo("command") == 0 && ns.compareTo(f.getName().toLowerCase()) == 0) {
                        j2 = new JSONObject(j.get("attr").toString());
                        String remote = j2.get("remote").toString().toLowerCase().trim();
                        ipLinea = remote.substring(0, remote.indexOf(":"));

                        ran.writeBytes("SITE: " + ipLinea + "\n");
                        int nJoins = 1; //centralizada
                        ran.writeBytes("CR: " + nJoins + "\n");
                        System.out.println("CR: " + nJoins);
                        if (this.datosBD.getDirbd().compareTo(ipLinea) != 0) {
                            ValorAsignadoOperacion = ValorAsignadoOperacion * 2;
                        }
                        ran.writeBytes("VAO: " + ValorAsignadoOperacion + "\n");
                        System.out.println("VAO: " + ValorAsignadoOperacion);
                        System.out.println(" ");

                        ArrayList<Atributo> desordenados = new ArrayList<>();
                        for (int o = 0; o < this.f.getAttributes().size(); o++) {
                            if (actual.toLowerCase().contains(this.f.getAttributes().get(o).getNombre().toLowerCase())) {
                                desordenados.add(this.f.getAttributes().get(o));
                                ran.writeBytes("Size of attribute: " + this.f.getAttributes().get(o).getNombre() + ": " + Double.parseDouble(this.datosBD.getTabla().getAtributos().get(o).getTamanio()) + "\n");
                                double costoOperacion = 0.0;
                                if (this.f.getAttributes().get(o).getCostoAtributoxSitio().containsKey(ipLinea)) {
                                    costoOperacion = this.f.getAttributes().get(o).getCostoAtributoxSitio().get(ipLinea);
                                }
                                double valorActual = ValorAsignadoOperacion * nJoins * Double.parseDouble(this.f.getAttributes().get(o).getTamanio());
                                ran.writeBytes("Current value: " + valorActual + "\n");
                                costoTotal += valorActual;
                                costoOperacion += valorActual;
                                ran.writeBytes("Operation cost: " + costoOperacion + "\n");
                                this.f.getAttributes().get(o).getCostoAtributoxSitio().put(ipLinea, costoOperacion);
                            }//cierre del if
                            else {
                                ran.writeBytes("\tThis operation do not contain: " + this.f.getAttributes().get(o).getNombre() + "\n");
                            }
                        } //cierre del for o
                        //no calcular costo, sino hasta que ya se haya creado getCostoAtributoxSitio por completo
                        this.initMatrizAfinidad(desordenados);
                        if (!sitios.contains(ipLinea)) {
                            sitios.add(ipLinea);
                        }
                    } else {
                        System.out.println("Missing 1");
                    }
                }//cierre del if actual
                else {
                    System.out.println("Missing 2");
                }
            }//cierre del while

            /*AFINIDAD
            .5.- Crear atributosSitioCostos sumando solo los conjuntos 
            de atributos por sitio que tengan valor mayor a 0 en cada uno de sus atributos en cada sitio,
            colocar en 0 si uno de los atributos del conjunto no se ocupa en el sitio en turno
             */
            Iterator<ArrayList<Atributo>> llaves = this.atributosSitioCostos.keySet().iterator();
            while (llaves.hasNext()) {
                ArrayList<Atributo> llave = llaves.next();
                for (String s : sitios) {
                    double sumaSitio = 0.0;
                    //averiguar si comparten sitio
                    boolean comparten = true;
                    for (Atributo a : llave) {
                        if (!a.isPk()) {
                            if (a.getCostoAtributoxSitio().containsKey(s)) {
                                if (a.getCostoAtributoxSitio().get(s) == 0) {
                                    comparten = false;
                                }
                            } else {
                                comparten = false;
                            }
                        }
                    }
                    //si sí comparten se suma, de otro modo 0
                    if (comparten) {
                        for (Atributo a : llave) {
                            if (a.getCostoAtributoxSitio().get(s) != null) {
                                sumaSitio += a.getCostoAtributoxSitio().get(s);
                            }
                        }
                        this.atributosSitioCostos.get(llave).put(s, sumaSitio);
                    } else {
                        this.atributosSitioCostos.get(llave).put(s, 0.0);
                    }

                }

            }
            System.out.println(" ");
            System.out.println("1.- Delete multimedia sets");
            /*1.- ELIMINAR CONJUNTOS MULTIMEDIA, TODO AQUELLO QUE TENGA ATRIBUTOS MULTIMEDIA
            1.1.- Eliminar el conjunto con la llave primaria sola, si es que es existe
            LOS COSTOS RELACIONADOS A LOS CONJUNTOS ELIMINADOS NO SON RELEVANTES, YA QUE NINGUN
            COSTO DE LA MATRIZ DE AFINIDAD ES EL COSTO FINAL DEL FRAGMENTO, LOS COSTOS REALES
            SE TOMAN DE getCostoAtributoxSitio DE CADA ATRIBUTO INCLUIDO EN EL CONJUNTO*/
            ArrayList<ArrayList<Atributo>> aEliminar = new ArrayList<>();
            for (ArrayList<Atributo> arr : this.atributosSitioCostos.keySet()) {
                boolean hayMultimedia = false;
                boolean esSoloPK = false;
                for (int i = 0; i < arr.size() && !hayMultimedia; i++) {
                    if (arr.get(i).isMultimedia() || arr.get(i).isDescriptor()) {
                        hayMultimedia = true;
                    }
                    if (arr.get(i).isPk()) {
                        esSoloPK = true;
                    }
                }
                if (hayMultimedia || (esSoloPK && arr.size() == 1)) {
                    aEliminar.add(arr);
                }
            }
            for (ArrayList<Atributo> arr : aEliminar) {
                this.atributosSitioCostos.remove(arr);
            }

            System.out.println("2.-Sum set cost in each site");
            //2.- SUMAR COSTOS DE CONJUNTOS EN CADA SITIO (UN COSTO POR CONJUNTO)
            HashMap<ArrayList<Atributo>, Double> atributosSuma = new HashMap<>();
            for (ArrayList<Atributo> arr : this.atributosSitioCostos.keySet()) {
                Double sum = 0.0;
                for (String ips : this.atributosSitioCostos.get(arr).keySet()) {
                    sum += this.atributosSitioCostos.get(arr).get(ips);
                }
                //System.out.println(ANSI_BLUE + "Attribute: " + ANSI_RESET + arr.get(0).getNombre() + ANSI_BLUE + "\nSum result: " + ANSI_RESET + sum);
                atributosSuma.put(arr, sum);
            }

            System.out.println("3.-Order sets by cost");
            //3.- ORDENAR CONJUNTOS POR COSTO
            ArrayList atributosSumaOrden = (ArrayList) atributosSuma.entrySet().stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).collect(Collectors.toList());
            HashMap<ArrayList<Atributo>, Double> filtrados = new HashMap<>();
            System.out.println("4.-Select and delete subsequent sets containing attributes already present");
            /*4.- SELECCIONAR Y ELIMINAR CONJUNTOS SUBSECUENTES QUE CONTENGAN ATRIBUTOS YA PRESENTES EN LAS SELECCIONES ANTERIORES
            ATENDER LLAVE PRIMARIA (no tomarla en cuenta, sino, siempre habría un solo conjunto, porque todos los conjuntos
            tienen llave primaria)*/
            for (Object o : atributosSumaOrden) {
                Entry e = (Entry) o;

                ArrayList<Atributo> llave = (ArrayList<Atributo>) e.getKey();
                Double valor = (Double) e.getValue();
                boolean yaLoContiene = false;
                for (int j = 0; j < llave.size() && !yaLoContiene; j++) {
                    if (!llave.get(j).isPk()) {
                        for (ArrayList<Atributo> i : filtrados.keySet()) {
                            for (int u = 0; u < i.size() && !yaLoContiene; u++) {
                                if (llave.get(j).getNombre().compareTo(i.get(u).getNombre()) == 0) {
                                    yaLoContiene = true;
                                }
                            }
                        }
                    }
                }
                if (!yaLoContiene) {
                    filtrados.put(llave, valor);
                }
            }

            System.out.println("5.-Add missing attributes (attributes without sets or cost) to the original site");
            /*5.- AGREGAR ATRIBUTOS FALTANTES (ATRIBUTOS SIN CONJUNTOS NI COSTO) A SITIO ORIGINAL
            no agregar atts multimedia /*/
            ArrayList<Atributo> faltantes = new ArrayList<>();
            for (Atributo t : this.f.getAttributes()) {
                ran.writeBytes("Attribute: " + t.getNombre() + "\n");
                boolean yaLoContiene = false;
                for (ArrayList<Atributo> l : filtrados.keySet()) {
                    for (Atributo i : l) {
                        if (i.getNombre().compareTo(t.getNombre()) == 0) {
                            yaLoContiene = true;
                        }
                    }
                }
                if (!yaLoContiene && !t.isMultimedia() && !t.isDescriptor() && !t.isPk()) {
                    faltantes.add(t);
                }
            }
            if (!faltantes.isEmpty()) {
                faltantes.add(this.datosBD.getTabla().getAtributoLlave());
                filtrados.put(faltantes, 0.0);
                faltantes.forEach(x -> System.out.println("Missing: " + x.getNombre()));
            }

            System.out.println("Creating table cost of attributes by sites");
            //Agregar a preesquema los nuevos fragmentos
            for (ArrayList<Atributo> listaAtt : filtrados.keySet()) {
                TablaCosto tc = new TablaCosto();
                tc.setArrAtri(listaAtt);
                tc.setCosto(filtrados.get(listaAtt));
                tc.setMultimedia(false);
                preEsquema.add(tc);
            }

            //SITIOS Y COSTOS MULTIMEDIA
            for (TablaCosto tc : preEsquema) {
                double costoFragmento = 0.0;
                for (int o = 0; o < tc.getArrAtri().size(); o++) {
                    //buscar en OBD estos atributos y sumar su costo en cada sitio
                    for (Atributo att : this.datosBD.getTabla().getAtributos()) {
                        if (att.getNombre().compareTo(tc.getArrAtri().get(o).getNombre()) == 0) {
                            costoFragmento += att.getCostoAtributoxSitio().values().stream().mapToDouble(Double::doubleValue).sum();
                        }
                    }
                }
                tc.setCosto(costoFragmento);
                System.out.println("Calculating scheme cost each fragment by site");
            }

            //1.- sumar por sitio el costo de cada atributo en el conjunto sacado de obd
            HashMap<ArrayList<Atributo>, HashMap<String, Double>> sumaXSitio = new HashMap<>();
            for (TablaCosto tc : preEsquema) {
                sumaXSitio.put(tc.getArrAtri(), new HashMap<>());
                for (int o = 0; o < tc.getArrAtri().size(); o++) {
                    for (Atributo att : this.datosBD.getTabla().getAtributos()) {
                        if (att.getNombre().compareTo(tc.getArrAtri().get(o).getNombre()) == 0) {
                            for (String sitio : att.getCostoAtributoxSitio().keySet()) {
                                if (sumaXSitio.get(tc.getArrAtri()).containsKey(sitio)) {
                                    sumaXSitio.get(tc.getArrAtri()).put(sitio, sumaXSitio.get(tc.getArrAtri()).get(sitio) + att.getCostoAtributoxSitio().get(sitio));
                                } else {
                                    sumaXSitio.get(tc.getArrAtri()).put(sitio, att.getCostoAtributoxSitio().get(sitio));
                                }
                            }
                        }
                    }
                }
            }
            //2.- hacer un maxentry del nuevo hashmap y ese será el sitio a donde asignar
            for (TablaCosto tc : preEsquema) {
                Entry<String, Double> entry = this.getMaxEntry(sumaXSitio.get(tc.getArrAtri()));
                if (entry != null) {
                    tc.setSitio(entry.getKey());
                } else {
                    tc.setSitio(this.datosBD.getDirbd());
                }
            }

            //3.- Agrupar fragmentos no multimedia que esten presentes en el mismo sitio
            ret.addAll(preEsquema);
            ran.writeBytes("\n ::::::::::Cumulative cost: " + costoTotal + "\n");
            System.out.println("");
            System.out.println("::::::::::Cumulative cost: " + costoTotal + "\n");
            ran.close();
            System.out.println(ANSI_BLUE + "------------------------------------------" + ANSI_RESET);
            System.out.println(" ");

        } catch (Exception ex) {
            //ex.printStackTrace();
            System.out.println(ANSI_RED + "Mistake in costoxSitioMongoV3: " + ex.getMessage() + ANSI_RESET);
        }
        return ret;
    }

    public void matrizAfinidad(ArrayList<Atributo> attOpe, String ip, double costoOperacion) {
        //Agregar llave primaria a attOpe si no la tiene
        boolean llavePrimaria = false;
        for (Atributo it : attOpe) {
            if (it.isPk()) {
                llavePrimaria = true;
            }
        }
        if (!llavePrimaria) {
            this.datosBD.getTabla().getAtributoLlave().setPk(true);
            attOpe.add(this.datosBD.getTabla().getAtributoLlave());
        }

        boolean itemEncontrado = false;
        Iterator<ArrayList<Atributo>> it = atributosSitioCostos.keySet().iterator();
        int control = 0;
        while (it.hasNext() && !itemEncontrado) {
            ArrayList<Atributo> clave = it.next();
            //buscar en listaAtributos este conjunto (attOpe)
            //buscar si hay algun atributo en a1 que no este en attOpe
            //que compartan la longitud
            //
            if (clave.size() == attOpe.size()) {
                boolean aunpuedenestarTodos = true;
                for (int j = 0; j < clave.size() && aunpuedenestarTodos; j++) {
                    boolean todosenAttOpe = false;
                    for (int i = 0; i < attOpe.size() && !todosenAttOpe; i++) {
                        if (clave.get(j).getNombre().compareTo(attOpe.get(i).getNombre()) == 0) {
                            todosenAttOpe = true;
                        }
                    }
                    if (!todosenAttOpe) {
                        System.out.println("No hay coincidencia en la linea " + control);
                        aunpuedenestarTodos = false;
                    }
                }
                if (aunpuedenestarTodos) {
                    System.out.println("Coincidencia en la linea " + control);
                    if (atributosSitioCostos.get(clave).containsKey(ip)) {
                        atributosSitioCostos.get(clave).put(ip, atributosSitioCostos.get(clave).get(ip) + costoOperacion);
                    } else {
                        atributosSitioCostos.get(clave).put(ip, costoOperacion);
                    }
                    itemEncontrado = true;
                }
            } else {
                System.out.println("No hay coincidencia en la linea " + control);
            }
            control++;
        }
        if (!itemEncontrado) {
            //agregar esta combinación nueva
            HashMap<String, Double> temp = new HashMap<>();
            temp.put(ip, costoOperacion);
            atributosSitioCostos.put(attOpe, temp);
        }

    }

    //PIPE
    public Entry<String, Double> getMaxEntry(Map<String, Double> map) {
        Entry<String, Double> maxEntry = null;
        for (Entry<String, Double> entry : map.entrySet()) {
            if (maxEntry == null) {
                maxEntry = entry;
            } else {
                Double value = entry.getValue();
                if (null != value && value > maxEntry.getValue()) {
                    maxEntry = entry;
                }
            }
        }

        return maxEntry;
    }

    //PIPE
    public ArrayList<TablaCosto> nombrar(ArrayList<TablaCosto> ori) {
        for (int i = 0; i < ori.size(); i++) {
            ori.get(i).setNombre(this.datosBD.getTabla().getNombre() + "_" + (i + 1));
        }
        return ori;
    }

    public void initMatrizAfinidad(ArrayList<Atributo> attOpe) {
        //Agregar llave primaria a attOpe si no la tiene
        boolean llavePrimaria = false;
        for (Atributo it : attOpe) {
            if (it.isPk()) {
                llavePrimaria = true;
            }
        }
        if (!llavePrimaria) {
            this.datosBD.getTabla().getAtributoLlave().setPk(true);
            attOpe.add(this.datosBD.getTabla().getAtributoLlave());
        }

        boolean itemEncontrado = false;
        Iterator<ArrayList<Atributo>> it = atributosSitioCostos.keySet().iterator();
        int control = 0;
        while (it.hasNext() && !itemEncontrado) {
            ArrayList<Atributo> clave = it.next();
            //buscar en listaAtributos este conjunto (attOpe)
            //buscar si hay algun atributo en a1 que no este en attOpe
            //que compartan la longitud
            //
            if (clave.size() == attOpe.size()) {
                boolean aunpuedenestarTodos = true;
                for (int j = 0; j < clave.size() && aunpuedenestarTodos; j++) {
                    boolean todosenAttOpe = false;
                    for (int i = 0; i < attOpe.size() && !todosenAttOpe; i++) {
                        if (clave.get(j).getNombre().compareTo(attOpe.get(i).getNombre()) == 0) {
                            todosenAttOpe = true;
                        }
                    }
                    if (!todosenAttOpe) {
                        //System.out.println("No hay coincidencia en la linea " + control);
                        aunpuedenestarTodos = false;
                    }
                }
                if (aunpuedenestarTodos) {
                    //System.out.println("Coincidencia en la linea " + control);
                    /*if (atributosSitioCostos.get(clave).containsKey(ip)) {
                        atributosSitioCostos.get(clave).put(ip, atributosSitioCostos.get(clave).get(ip) + costoOperacion);
                    } else {
                        atributosSitioCostos.get(clave).put(ip, costoOperacion);
                    }*/
                    itemEncontrado = true;
                }
            } else {
                //System.out.println("No hay coincidencia en la linea " + control);
            }
            control++;
        }
        if (!itemEncontrado) {
            //agregar esta combinación nueva
            atributosSitioCostos.put(attOpe, new HashMap<>());
        }

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

}//Fín de Vertical

