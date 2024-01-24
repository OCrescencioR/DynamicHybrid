package tesis.msc.dh.Fragmentations;

import com.mongodb.client.MongoCollection;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.bson.Document;
import org.json.JSONObject;
import tesis.msc.dh.Data.DataAccessMongoDB;
import tesis.msc.dh.Data.DataAccessXamana;
import tesis.msc.dh.Model.CostoSitio;
import tesis.msc.dh.Model.Atributo;
import tesis.msc.dh.Model.Fragment;
import tesis.msc.dh.Model.BD;
import tesis.msc.tools.Tools;

/**
 *
 * @author Felipe Castro Medina y adaptado para la fragmentación híbrida
 * dinámica por OCrescencioR
 */
public class HMongo {

    private List<Fragment> esquema;
    private List<ArrayList<String>> MCRUD;
    private List<ArrayList<String>> ALP;
    RandomAccessFile raf;
    BD datosBD;
    Fragment f;
    DataAccessXamana accessXamana;
    private ArrayList<Fragment> todos_Fragmentos;
    private static final String ANSI_BLUE = "\u001B[34m";
    private static final String ANSI_RED = "\u001B[31m";
    private static final String ANSI_RESET = "\u001B[0m";

    public HMongo(BD aThis, RandomAccessFile raf, Fragment f, ArrayList<Fragment> todos_Fragmentos) {
        this.datosBD = aThis;
        this.raf = raf;
        esquema = new ArrayList<>();
        this.f = f;
        this.todos_Fragmentos = todos_Fragmentos;

    }

    public void crearEsquema() {
        //MCRUD
        HashMap<String, CostoSitio> predicadoCosto = new HashMap<>();
        this.MCRUD = new ArrayList<>();
        System.out.println(ANSI_BLUE + "-----------HORIZONTAL-------------" + ANSI_RESET);
        try {
            raf.seek(0);
            int lNumeroLineas = 0;
            while (raf.getFilePointer() < raf.length()) {
                String actual = raf.readLine();
                lNumeroLineas++;
                String ipLinea = "";

                if (actual.toLowerCase().contains("command") && actual.toLowerCase().contains(this.f.getName().toLowerCase())) {
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

                    if (ValorAsignadoOperacion != 0 && c.compareTo("command") == 0 && ns.compareTo(this.f.getName().toLowerCase()) == 0) {
                        j2 = new JSONObject(j.get("attr").toString());
                        String remote = j2.get("remote").toString().toLowerCase().trim();
                        ipLinea = remote.substring(0, remote.indexOf(":"));
                        System.out.println("Number of operation: " + ANSI_BLUE + lNumeroLineas + ANSI_RESET);
                        System.out.println(actual);
                        //System.out.println("SITE: " + ipLinea);
                        String where = Tools.where_statement(actual);
                        System.out.println("Where: " + where);
                        System.out.println(" ");

                        for (Atributo a : this.f.getAttributes()) {
                            if (where.contains(a.getNombre())) {
                                Pattern pattern = Pattern.compile(a.getNombre());
                                Matcher matcher = pattern.matcher(where);

                                while (matcher.find()) {
                                    String r = where.substring(matcher.start(), where.indexOf("}", matcher.start()));
                                    if (r.contains("$eq") || r.contains("$gt") || r.contains("$gte")
                                            || r.contains("$in") || r.contains("$lt")
                                            || r.contains("$lte") || r.contains("$ne")
                                            || r.contains("$nin")) {
                                        ArrayList<String> item = new ArrayList<>();
                                        item.add("" + ValorAsignadoOperacion);
                                        item.add(ipLinea);
                                        String p = "{\"" + r;
                                        Long nLlaves = p.chars().filter(x -> x == '{').count();
                                        p = p + (new String(new char[nLlaves.intValue()]).replace("\0", "}"));
                                        item.add(p);

                                        double tuplas = Tools.nTuplasBD_Xamana(p); //nTuplasBD_Xamana permite obtener el nro de tuplas almacenados en la BD, tabla predicado
                                        item.add("" + tuplas);
                                        double costoOpe = (ipLinea.compareTo(this.f.getFragmentIP()) == 0)
                                                ? ValorAsignadoOperacion * 1 * tuplas : ValorAsignadoOperacion * 2 * tuplas;
                                        item.add("" + costoOpe);
                                        if (predicadoCosto.containsKey(p)) {
                                            if (predicadoCosto.get(p).getCostoSitio().containsKey(ipLinea)) {
                                                predicadoCosto.get(p).getCostoSitio().put(ipLinea, predicadoCosto.get(p).getCostoSitio().get(ipLinea) + costoOpe);
                                            } else {
                                                predicadoCosto.get(p).getCostoSitio().put(ipLinea, costoOpe);
                                            }
                                        } else {
                                            CostoSitio cs = new CostoSitio();
                                            cs.getCostoSitio().put(ipLinea, costoOpe);
                                            predicadoCosto.put(p, cs);
                                        }
                                        this.getMCRUD().add(item);

                                    } else {
                                        ArrayList<String> item = new ArrayList<>();
                                        item.add("" + ValorAsignadoOperacion);
                                        item.add(ipLinea);
                                        String p = "{\"" + r + "}";
                                        item.add(p);
                                        double tuplas = Tools.nTuplasBD_Xamana(p);
                                        item.add("" + tuplas);
                                        double costoOpe = (ipLinea.compareTo(this.f.getFragmentIP()) == 0)
                                                ? ValorAsignadoOperacion * 1 * tuplas : ValorAsignadoOperacion * 2 * tuplas;
                                        item.add("" + costoOpe);
                                        if (predicadoCosto.containsKey(p)) {
                                            if (predicadoCosto.get(p).getCostoSitio().containsKey(ipLinea)) {
                                                predicadoCosto.get(p).getCostoSitio().put(ipLinea, predicadoCosto.get(p).getCostoSitio().get(ipLinea) + costoOpe);
                                            } else {
                                                predicadoCosto.get(p).getCostoSitio().put(ipLinea, costoOpe);
                                            }
                                        } else {
                                            CostoSitio cs = new CostoSitio();
                                            cs.getCostoSitio().put(ipLinea, costoOpe);
                                            predicadoCosto.put(p, cs);
                                        }
                                        this.getMCRUD().add(item);
                                    }
                                }
                            }//cierre del if sí where contiene el nombre del atributo

                        } //Cierre del for por cada atributo

                    }

                }

            }
            System.out.println(ANSI_BLUE + "::::::::::Creating MCRUD::::::::::" + ANSI_RESET);
            //this.getMCRUD().forEach(mcrud -> System.out.println(mcrud));
        } catch (Exception ex) {
            //ex.printStackTrace();
            System.out.println(ANSI_RED + "Mistake in crearEsquema() method within HMongo class: " + ex.getMessage() + ANSI_RESET);
        }
        HashMap<String, Double> atributoValor = new HashMap<>();

        System.out.println(ANSI_BLUE + "::::::::::Creating ALP::::::::::::" + ANSI_RESET);
        //ALP
        if (!predicadoCosto.isEmpty()) {
            for (Atributo a : this.f.getAttributes()) {
                for (String clave : predicadoCosto.keySet()) {
                    if (clave.contains(a.getNombre())) {
                        //sumar valores de sitios por predicado
                        double costoPredicadoSitio = 0.0;
                        for (String sitio : predicadoCosto.get(clave).getCostoSitio().keySet()) {
                            costoPredicadoSitio += predicadoCosto.get(clave).getCostoSitio().get(sitio);
                        }
                        if (atributoValor.containsKey(a.getNombre())) {
                            atributoValor.put(a.getNombre(), atributoValor.get(a.getNombre()) + costoPredicadoSitio);
                        } else {
                            atributoValor.put(a.getNombre(), costoPredicadoSitio);
                        }
                    }

                }
            }

        }
        String atributo = " ";

        this.ALP = new ArrayList<>();
        if (!atributoValor.isEmpty()) {
            double valor = 0.0;

            for (String clave : atributoValor.keySet()) {
                //System.out.println("Clave: " + clave);
                ArrayList<String> item2 = new ArrayList<>();
                item2.add(clave);
                item2.add("" + atributoValor.get(clave));
                this.ALP.add(item2);
                if (atributoValor.get(clave) >= valor) {
                    atributo = clave;
                    valor = atributoValor.get(clave);
                }
                //this.ALP.forEach(x -> System.out.println("ALP: " + x));
            }

        }
        //System.out.println("Attribute: " + atributo);
        System.out.println(ANSI_BLUE + "::::::::::Final Table:::::::::::::" + ANSI_RESET);
        System.out.println(" ");
        //Tabla Final
        HashMap<String, CostoSitio> predicadoCostoFiltrado = new HashMap<>();
        if (!atributo.isEmpty()) {
            for (String clave : predicadoCosto.keySet()) {
                if (clave.contains(atributo)) {
                    predicadoCostoFiltrado.put(clave, predicadoCosto.get(clave));
                }
            }
        }

        if (!predicadoCostoFiltrado.isEmpty()) {
            for (String clave : predicadoCostoFiltrado.keySet()) {
                Fragment f = new Fragment();
                f.setPredicate(clave);
                //System.out.println("Clave: " + clave);
                //donde asignar
                double sitioMasCostoso = 0.0;
                double costoFragmento = 0.0;
                String sitioAsignacion = "";
                for (String s : predicadoCostoFiltrado.get(clave).getCostoSitio().keySet()) {
                    if (predicadoCostoFiltrado.get(clave).getCostoSitio().get(s) >= sitioMasCostoso) {
                        sitioMasCostoso = predicadoCostoFiltrado.get(clave).getCostoSitio().get(s);
                        sitioAsignacion = s;
                    }
                    costoFragmento += predicadoCostoFiltrado.get(clave).getCostoSitio().get(s);
                }
                f.setPerformance(costoFragmento);
                f.setSite(sitioAsignacion);
                this.esquema.add(f);
                //this.esquema.forEach(e -> System.out.println(e));
            }
            //traslape, filtrado y fragmento sin predicado
            this.traslapeV2(0);
            this.nombrar();
            this.setEsquema(this.filtrado());
            this.traslapeV2(1);
            System.out.println(ANSI_BLUE + "------------------------------------------" + ANSI_RESET);
            System.out.println("Horizontal Scheme");
            System.out.println(ANSI_BLUE + "------------------------------------------" + ANSI_RESET);
            //this.esquema.forEach(x -> System.out.println("Predicate: " + x.getPredicate() + " " + "Traslape: " + x.getPredicadoTraslape()));
            this.esquema.forEach(x -> System.out.println("Fragment name: " + x.getName() + " " + "Predicate: " + x.getPredicate() + " " + "Overlap: " + x.getPredicadoTraslape()));
            System.out.println(ANSI_BLUE + "------------------------------------------" + ANSI_RESET);
            System.out.println(" ");
            System.out.println(ANSI_BLUE + "---------------Finisched Horizontal---------------------------" + ANSI_RESET);
            System.out.println(" ");
        } else {
            System.out.println("No se generan nuevos fragmentos");
        }

    } //Cierra crearEsquema

    private double nTuplas02(String nombre, String predicado, String direccion) {
        //System.out.println("Fragment name: " + nombre + "\nCount of tuples: " + predicado + "\nSite : " + direccion);
        double count1 = 0.0;
        try {
            //mongo
            if (nombre.equals(this.datosBD.getTabla().getNombre())) {
                //System.out.println("Equals");
            } else {
                DataAccessMongoDB accOrigen = new DataAccessMongoDB(direccion + ":" + this.datosBD.getPortbd(), this.datosBD.getNombd(), this.datosBD.getTabla().getNombre(), this.datosBD.getUsubd(), this.datosBD.getPassbd());
                if (accOrigen.connect()) {
                    MongoCollection<Document> doc = accOrigen.getMongoClient().getDatabase(this.datosBD.getNombd()).getCollection(nombre);
                    Document d = Document.parse((predicado.isEmpty()) ? "{}" : predicado);
                    count1 = doc.countDocuments(d);
                    accOrigen.getMongoClient().close();
                }
            }
        } catch (Exception ex) {
            //ex.printStackTrace();
            System.out.println(ANSI_RED + "Mistake in nTuplas02(String nombre, String predicado, String direccion) method within HMongo class: " + ex.getMessage() + ANSI_RESET);
        }
        /*
        System.out.println("Number of tuples:" + " " + count1);
        System.out.println("");
        /*/
        return count1;
    }

    private void nombrar() {
        for (int i = 0; i < this.getEsquema().size(); i++) {
            this.getEsquema().get(i).setName(this.datosBD.getTabla().getNombre() + "_" + (i + 1));
            //System.out.format("Name: ", this.getEsquema().get(i).getName());
        }
    }

    private List<Fragment> filtrado() {
        ArrayList<Fragment> arr = new ArrayList<>();
        //eliminar fragmentos que no contienen tuplas
        for (Fragment f : this.esquema) {
            String predicado = f.getPredicadoTraslape();
            if (f.getTuples() > 0) {
                arr.add(f);
            }

        }
        return arr;
    }

    private void traslapeV2(int modo) {//0 no incluir sin predicado 1 incluirlo
        //System.out.println(ANSI_BLUE + "-----------------Traslape-----------------" + ANSI_RESET);
        Collections.sort(this.esquema, (Fragment f1, Fragment f2) -> new Double(f2.getPerformance()).compareTo(new Double(f1.getPerformance())));
        //producir predicados con traslape
        String negado = "";
        for (Fragment f : this.esquema) {
            if (f.getPredicate().contains(":")) {
                String[] camposValores = f.getPredicate().substring(1, f.getPredicate().length() - 1).split(",");
                if (negado.isEmpty()) {
                    String valorFuera = "";
                    for (String campoValorDentro : camposValores) {
                        String campo = campoValorDentro.substring(0, campoValorDentro.indexOf(":"));
                        String valor = campoValorDentro.substring(campoValorDentro.indexOf(":") + 1);
                        if (!valor.contains("$eq") && !valor.contains("$gt") && !valor.contains("$gte")
                                && !valor.contains("$in") && !valor.contains("$lt")
                                && !valor.contains("$lte") && !valor.contains("$ne")
                                && !valor.contains("$nin")) {
                            valorFuera += campo + ":{$eq:" + valor + "},";
                        } else {
                            valorFuera += campo + ":" + valor + ",";
                        }
                    }
                    valorFuera = valorFuera.substring(0, valorFuera.length() - 1);
                    f.setPredicadoTraslape("{" + valorFuera + "}");
                } else {
                    //corrección de llaves
                    f.setPredicate(f.getPredicate().replace("}", ""));
                    Long nLlaves = f.getPredicate().chars().filter(x -> x == '{').count();
                    f.setPredicate(f.getPredicate() + (new String(new char[nLlaves.intValue()]).replace("\0", "}")));
                    f.setPredicadoTraslape(negado + f.getPredicate());
                }
                String valorFuera = "";
                for (String campoValorDentro : camposValores) {
                    String campo = campoValorDentro.substring(0, campoValorDentro.indexOf(":"));
                    String valor = campoValorDentro.substring(campoValorDentro.indexOf(":") + 1);
                    if (!valor.contains("$eq") && !valor.contains("$gt") && !valor.contains("$gte")
                            && !valor.contains("$in") && !valor.contains("$lt")
                            && !valor.contains("$lte") && !valor.contains("$ne")
                            && !valor.contains("$nin")) {
                        String temp = "{" + campo + ":{$not:{$eq:" + valor;

                        //corrección de llaves
                        temp = temp.replace("}", "");
                        Long nLlaves = temp.chars().filter(x -> x == '{').count();
                        temp = (temp + (new String(new char[nLlaves.intValue()]).replace("\0", "}"))) + ",";

                        valorFuera += temp;
                    } else {
                        String temp = "{" + campo + ":{$not:" + valor;

                        //corrección de llaves
                        temp = temp.replace("}", "");
                        Long nLlaves = temp.chars().filter(x -> x == '{').count();
                        temp = (temp + (new String(new char[nLlaves.intValue()]).replace("\0", "}"))) + ",";

                        valorFuera += temp;
                    }
                }
                valorFuera = valorFuera.substring(0, valorFuera.length() - 1);
                negado = "{$or:[" + valorFuera + "]}," + negado;
            }
        }
        for (Fragment f2 : this.esquema) {
            if (f2.getPredicadoTraslape() != null) {
                if (f2.getPredicadoTraslape().length() >= 1 && f2.getPredicadoTraslape().charAt(f2.getPredicadoTraslape().length() - 1) == ',') {
                    f2.setPredicadoTraslape(f2.getPredicadoTraslape().substring(0, f2.getPredicadoTraslape().length() - 1));

                    for (Fragment fragment : this.todos_Fragmentos) {
                        f2.setTuples((long) f2.getTuples() + (long) this.nTuplas02(fragment.getName(), f2.getPredicadoTraslape(), fragment.getSite()));
                    }

                }
                f2.setPredicadoTraslape("{\"$and\":[" + f2.getPredicadoTraslape() + "]}");
                for (Fragment fragment : this.todos_Fragmentos) {
                    f2.setTuples((long) f2.getTuples() + (long) this.nTuplas02(fragment.getName(), f2.getPredicadoTraslape(), fragment.getSite()));
                }
            }
        }
        if (negado.length() >= 1 && negado.charAt(negado.length() - 1) == ',') {
            negado = negado.substring(0, negado.length() - 1);
        }

        //frag Sin predicado
        if (modo == 1) {
            String and = "{\"$and\":[" + negado + "]}";
            double tuplas = 0;

            for (Fragment fragment : this.todos_Fragmentos) {
                tuplas = tuplas + this.nTuplas02(fragment.getName(), and, fragment.getSite());
            }

            if (tuplas > 0) {
                Fragment fsp = new Fragment();
                fsp.setName("Sin predicado");
                fsp.setPerformance(0);
                fsp.setPredicate(negado);
                fsp.setPredicadoTraslape(and);
                fsp.setTuples((long) tuplas);
                fsp.setSite(this.datosBD.getDirbd());
                this.getEsquema().add(fsp);
            }
        }
    }

    /**
     * @return the MCRUD
     */
    public List<ArrayList<String>> getMCRUD() {
        return MCRUD;
    }

    /**
     * @param MCRUD the MCRUD to set
     */
    public void setMCRUD(List<ArrayList<String>> MCRUD) {
        this.MCRUD = MCRUD;
    }

    /**
     * @return the ALP
     */
    public List<ArrayList<String>> getALP() {
        return ALP;
    }

    /**
     * @param ALP the ALP to set
     */
    public void setALP(List<ArrayList<String>> ALP) {
        this.ALP = ALP;
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

}
