package tesis.msc.dh.Fragmentations;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import java.io.DataOutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONArray;
import org.json.JSONObject;
import tesis.msc.dh.Data.DataAccessMongoDB;
import tesis.msc.dh.Data.DataAccessXamana;
import tesis.msc.dh.Model.Atributo;
import tesis.msc.dh.Model.Fragment;
import tesis.msc.dh.Model.PredicadoCosto;
import tesis.msc.dh.Model.TablaCosto;
import tesis.msc.dh.Model.BD;
import tesis.msc.dh.Scheme.Schema;
import tesis.msc.tools.Tools;

/**
 * Clase que se basa en HybridDesign
 *
 * @author OCrescencioR
 */
public class Hybrid {

    private double HVCost, VHCost;
    private ArrayList<Fragment> fragments;
    private Fragment fragment;
    BD datosBD;
    private int progress1;
    DataAccessXamana accessXamana;
    private static final String ANSI_BLUE = "\u001B[34m";
    private static final String ANSI_RED = "\u001B[31m";
    private static final String ANSI_RESET = "\u001B[0m";
    private String miIP; //IP del nodo a donde esté el vigilante

    public Hybrid(BD datosBD, Fragment f) {
        fragments = new ArrayList<>();
        this.datosBD = datosBD;
        this.fragment = f;
    }

    public void mix(Schema schema, ArrayList<TablaCosto> esquemaVertical) {
        List<Fragment> hfs = schema.getSchema();
        for (Fragment hf : hfs) {
            for (int i = 0; i < esquemaVertical.size(); i++) {
                TablaCosto vf = esquemaVertical.get(i);
                Fragment newFragment = new Fragment();
                newFragment.setName(hf.getName() + "_" + i);
                newFragment.setPredicate(hf.getPredicate());
                newFragment.setAttributes(vf.getArrAtri());
                newFragment.setSiteh(hf.getSite());
                newFragment.setSitev(vf.getSitio());
                getFragments().add(newFragment);
            }
        }
    }

    public void mixMongo(List<Fragment> hfs, ArrayList<TablaCosto> esquemaVertical) {
        for (Fragment hf : hfs) {
            for (int i = 0; i < esquemaVertical.size(); i++) {
                TablaCosto vf = esquemaVertical.get(i);
                Fragment newFragment = new Fragment();
                newFragment.setName(hf.getName() + "_" + i);
                newFragment.setPredicate(hf.getPredicate());
                newFragment.setAttributes(vf.getArrAtri());
                newFragment.setSiteh(hf.getSite());
                newFragment.setSitev(vf.getSitio());
                getFragments().add(newFragment);

            }
        }
    }

    public void costsMongoDB(RandomAccessFile raf) {
        try {
            int lNumeroLineas = 0;
            raf.seek(0);
            System.out.println(" ");
            System.out.println(ANSI_BLUE + "-----------HYBRID-------------" + ANSI_RESET);
            HashMap<String, String> NombreIp = new HashMap<>();
            //limpiar
            this.setVHCost(0.0);
            this.setHVCost(0.0);

            for (Fragment temp : this.getFragments()) {
                temp.setCostH(0.0);
                temp.setCostV(0.0);
            }
            while (raf.getFilePointer() < raf.length()) {

                String actual = raf.readLine();
                String ipLinea = "";
                lNumeroLineas++;
                //String nombre = "";
                if (actual.toLowerCase().contains("command") && actual.toLowerCase().contains(this.fragment.getName().toLowerCase())) {

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
                    if (ValorAsignadoOperacion != 0 && c.compareTo("command") == 0 && ns.compareTo(this.fragment.getName().toLowerCase()) == 0) {
                        j2 = new JSONObject(j.get("attr").toString());
                        String remote = j2.get("remote").toString().toLowerCase().trim();
                        ipLinea = remote.substring(0, remote.indexOf(":"));
                        System.out.println("Number of operation: " + ANSI_BLUE + lNumeroLineas + ANSI_RESET);
                        System.out.println(actual);
                        //System.out.println("SITE: " + ipLinea);
                        NombreIp.put(ipLinea, ipLinea);
                        /*calcular el número de sitios que necesita visitar para llevar
                        afn cabo la operación (será el número de reuniones necesarias)
                        primero sitiov y luego sitioh
                        /*/
                        ///BUSCAR QUÉ FRAGMENTOS OCUPAN EL PREDICADO DE LA OPERACIÓN 
                        int nJoins = 0;
                        ArrayList<String> atributosOpe = buscarAtributos(actual);
                        //atributosOpe.forEach(x -> System.out.print("Operations attribute: " + x + "\n"));
                        ArrayList<Fragment> fResuelvenAtributos = new ArrayList<>();
                        for (Fragment fragmento : this.getFragments()) {
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

                        String where_statement = Tools.where_statement(actual);
                        double tuplasDelWhere = Tools.nTuplasBD_Xamana(where_statement.toLowerCase());
                        ArrayList<Fragment> fResuelvenPredicados = new ArrayList<>();

                        //filtrado por predicado
                        if (tuplasDelWhere > 0) {
                            for (Fragment fragmento : fResuelvenAtributos) {
                                double tuplasDeAmbosPredicados = 0;
                                if (!where_statement.isEmpty()) {
                                    //fragmento sin predicado
                                    if (fragmento.getName().toLowerCase().contains("predicate") || fragmento.getName().toLowerCase().contains("predicado")) {
                                        String predicadoFragmento = "";
                                        for (Fragment hf2 : this.getFragments()) {
                                            if (!hf2.getName().contains("predicate") && !hf2.getName().contains("predicado")) {
                                                System.out.println(ANSI_BLUE + "Fragment name: " + ANSI_RESET + hf2.getName() + ANSI_BLUE + "\nPredicate: " + ANSI_RESET + hf2.getPredicate());
                                                String campo = hf2.getPredicate().substring(1, hf2.getPredicate().indexOf(":"));
                                                String valor = hf2.getPredicate().substring(hf2.getPredicate().indexOf(":") + 1, hf2.getPredicate().length() - 1);
                                                predicadoFragmento = "{" + campo + ":{\"$not\":" + valor + "}}," + predicadoFragmento;
                                                System.out.println(ANSI_BLUE + "Predicate fragment: " + ANSI_RESET + predicadoFragmento);

                                            }
                                        }
                                        for (Fragment hf3 : this.todos_Fragmentos()) {
                                            tuplasDeAmbosPredicados = +this.nTuplas02(hf3.getName(), "{\"$and\":[" + where_statement.toLowerCase() + "," + predicadoFragmento.substring(0, predicadoFragmento.length() - 1) + "]}", hf3.getSite());
                                            System.out.println("Tuples: {$and:[" + where_statement.toLowerCase() + "," + predicadoFragmento.substring(0, predicadoFragmento.length() - 1) + "]}");
                                        }

                                    } else {
                                        String campo = fragmento.getPredicate().substring(1, fragmento.getPredicate().indexOf(":"));
                                        String valor = fragmento.getPredicate().substring(fragmento.getPredicate().indexOf(":") + 1, fragmento.getPredicate().length() - 1);
                                        System.out.println("Campo: " + " " + campo + " " + "Valor: " + " " + valor);
                                        if (campo.contains("$and")) {
                                            valor = valor.substring(0, valor.length() - 1) + "," + where_statement.toLowerCase() + "]";
                                            for (Fragment hf4 : this.todos_Fragmentos()) {
                                                tuplasDeAmbosPredicados = +this.nTuplas02(hf4.getName(), "{" + campo + ":" + valor + "}", hf4.getSite());
                                            }

                                        } else {
                                            for (Fragment hf5 : this.todos_Fragmentos()) {
                                                tuplasDeAmbosPredicados = +this.nTuplas02(hf5.getName(), "{\"$and\":[" + where_statement.toLowerCase() + ",{" + campo + ":" + valor + "}]}", hf5.getSite());
                                            }

                                        }
                                        System.out.println("Tuples: " + "{$and:[" + where_statement.toLowerCase() + ",{" + campo + ":" + valor + "}]}" + ": " + tuplasDeAmbosPredicados);
                                    }
                                } else {
                                    tuplasDeAmbosPredicados = 1;
                                }
                                if (tuplasDeAmbosPredicados > 0) {
                                    fResuelvenPredicados.add(fragmento);
                                }
                            }

                        }
                        nJoins = (fResuelvenPredicados.size() < 1) ? 1 : fResuelvenPredicados.size();
                        /*buscar en esquema si la operación se resuelve en parte o entera de manera remota 
                        primero sitiov y luego sitioh */
                        boolean siHay = false;

                        for (Fragment fragmento : fResuelvenPredicados) {
                            if (fragmento.getSitev().compareTo(ipLinea) != 0) {
                                siHay = true;
                            }
                        }
                        if (siHay) {
                            ValorAsignadoOperacion = ValorAsignadoOperacion * 2;
                        }

                        System.out.println("VAO: " + ValorAsignadoOperacion);
                        System.out.println("CR: " + nJoins);
                        double costoOperacionv = 0.0;

                        for (Atributo a : this.datosBD.getTabla().getAtributos()) {
                            if (actual.toLowerCase().contains(a.getNombre().toLowerCase())) {
                                //System.out.println("Size of attribute " + afn.getNombre() + ": " + Double.parseDouble(afn.getTamanio()));
                                costoOperacionv += ValorAsignadoOperacion * nJoins * Double.parseDouble(a.getTamanio());
                            }
                        }
                        System.out.println("Operation cost vertical-vertical: " + costoOperacionv);
                        this.setVHCost(this.getVHCost() + costoOperacionv);
                        //se recalcula pero ahora con sitioh para el HVcosto
                        ValorAsignadoOperacion = Tools.VAO(j2.get("command").toString());
                        siHay = false;

                        for (Fragment fragmento : fResuelvenPredicados) {
                            if (fragmento.getSiteh().compareTo(ipLinea) != 0) {
                                siHay = true;
                            }
                        }
                        if (siHay) {
                            ValorAsignadoOperacion = ValorAsignadoOperacion * 2;
                        }

                        double costoOperacionh = 0.0;
                        for (Atributo a : this.datosBD.getTabla().getAtributos()) {
                            if (actual.toLowerCase().contains(a.getNombre().toLowerCase())) {
                                //System.out.println("Size of attribute " + afn.getNombre() + ": " + Double.parseDouble(afn.getTamanio()));
                                costoOperacionh += ValorAsignadoOperacion * nJoins * Double.parseDouble(a.getTamanio());
                            }
                        }

                        System.out.println("Operation cost vertical-horizontal: " + costoOperacionh);
                        this.setHVCost(this.getHVCost() + costoOperacionh);
                        //////////horizontal
                        System.out.println("Tuples: " + tuplasDelWhere);
                        /*¿qué sitios se requieren para satisfacer el predicado y atributos 
                        de la operación? R: los mismos sitios de la vertical /*/

                        //¿se requieren sitios remotos? R: de la misma manera que en la vertical
                        double costohorizontalh = tuplasDelWhere * ValorAsignadoOperacion;
                        ValorAsignadoOperacion = Tools.VAO(j2.get("command").toString());
                        siHay = false;
                        for (Fragment fragmento : fResuelvenPredicados) {
                            if (fragmento.getSitev().compareTo(ipLinea) != 0) {
                                siHay = true;
                            }
                        }
                        if (siHay) {
                            ValorAsignadoOperacion = ValorAsignadoOperacion * 2;
                        }
                        System.out.println("VAO: " + ValorAsignadoOperacion);
                        double costohorizontalv = tuplasDelWhere * ValorAsignadoOperacion;

                        System.out.println("Remote cost horizontal-vertical: " + costohorizontalv);
                        System.out.println("Remote cost horizontal-horizontal: " + costohorizontalh);
                        System.out.println(" ");

                        this.setVHCost(this.getVHCost() + costohorizontalv);
                        this.setHVCost(this.getHVCost() + costohorizontalh);

                        //Agregar el costo afn los fragmentos que resuelven la operación
                        for (int i = 0; i < this.fragments.size(); i++) {
                            boolean b = false;
                            for (int r = 0; r < fResuelvenPredicados.size(); r++) {

                                if (fResuelvenPredicados.get(r).getName().compareTo(fragments.get(i).getName()) == 0) {
                                    b = true;
                                }
                            }
                            if (b) {
                                System.out.println("The cost of operation is allocated to the fragment: " + ANSI_BLUE + fragments.get(i).getName() + ANSI_RESET);
                                fragments.get(i).setCostV(fragments.get(i).getCostH() + costoOperacionv + costohorizontalv);
                                fragments.get(i).setCostH(fragments.get(i).getCostV() + costoOperacionh + costohorizontalh);
                                System.out.println(" ");
                            }
                        }

                    }

                }

            }
            System.out.println(ANSI_BLUE + "------------------------------------------" + ANSI_RESET);
            System.out.println(" ");

        } catch (Exception ex) {
            //ex.printStackTrace();
            System.out.println(ANSI_RED + "Mistake in costsMongoDB(RandomAccessFile raf) method within Hybrid class: " + ex.getMessage() + ANSI_RESET);
        }
    }

    private ArrayList<String> buscarAtributos(String actual) {
        ArrayList<String> ret = new ArrayList<>();
        if (this.datosBD.getTipoBD().compareTo("d") == 0) {
            //mongo
            JSONObject j;
            try {
                j = new JSONObject(actual);
                String c = j.get("c").toString().toLowerCase().trim();
                JSONObject j2 = new JSONObject(j.get("attr").toString());
                JSONObject j3 = new JSONObject(j2.get("command").toString());
                if (j3.has("find")) {
                    if (j3.has("projection")) {
                        JSONObject j4 = new JSONObject(j3.get("projection").toString());
                        for (Atributo a : this.datosBD.getTabla().getAtributos()) {
                            if (j4.has(a.getNombre())) {
                                ret.add(a.getNombre().toLowerCase());
                            }
                        }
                    } else {
                        for (Atributo a : this.datosBD.getTabla().getAtributos()) {
                            ret.add(a.getNombre().toLowerCase());
                        }
                    }
                } else if (j3.has("delete")) {
                    for (Atributo a : this.datosBD.getTabla().getAtributos()) {
                        ret.add(a.getNombre().toLowerCase());
                    }
                } else if (j3.has("insert")) {
                    //insert many
                    if (j3.has("documents")) {
                        JSONArray arr = j3.getJSONArray("documents");
                        for (int i = 0; i < arr.length(); i++) {
                            for (Atributo a : this.datosBD.getTabla().getAtributos()) {
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
                    for (Atributo a : this.datosBD.getTabla().getAtributos()) {
                        if (j6.has(a.getNombre())) {
                            ret.add(a.getNombre().toLowerCase());
                        }
                    }
                }
            } catch (Exception ex) {
                //ex.printStackTrace();
                System.out.println(ANSI_RED + "Mistake in buscarAtributos(String actual) method within Hybrid class: " + ex.getMessage());
            }

        }

        return ret;

    }

    private long nTuplas02(String nombre, String predicado, String direccion) {
        //System.out.println("Count of tuples: " + predicado);
        long count1 = 0;
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
            System.out.println(ANSI_RED + "Mistake in nTuplas02(String nombre, String predicado, String direccion) within Hybrid class: " + ex.getMessage() + ANSI_RESET);
        }
        /*
        System.out.println("Number of tuples:" + " " + count1);
        System.out.println("");
        /*/
        return count1;
    }

    public void hybridOverlapMongoDB(int opc) {
        ArrayList<String> predicados = new ArrayList<>(); //predicados sin repetir (estan repetidos en el esquema híbrido)
        for (Fragment f : this.fragments) {
            if (!predicados.contains(f.getPredicate())) {
                predicados.add(f.getPredicate());
            }
        }
        ArrayList<PredicadoCosto> predicadoCosto = new ArrayList<>();
        for (String item : predicados) {
            int ocurrencias = 0;
            double costo = 0;
            for (Fragment f : this.fragments) {
                if (f.getPredicate().compareTo(item) == 0) {
                    ocurrencias++;
                    costo = (opc == 1) ? costo + f.getCostV() : costo + f.getCostH();
                }
            }
            PredicadoCosto p = new PredicadoCosto();
            p.setPredicado(item);
            p.setCosto(costo / ocurrencias);
            predicadoCosto.add(p);
        }
        Collections.sort(predicadoCosto, (PredicadoCosto a, PredicadoCosto b) -> new Double(b.getCosto()).compareTo(new Double(a.getCosto())));
        ArrayList<Fragment> ordenados = new ArrayList<>();
        for (PredicadoCosto item : predicadoCosto) {
            for (Fragment hf : this.getFragments()) {
                if (hf.getPredicate().compareTo(item.getPredicado()) == 0) {
                    ordenados.add(hf);
                }
            }
        }
        this.setFragments(ordenados);
        String negado = "";
        ArrayList<PredicadoCosto> aux = predicadoCosto;
        for (PredicadoCosto item : predicadoCosto) {
            if (item.getPredicado().contains(":")) {
                String[] camposValores = item.getPredicado().substring(1, item.getPredicado().length() - 1).split(",");
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
                    item.setPredicadoTraslapeHibrido("{" + valorFuera + "}");
                } else {
                    //corrección de llaves
                    item.setPredicado(item.getPredicado().replace("}", ""));
                    Long nLlaves = item.getPredicado().chars().filter(x -> x == '{').count();
                    item.setPredicado(item.getPredicado() + (new String(new char[nLlaves.intValue()]).replace("\0", "}")));
                    item.setPredicadoTraslapeHibrido(negado + item.getPredicado());
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
            }//cierre del if (item.getPredicado().contains(":"))
        }// cierre del for(PredicadoCosto item : predicadoCosto)

        for (PredicadoCosto f2 : predicadoCosto) {
            if (f2.getPredicadoTraslapeHibrido() != null) {
                if (f2.getPredicadoTraslapeHibrido().length() >= 1 && f2.getPredicadoTraslapeHibrido().charAt(f2.getPredicadoTraslapeHibrido().length() - 1) == ',') {
                    f2.setPredicadoTraslapeHibrido(f2.getPredicadoTraslapeHibrido().substring(0, f2.getPredicadoTraslapeHibrido().length() - 1));
                }
                f2.setPredicadoTraslapeHibrido("{\"$and\":[" + f2.getPredicadoTraslapeHibrido() + "]}");
            } //fin del if(!hf2..
        }//fin del for PredicadoCosto

        if (negado.length() >= 1 && negado.charAt(negado.length() - 1) == ',') {
            negado = negado.substring(0, negado.length() - 1);
        }

        String and = "{\"$and\":[" + negado + "]}";
        //agregar el traslape al esquema
        for (int i = 0; i < aux.size(); i++) {
            for (Fragment f : this.getFragments()) {
                if (aux.get(i).getPredicado().compareTo(f.getPredicate()) == 0) {
                    f.setPredicate(predicadoCosto.get(i).getPredicadoTraslapeHibrido());
                }
            }
        }

        miIP = Tools.getIP();
        Fragment newFragment = new Fragment();
        newFragment.setName("temp");
        newFragment.setAttributes(this.datosBD.getTabla().getAtributos());
        newFragment.setCostH(0.0);
        newFragment.setCostV(0.0);
        newFragment.setSitev(miIP);
        newFragment.setSiteh(miIP);
        newFragment.setPredicate(and);
        this.getFragments().add(newFragment);

    } //fin de hybridOverlapMongoDB

    private void hybridFragmentationMongoDB(int opc, String fragmentar) {
        try {
            //creación de tablas
            this.progress1 = 1;
            int percent = 100 / this.fragments.size();
            for (Fragment frag : this.fragments) {
                this.progress1 += percent;

                String sitio = (opc == 1) ? frag.getSitev() : frag.getSiteh();
                double costo = (opc == 1) ? frag.getCostV() : frag.getCostH();
                if (sitio != this.fragment.getFragmentIP() && fragmentar == frag.getName()) {
                    DataAccessMongoDB accOrigen = new DataAccessMongoDB(this.fragment.getFragmentIP() + ":" + this.datosBD.getPortbd(), this.datosBD.getPortbd(), this.fragment.getName(), this.datosBD.getUsubd(), this.datosBD.getPassbd());
                    DataAccessMongoDB accDestino = new DataAccessMongoDB(sitio + ":" + this.datosBD.getPortbd(), this.datosBD.getNombd(), this.fragment.getName(), this.datosBD.getUsubd(), this.datosBD.getPassbd());
                    if (accDestino.connect() && accOrigen.connect()) {
                        //Destino
                        MongoDatabase dbDestino = accDestino.getMongoClient().getDatabase(this.datosBD.getNombd());
                        MongoCollection<Document> docDestino = dbDestino.getCollection(this.fragment.getName());
                        dbDestino.createCollection(this.fragment.getName());

                        //Origen
                        MongoDatabase dbOrigen = accOrigen.getMongoClient().getDatabase(this.datosBD.getNombd());
                        MongoCollection<Document> docOrigen = dbOrigen.getCollection(this.fragment.getName());

                        List<String> nombresAtt = new ArrayList<>();
                        frag.getAttributes().forEach(x -> nombresAtt.add(x.getNombre()));
                        Bson proyeccion = Projections.fields(Projections.include(nombresAtt));
                        Document d = Document.parse((frag.getPredicate().isEmpty()) ? "{}" : frag.getPredicate());
                        MongoCursor<Document> resultado = docOrigen.find(d).projection(proyeccion).iterator();
                        while (resultado.hasNext()) {
                            docDestino.insertOne(resultado.next());
                        }
                        double cDesempenioTablaOriginal = (opc == 1) ? this.VHCost : this.HVCost;
                        //Insertar afn XAMANA
                        //SITIO
                        String id_sitio = "", sql;
                        DataAccessXamana accXamana = new DataAccessXamana();
                        if (accXamana.connect()) {
                            sql = "insert into Sitio (direccion) "
                                    + "select * from (select '" + sitio + "' as direccion ) as tmp "
                                    + "where not exists (select direccion from Sitio where direccion='" + sitio + "') limit 1;";
                            accXamana.ejecutarComando(sql);
                            sql = "select id_sitio from Sitio where direccion='" + sitio + "'";

                            id_sitio = "" + ((ArrayList) accXamana.ejecutarConsulta(sql).get(0)).get(0);
                            //FRAGMENTO
                            sql = "update Fragmento set id_sitio = '" + id_sitio + "' where  id_fragmento = " + this.fragment.getId_fragment();
                            accXamana.ejecutarComando(sql);
                            System.out.println(sql);
                            accXamana.ejecutarComando("update Tabla set ndesempenio=" + cDesempenioTablaOriginal + " where id_tabla=" + this.datosBD.getTabla().getIdTabla());
                        }
                        //Para eliminar el fragmento del origen
                        System.out.println("Deleting origin...............");
                        accOrigen.getMongoClient().getDatabase(this.datosBD.getNombd()).getCollection(this.fragment.getName()).drop();
                        //Recargar fragmentos en destino
                        //Socket socket2 = new Socket(accDestino.ip, 5000);
                        Socket socket2 = new Socket(sitio, 5000);
                        new DataOutputStream(socket2.getOutputStream()).writeBoolean(true);
                        socket2.close();
                    }
                }
                if (fragmentar == "true" && frag.getTuples() > 0) {
                    System.out.println("Fragment name: " + frag.getName());
                    System.out.println("Destination: " + sitio);
                    DataAccessMongoDB accOrigen = new DataAccessMongoDB(this.fragment.getFragmentIP() + ":" + this.datosBD.getPortbd(), this.datosBD.getPortbd(), this.fragment.getName(), this.datosBD.getUsubd(), this.datosBD.getPassbd());
                    DataAccessMongoDB accDestino = new DataAccessMongoDB(sitio + ":" + this.datosBD.getPortbd(), this.datosBD.getNombd(), frag.getName(), this.datosBD.getUsubd(), this.datosBD.getPassbd());
                    if (accDestino.connect() && accOrigen.connect()) {
                        //Destino
                        MongoDatabase dbDestino = accDestino.getMongoClient().getDatabase(this.datosBD.getNombd());
                        MongoCollection<Document> docDestino = dbDestino.getCollection(frag.getName());
                        dbDestino.createCollection(frag.getName());

                        //Origen
                        MongoDatabase dbOrigen = accOrigen.getMongoClient().getDatabase(this.datosBD.getNombd());
                        MongoCollection<Document> docOrigen = dbOrigen.getCollection(this.fragment.getName());

                        List<String> nombresAtt = new ArrayList<>();
                        frag.getAttributes().forEach(x -> nombresAtt.add(x.getNombre()));
                        Bson proyeccion = Projections.fields(Projections.include(nombresAtt));
                        Document d = Document.parse((frag.getPredicate().isEmpty()) ? "{}" : frag.getPredicate());
                        MongoCursor<Document> resultado = docOrigen.find(d).projection(proyeccion).iterator();
                        while (resultado.hasNext()) {
                            docDestino.insertOne(resultado.next());
                        }

                    }
                    double cDesempenioTablaOriginal = (opc == 1) ? this.VHCost : this.HVCost;
                    //Insertar afn XAMANA
                    //SITIO
                    String id_sitio = "", idFragmento = "", sql;
                    DataAccessXamana accXamana = new DataAccessXamana();
                    if (accXamana.connect()) {
                        sql = "insert into Sitio (direccion) "
                                + "select * from (select '" + sitio + "' as direccion ) as tmp "
                                + "where not exists (select direccion from Sitio where direccion='" + sitio + "') limit 1;";
                        accXamana.ejecutarComando(sql);
                        sql = "select id_sitio from Sitio where direccion='" + sitio + "'";

                        id_sitio = "" + ((ArrayList) accXamana.ejecutarConsulta(sql).get(0)).get(0);
                        //FRAGMENTO
                        sql = "insert into Fragmento(nombre, id_tabla, id_sitio, noperaciones, ndesempenio, isCBIR) value ('"
                                + frag.getName() + "', " + this.datosBD.getTabla().getIdTabla()
                                + ", " + id_sitio + ", " + this.datosBD.getTabla().getNoperaciones() + ", " + costo + ",0)";
                        accXamana.ejecutarComando(sql);
                        System.out.println(sql);

                        idFragmento = "" + ((ArrayList) accXamana.ejecutarConsulta("Select LAST_INSERT_ID()").get(0)).get(0);
                        if (!Tools.isNullorEmpty(idFragmento)) {
                            for (Atributo a : frag.getAttributes()) {
                                accXamana.ejecutarComando("insert into Atributo (tipo,size,nom,id_fragmento,descriptor,multimedia,pk,id_tabla) "
                                        + "values ('" + a.getTipo() + "', " + a.getTamanio() + ", '" + a.getNombre() + "', " + idFragmento + ", "
                                        + (a.isDescriptor() ? 1 : 0) + ", " + (a.isMultimedia() ? 1 : 0) + ", "
                                        + (a.getNombre().compareTo(this.datosBD.getTabla().getAtributoLlave().getNombre()) == 0 ? 1 : 0) + ", null)");
                            }
                            //Desempenio de tabla original
                            accXamana.ejecutarComando("update Tabla set ndesempenio=" + cDesempenioTablaOriginal + " where id_tabla=" + this.datosBD.getTabla().getIdTabla());
                        }
                    }
                    //Recargar fragmentos en destino
                    //Socket socket2 = new Socket(accDestino.ip, 5000);
                    Socket socket2 = new Socket(sitio, 5000);
                    new DataOutputStream(socket2.getOutputStream()).writeBoolean(true);
                    socket2.close();
                    this.progress1 = this.getProgress1() + percent;
                }
            }//cierre del for
            if (fragmentar == "true") {
                DataAccessMongoDB accOrigen = new DataAccessMongoDB(this.fragment.getFragmentIP() + ":" + this.datosBD.getPortbd(), this.datosBD.getPortbd(), this.fragment.getName(), this.datosBD.getUsubd(), this.datosBD.getPassbd());
                DataAccessXamana accXamana = new DataAccessXamana();
                if (accOrigen.connect() && accXamana.connect()) {
                    //Para eliminar el fragmento del origen en MongoDB
                    System.out.println("Deleting origin...............");
                    accOrigen.getMongoClient().getDatabase(this.datosBD.getNombd()).getCollection(this.fragment.getName()).drop();
                    //Eliminar el fragmento en XAMANA:
                    accXamana.ejecutarComando("delete from Atributo where id_fragmento=" + this.fragment.getId_fragment());
                    accXamana.ejecutarComando("delete from Fragmento where id_fragmento=" + this.fragment.getId_fragment());
                }

            }
        } catch (Exception ex) {
            ex.printStackTrace();
            //System.out.println(ANSI_RED + "Mistake in hybridFragmentationMongoDB method within Hybrid class: " + ex.getMessage() + ANSI_RESET);
        }
        this.progress1 = 100;
        this.datosBD.setProgress1(100);

    }

    public void hybridFragmentation(String fragmentar) {
        //1 vertical 2 para horizontal
        int opc = (this.getHVCost() > this.getVHCost()) ? 1 : 2;
        //Sí es para MongoDB
        if (this.datosBD.getTipoBD().compareTo("d") == 0) {
            hybridFragmentationMongoDB(opc, fragmentar);
        }

    }

    public String nombrar() {
        String fragmentar = "true";
        int contador = 1;
        for (int i = 0; i < this.getFragments().size(); i++) {
            double c = 0;
            long c2 = 0;
            int natributos = 0;

            //1A) Calcular el total de documentos (tuplas) de los fragmentos originales
            c = this.nTuplas02(this.fragment.getName(), "", this.fragment.getFragmentIP());
            //1B) Calcular el total de tuplas del nuevo fragmento a formar 
            c2 = this.nTuplas02(this.fragment.getName(), this.getFragments().get(i).getPredicate(), this.fragment.getFragmentIP());
            System.out.println("Total tuples of the original fragment: " + c + "\nTotal tuples of the new fragment: " + c2);

            //2) Obtener los atributos de los fragmentos:
            //2A) Atributos del fragmento original:
            for (int afo = 0; afo < this.fragment.getAttributes().size(); afo++) {
                //System.out.println("Attributes original fragment [" + afo + "]= " + this.fragment.getAttributes().get(afo).getNombre());

                //2B) Atributos del nuevo fragmento:
                for (int afn = 0; afn < this.getFragments().get(i).getAttributes().size(); afn++) {
                    //System.out.println("Attributes new fragment [" + afn + "]= " + this.getFragments().get(i).getAttributes().get(afn).getNombre());

                    /*
                     Comparar cada uno de los atributos del fragmento original con los atributos del nuevo fragmento
                    Sí los atributos del fragmento original son iguales al del fragmento nuevo, entonces que los acumule
                    para posteriormente utilizar el resultado obtenido, caso contrario que no haga nada
                    /*/
                    if (this.fragment.getAttributes().get(afo).getNombre() == this.getFragments().get(i).getAttributes().get(afn).getNombre()) {
                        //System.out.println(ANSI_BLUE + "Equals Attributes: " + ANSI_RESET);
                        afn = this.getFragments().get(i).getAttributes().size();
                        natributos++;
                    } else {
                        //System.out.println(ANSI_RED + "Attributes not equals: " + ANSI_RESET);
                    }
                }//fin del for de los atributos nuevos
            }//fin del for de los atributos originales

            /*Se recuperan los atributos que son iguales y se compara sí son iguales 
            al tamaño del fragmento original, además, sí el número de tuplas del nuevo 
            framento es igual al número de tuplas del fragmento, sí se cumplen estas
            dos concidiones no se requiere fragmentar, caso contrario, se requiere
            refragmentar
            /*/
            if (natributos == this.fragment.getAttributes().size() && c2 == c) {
                fragmentar = this.getFragments().get(i).getName();
                System.out.println("Do not required fragmentation because have equals attributes and tuples");
            } else {
                /*Antes de nombrar para proceder a refragmentar, verificar si el nuevo 
                fragmento a formar tiene tuplas antes de que lo nombre, por lo que,
                se compara sí el nuevo fragmentos tiene tuplas
                /*/
                if (c2 > 0) {
                    this.getFragments().get(i).setTuples(c2);
                    this.getFragments().get(i).setName(this.fragment.getName() + "_" + contador);
                    System.out.println("Fragment name: " + this.getFragments().get(i).getName());
                    System.out.println("Overlap: " + this.getFragments().get(i).getPredicate());
                    this.getFragments().get(i).getAttributes().forEach(x -> System.out.println("Attribute: " + x.getNombre()));
                    contador = contador + 1;
                    System.out.println(" ");
                } else {
                    this.getFragments().get(i).setTuples(0);
                    System.out.println("The fragment is empty ");
                }
            }
        }
        return fragmentar;
    }

    public double nTuplasBD_Xamana(String predicado) {
        double res = 0.0;
        String sql;
        try {
            accessXamana = new DataAccessXamana();
            if (accessXamana.connect()) {
                sql = "Select MAX(nTuplas) FROM Predicado WHERE descripcion_NOSQL = '" + predicado + "'";
                res = accessXamana.ejecutarComando(sql);
            }

        } catch (Exception ex) {
            //ex.printStackTrace();
            System.out.println(ANSI_RED + "Mistake in nTuplasBD_Xamana(String predicado) method within Hybrid class: " + ex.getMessage() + ANSI_RESET);
        }
        return res;
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
        //all.forEach(x -> System.out.println("Framents name: " + x.getName() + "\nPredicate: " + x.getPredicate()));

        return all;

    }

    /**
     * @return the progress1
     */
    public int getProgress1() {
        return progress1;
    }

    /**
     * @param progress1 the progress1 to set
     */
    public void setProgress1(int progress1) {
        this.progress1 = progress1;
    }

    /**
     * @param HVCost the HVCost to set
     */
    public void setHVCost(double HVCost) {
        this.HVCost = HVCost;
    }

    /**
     * @param VHCost the VHCost to set
     */
    public void setVHCost(double VHCost) {
        this.VHCost = VHCost;
    }

    /**
     * @return the HVCost
     */
    public double getHVCost() {
        return HVCost;
    }

    /**
     * @return the VHCost
     */
    public double getVHCost() {
        return VHCost;
    }

    /**
     * @return the fragments
     */
    public ArrayList<Fragment> getFragments() {
        return fragments;
    }

    /**
     * @param fragments the fragments to set
     */
    public void setFragments(ArrayList<Fragment> fragments) {
        this.fragments = fragments;
    }

}
