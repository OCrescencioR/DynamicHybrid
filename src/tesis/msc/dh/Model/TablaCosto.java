package tesis.msc.dh.Model;

import java.util.ArrayList;

/**
 *
 * @author aldoo
 */
public class TablaCosto {

    String sitio;
    Atributo atributo;
    double costo;
    String nombre;
    ArrayList<Atributo> arrAtri;
    private boolean multimedia;

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public ArrayList<Atributo> getArrAtri() {
        return arrAtri;
    }

    public void setArrAtri(ArrayList<Atributo> arrAtri) {
        this.arrAtri = arrAtri;
    }

    public double getCosto() {
        return costo;
    }

    public void setCosto(double costo) {
        this.costo = costo;
    }

    public String getSitio() {
        return sitio;
    }

    public void setSitio(String sitio) {
        this.sitio = sitio;
    }

    public Atributo getAtributo() {
        return atributo;
    }

    public void setAtributo(Atributo atributo) {
        this.atributo = atributo;
    }

    public String getAtributosxComa() {
        String cad = "";
        for (Atributo at : this.getArrAtri()) {
            cad += at.getNombre() + ",";
        }
        if (cad.length() > 1) {
            return cad.substring(0, cad.length() - 1);
        } else {
            return "";
        }

    }

    /**
     * @return the multimedia
     */
    public boolean isMultimedia() {
        return multimedia;
    }

    /**
     * @param multimedia the multimedia to set
     */
    public void setMultimedia(boolean multimedia) {
        this.multimedia = multimedia;
    }

}
