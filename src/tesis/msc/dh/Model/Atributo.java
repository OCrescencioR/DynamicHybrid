package tesis.msc.dh.Model;

import java.util.HashMap;

/**
 *
 * @author Felipe Castro Medina
 */
public class Atributo {

    private String idAtributo;
    private String nombre;
    private String tipo;
    private String tamanio;
    private boolean multimedia;
    private boolean descriptor;
    private HashMap<String, Double> costoAtributoxSitio;
    private String sitioMasCostoso;
    private boolean pk;

    public Atributo() {
        costoAtributoxSitio = new HashMap<>();
    }

    public Atributo(String idAtributo, String nombre, String tipo, String tamanio, boolean multimedia, boolean descriptor, HashMap<String, Double> costoAtributoxSitio, String sitioMasCostoso, boolean pk) {
        this.idAtributo = idAtributo;
        this.setNombre(nombre);
        this.setTipo(tipo);
        this.setTamanio(tamanio);
        this.setMultimedia(multimedia);
        this.setDescriptor(descriptor);
        costoAtributoxSitio = new HashMap<>();
        this.setSitioMasCostoso(sitioMasCostoso);
        this.setPk(pk);
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
     * @return the tipo
     */
    public String getTipo() {
        return tipo;
    }

    /**
     * @param tipo the tipo to set
     */
    public void setTipo(String tipo) {
        this.tipo = tipo;
    }

    /**
     * @return the tamanio
     */
    public String getTamanio() {
        return tamanio;
    }

    /**
     * @param tamanio the tamanio to set
     */
    public void setTamanio(String tamanio) {
        this.tamanio = tamanio;
    }

    /**
     * @return the idAtributo
     */
    public String getIdAtributo() {
        return idAtributo;
    }

    /**
     * @param idAtributo the idAtributo to set
     */
    public void setIdAtributo(String idAtributo) {
        this.idAtributo = idAtributo;
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

    /**
     * @return the descriptor
     */
    public boolean isDescriptor() {
        return descriptor;
    }

    /**
     * @param descriptor the descriptor to set
     */
    public void setDescriptor(boolean descriptor) {
        this.descriptor = descriptor;
    }

    /**
     * @return the pk
     */
    public boolean isPk() {
        return pk;
    }

    /**
     * @param pk the pk to set
     */
    public void setPk(boolean pk) {
        this.pk = pk;
    }

    /**
     * @return the costoAtributoxSitio
     */
    public HashMap<String, Double> getCostoAtributoxSitio() {
        return costoAtributoxSitio;
    }

    /**
     * @param costoAtributoxSitio the costoAtributoxSitio to set
     */
    public void setCostoAtributoxSitio(HashMap<String, Double> costoAtributoxSitio) {
        this.costoAtributoxSitio = costoAtributoxSitio;
    }

    /**
     * @return the sitioMasCostoso
     */
    public String getSitioMasCostoso() {
        return sitioMasCostoso;
    }

    /**
     * @param sitioMasCostoso the sitioMasCostoso to set
     */
    public void setSitioMasCostoso(String sitioMasCostoso) {
        this.sitioMasCostoso = sitioMasCostoso;
    }

}
