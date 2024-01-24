package tesis.msc.dh.Model;

import java.util.HashMap;

/**
 *
 * @author OCrescencioR
 */
public class CostoSitio {

    private HashMap<String, Double> costoSitio = new HashMap<>();

    /**
     * @return the costoSitio
     */
    public HashMap<String, Double> getCostoSitio() {
        return costoSitio;
    }

    /**
     * @param costoSitio the costoSitio to set
     */
    public void setCostoSitio(HashMap<String, Double> costoSitio) {
        this.costoSitio = costoSitio;
    }

}
