package tesis.msc.dh.Model;

/**
 *
 * @author Felipe Castro Medina
 */
public class PredicadoCosto {

    private String predicado = "";
    private String predicadoTraslapeHibrido = "";
    private Double costo;
    private String negado;

    /**
     * @return the predicado
     */
    public String getPredicado() {
        return predicado;
    }

    /**
     * @param predicado the predicado to set
     */
    public void setPredicado(String predicado) {
        this.predicado = predicado;
    }

    /**
     * @return the costo
     */
    public Double getCosto() {
        return costo;
    }

    /**
     * @param costo the costo to set
     */
    public void setCosto(Double costo) {
        this.costo = costo;
    }

    /**
     * @return the negado
     */
    public String getNegado() {
        return negado;
    }

    /**
     * @param negado the negado to set
     */
    public void setNegado(String negado) {
        this.negado = negado;
    }

    /**
     * @return the predicadoTraslapeHibrido
     */
    public String getPredicadoTraslapeHibrido() {
        return predicadoTraslapeHibrido;
    }

    /**
     * @param predicadoTraslapeHibrido the predicadoTraslapeHibrido to set
     */
    public void setPredicadoTraslapeHibrido(String predicadoTraslapeHibrido) {
        this.predicadoTraslapeHibrido = predicadoTraslapeHibrido;
    }
}
