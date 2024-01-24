package tesis.msc.tools;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Scanner;
import org.json.JSONArray;
import org.json.JSONObject;
import tesis.msc.dh.Data.DataAccessXamana;

/**
 *
 * @author OCrescencioR
 */
public class Tools {

    public static boolean isNullorEmpty(ArrayList arr) {
        boolean res = false;
        try {
            if (arr == null || arr.isEmpty()) {
                res = true;
            } else {
                res = false;
            }
        } catch (Exception e) {
            res = true;
        }
        return res;
    }

    public static boolean isNullorEmpty(String s) {
        return (s != null) ? ((!s.isEmpty()) ? false : true) : true;
    }

    public static String readToken() {

        Scanner in = new Scanner(System.in);
        System.out.println("Enter token obtained from the Web application to initiate the method (The token is only an integer)");
        String token = in.nextLine();

        return token;
    }

    public static String readLog() {

        Scanner in = new Scanner(System.in);
        System.out.println("Enter database log location:" + " " + "C:\\LOGS");
        String log = in.nextLine();
        return log;
    }

    public static String getIP() {
        System.out.println(" ");
        String ip = "";
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface iface = interfaces.nextElement();
                if (iface.isLoopback() || !iface.isUp()) {
                    continue;
                }
                Enumeration<InetAddress> addresses = iface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();

                    if (addr instanceof Inet6Address) {
                        continue;
                    }

                    ip = addr.getHostAddress();
                    //System.out.println(ip);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ip;
    }

    public static int VAO(String linea) {
        int ret = 0;
        JSONObject j3 = null;
        try {
            j3 = new JSONObject(linea);
            if (j3.has("find")) {
                ret = 1;
            } else if (j3.has("delete") || j3.has("insert")) {
                ret = 2;
            } else if (j3.has("update")) {
                ret = 3;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return ret;
    }

    public static String where_statement(String actual) {
        String ret = "";
        //mongo
        JSONObject j;
        try {
            j = new JSONObject(actual);
            String c = j.get("c").toString().toLowerCase().trim();
            JSONObject j2 = new JSONObject(j.get("attr").toString());
            JSONObject j3 = new JSONObject(j2.get("command").toString());
            if (j3.has("find")) {
                if (j3.has("filter")) {
                    JSONObject j4 = new JSONObject(j3.get("filter").toString());
                    ret = j4.toString();
                }
            } else if (j3.has("delete")) {
                if (j3.has("deletes")) {
                    JSONArray j4 = j3.getJSONArray("deletes");
                    if (j4.length() >= 0) {
                        JSONObject j5 = new JSONObject(j4.get(0) + "");
                        if (j5.has("q")) {
                            ret = j5.get("q").toString();
                        }
                    }
                }
            } else if (j3.has("insert")) {
                //insert one
                if (j3.has("documents")) {
                    JSONArray arr = j3.getJSONArray("documents");
                    if (arr.length() >= 0) {
                        ret = arr.get(0).toString();
                    }
                }
            } else if (j3.has("update")) {
                if (j3.has("updates")) {
                    JSONArray j4 = j3.getJSONArray("updates");
                    if (j4.length() >= 0) {
                        JSONObject j5 = new JSONObject(j4.get(0) + "");
                        if (j5.has("q")) {
                            ret = j5.get("q").toString();
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    public static double nTuplasBD_Xamana(String predicado) {
        double res = 0.0;
        String sql;
        try {
            DataAccessXamana accessXamana = new DataAccessXamana();
            if (accessXamana.connect()) {
                sql = "Select MAX(nTuplas) FROM Predicado WHERE descripcion = '" + predicado + "'";
                res = (double) ((ArrayList) accessXamana.ejecutarConsulta(sql).get(0)).get(0);
                //((ArrayList) accXamana.ejecutarConsulta(sql).get(0)).get(0);
            }

        } catch (Exception ex) {
            //ex.printStackTrace();
            System.out.println("Mistake in nTuplasBD_XAMANA() " + ex.getMessage());
        }
        return res;
    }

}
