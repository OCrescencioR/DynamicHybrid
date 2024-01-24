package tesis.msc.dh.Dynamic.Hybrid;

import java.io.DataInputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 *
 * @author Felipe Castro Medina
 */
public class EstadoHilo extends Thread {

    public boolean banderaRefragmentacion;

    @Override
    public void run() {
        try {
            ServerSocket crearServidor = new ServerSocket(5000);
            while (true) {
                System.out.println("Accepting connections over socket");
                Socket socket = crearServidor.accept();
                this.banderaRefragmentacion = new DataInputStream(socket.getInputStream()).readBoolean();
                System.out.println("Re-fragment flag: " + banderaRefragmentacion);
                socket.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
