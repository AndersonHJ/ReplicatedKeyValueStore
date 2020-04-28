package server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import util.Logger;

/**
 * @author Shiqi Luo
 *
 */
public class KeyValueMultithreadService {

	private Socket client = null;
	private ServerSocket tcpServerSocket = null;
	private int port;

	private KeyValueStoreService keyValueStore = null;
	private Thread participant;
	
	private Coodinator coodinator;
	/**
	 * 
	 */
	public KeyValueMultithreadService(int port) throws IOException {
		this.tcpServerSocket = new ServerSocket(port);
		this.keyValueStore = new KeyValueStoreService();
		this.participant = new Thread(new ParticipantListenThread(port, this.keyValueStore));
		this.participant.start();
		this.port = port;
		this.coodinator = new Coodinator(port);
	}
	
	public void runService() {
		
		while(true){
			try{
				this.client = this.tcpServerSocket.accept();

				Thread clientHandler = new Thread(
						new KeyValueThread(this.client, this.keyValueStore, this.coodinator));
				clientHandler.start();
				
			} catch(IOException e) {
				e.printStackTrace();
				Logger.logServerError(e.getMessage(), this.client.getRemoteSocketAddress().toString());
			}
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			if(args.length != 1){
				throw new IllegalArgumentException("Only accept 1 argument: port");
			}
			KeyValueMultithreadService tcpServer = new KeyValueMultithreadService(Integer.valueOf(args[0]));
			tcpServer.runService();
			
		} catch (IllegalArgumentException | IOException e) {
			Logger.logServerError(e.getMessage(), "null");
		}
	}

}

