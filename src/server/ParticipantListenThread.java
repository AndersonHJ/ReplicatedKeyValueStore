/**
 * 
 */
package server;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import Model.OperationType;
import Model.TransactionMessage;
import util.Logger;

/**
 * @author Shiqi Luo
 *
 */
public class ParticipantListenThread implements Runnable {

	// TCP part
	private Socket coodinator = null;
	private ServerSocket listenerSocket = null;
	private DataInputStream input = null;
	private DataOutputStream output = null;
	
	private KeyValueStoreService keyValueStore = null;
	
	/**
	 * @param port
	 * @throws IOException 
	 * 
	 */
	public ParticipantListenThread(int port, KeyValueStoreService keyValueStore) throws IOException {
		this.listenerSocket = new ServerSocket(port+1);
		this.keyValueStore = keyValueStore;
	}
	
	@Override
	public void run() {
		String msg = "";
		String commitMsg = "";
		String response = "";
		
		try{
			while(true){
				this.coodinator = this.listenerSocket.accept();
				this.input = new DataInputStream(coodinator.getInputStream());
				this.output = new DataOutputStream(coodinator.getOutputStream());
				
				// waiting for prepare message
				msg = this.input.readUTF();
				Logger.logServerEvent("participant get a transaction start message: " + msg + ", from " + this.coodinator.getRemoteSocketAddress(), 
						Thread.currentThread().getId()+"");

				while(this.keyValueStore.isWriteLock()){
					Thread.sleep(500);
				}
				
				if(!this.keyValueStore.isWriteLock()){
					this.keyValueStore.writeLock();
					// lock the resource and send back the vote-commit message
					Logger.logServerEvent("participant write lock resource", Thread.currentThread().getId()+"");
					// ready state
					this.output.writeUTF("vote");
					Logger.logServerEvent("participant response vote to " + this.coodinator.getRemoteSocketAddress(), Thread.currentThread().getId()+"");
				}
				
				// waiting for commit message or abort message
				commitMsg = this.input.readUTF();
				Logger.logServerEvent("participant get transaction commit message: " + commitMsg + ", from " + this.coodinator.getRemoteSocketAddress(), 
						Thread.currentThread().getId()+"");

				TransactionMessage request = TransactionMessage.parseQuery(msg);

				// commit the action if received commit message
				if(commitMsg.equals("commit")){
					boolean result = true;
					if(request.getOperationType().equals(OperationType.put)){
						result = this.keyValueStore.put(request.getKey(), request.getValue());
						if(result == false){
							this.output.writeUTF("abortack");
						}
						response = "put operation " + (result == true ? "successed" : "failed");
						Logger.logServerEvent("participant " + response, Thread.currentThread().getId()+"");
					}
					else if(request.getOperationType().equals(OperationType.delete)){
						result = this.keyValueStore.delete(request.getKey());
						if(result == false){
							this.output.writeUTF("abortack");
						}
						response = "delete operation " + (result == true ? "successed" : "failed(key is not existed)");
						Logger.logServerEvent("participant " + response, Thread.currentThread().getId()+"");
					}
					else{
						this.output.writeUTF("abortack");
						throw new IllegalArgumentException("operation: " + msg + " is not supported");
					}

					if(result == false){
						this.output.writeUTF("abortack");
						Logger.logServerEvent("participant response abort ack to " + this.coodinator.getRemoteSocketAddress(), 
								Thread.currentThread().getId()+"");
					}
					else{
						this.output.writeUTF("ack");
						Logger.logServerEvent("participant response commit ack to " + this.coodinator.getRemoteSocketAddress(), 
								Thread.currentThread().getId()+"");
					}
				}
				else{
					this.output.writeUTF("ack");
					Logger.logServerEvent("participant response abort ack to " + this.coodinator.getRemoteSocketAddress(), 
							Thread.currentThread().getId()+"");
				}

				this.keyValueStore.writeUnlock();
				Logger.logServerEvent("participant write unlock resource", Thread.currentThread().getId()+"");
			}
		} catch(Exception e2) {
			e2.printStackTrace();
			Logger.logServerError(e2.toString(), this.coodinator.getRemoteSocketAddress().toString());
			try {
				if(this.keyValueStore.isWriteLock()){
					this.keyValueStore.writeUnlock();
				}
				this.output.close();
				this.input.close();
				this.coodinator.close();
			} catch (IOException | InterruptedException e3) {
				Logger.logServerError(e3.toString(), this.coodinator.getRemoteSocketAddress().toString());
			}
			
		}

	}
}
