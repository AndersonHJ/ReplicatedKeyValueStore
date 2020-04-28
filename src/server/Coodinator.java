package server;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import util.Constants;
import util.Logger;

/**
 * @author Shiqi Luo
 *
 */
public class Coodinator {

	private ConcurrentHashMap<CoodinatorConnectThread, Integer> participants;
	private String message;
	private int port;
	private AtomicInteger commitVoteCount = new AtomicInteger(0);
	private AtomicInteger abortVoteCount = new AtomicInteger(0);
	// As professor metioned, we don't focus fault tolerance right now
	private AtomicInteger inactiveCount = new AtomicInteger(0);
	
	private AtomicInteger commitAckCount = new AtomicInteger(0);
	
	/**
	 * @throws IOException 
	 * @throws UnknownHostException 
	 * 
	 */
	public Coodinator(int port) throws UnknownHostException, IOException {
		participants = new ConcurrentHashMap<>();
		this.port = port+1;
	}
	
	public void sendPrepareMessage(String request) throws UnknownHostException, IOException{
		if(this.participants.size() == 0){
			inactiveCount = new AtomicInteger(0);
			for(int i = 16811; i < 16811 + (Constants.replicaNum * 10); i+=10){
				if(i != port){
					CoodinatorConnectThread participantConnectThread = new CoodinatorConnectThread(i);
					participantConnectThread.start();
					participants.put(participantConnectThread, 0);
				}
				
			}
			Logger.logServerEvent("init coodinator, participantNum: " + participants.size(), Thread.currentThread().getId()+"");
		}
		this.message = request;
		this.abortVoteCount = new AtomicInteger(0);
		this.commitVoteCount = new AtomicInteger(0);
		this.commitAckCount = new AtomicInteger(0);

		for(CoodinatorConnectThread thread: this.participants.keySet()){
			thread.setRunState();
		}
	}
	
	public void finishTransaction(){
		Logger.logServerEvent("finish transaction", Thread.currentThread().getId()+"");
		for(CoodinatorConnectThread thread: this.participants.keySet()){
			thread.setStopState();
		}
	}
	
	public String generatePutMessage(String key, String value){
		return "put " + key.length() + " " + key + value;
	}
	
	public String generateDelMessage(String key){
		return "delete " + key;
	}
	
	public boolean finishVote(){
		for(CoodinatorConnectThread thread: this.participants.keySet()){
			if(thread.isGetResponse() == false){
				return false;
			}
		}
		return this.abortVoteCount.get() + this.commitVoteCount.get() + this.inactiveCount.get() == Constants.replicaNum - 1;
	}
	
	public boolean checkVote(){
		if(this.commitAckCount.get() == Constants.replicaNum - 1){
			return true;
		}
		else{
			return false;
		}
	}
	
	private class CoodinatorConnectThread extends Thread {
		private Socket socket;
		private int port;
		private DataInputStream input = null;
		private DataOutputStream output = null;
		private boolean runState = false;
		
		private boolean getResponse = false;

		public CoodinatorConnectThread(int port) {
			this.port = port;
		}
		
		public void setRunState(){
			runState = true;
		}
		
		public void setStopState(){
			runState = false;
			this.getResponse = false;
		}

		public boolean isGetResponse(){
			return this.getResponse;
		}

		@Override
		public void run() {
			while(true){
				while(this.runState == false || this.getResponse == true){
					try {
						sleep(100);
					} catch (InterruptedException e) {

						e.printStackTrace();
					}
				}
				
				try {
					try{
						this.socket = new Socket("localhost", port);
						this.socket.setSoTimeout(4 * 1000);
					} catch (ConnectException ce){
						inactiveCount.getAndIncrement();
						participants.remove(this);
						return;
					}

					this.input = new DataInputStream(socket.getInputStream());
					this.output = new DataOutputStream(socket.getOutputStream());
					// send prepare message
					this.output.writeUTF(message);
					
					Logger.logServerEvent("coodinator send a start transaction message: " + message + ", to " + this.socket.getRemoteSocketAddress(), 
							Thread.currentThread().getId()+"");
				
					// waiting vote response
					String vote = this.input.readUTF();
					Logger.logServerEvent("coodinator get vote response: " + vote + ", from " + this.socket.getRemoteSocketAddress(),
							Thread.currentThread().getId()+"");
					
					if(vote.equals("vote")){
						commitVoteCount.getAndIncrement();
					}
					else{
						abortVoteCount.getAndIncrement();
					}
					
					while(commitVoteCount.get() + abortVoteCount.get() + inactiveCount.get() < Constants.replicaNum - 1){
						sleep(100);
					}
					Logger.logServerEvent("vote result: commit-" + commitVoteCount + ", abort-" + abortVoteCount, Thread.currentThread().getId()+"");
					
					// send commit/abort message
					if(commitVoteCount.get() == Constants.replicaNum - 1){
						this.output.writeUTF("commit");
						Logger.logServerEvent("coodinator sent commit request to " + this.socket.getRemoteSocketAddress(),
								Thread.currentThread().getId()+"");
					}
					else{
						this.output.writeUTF("abort");
						Logger.logServerEvent("coodinator sent abort request to " + this.socket.getRemoteSocketAddress(),
								Thread.currentThread().getId()+"");
					}

					// waiting ack
					String ackMsg = this.input.readUTF();
					Logger.logServerEvent("coodinator get response: " + ackMsg + " from " + this.socket.getRemoteSocketAddress(),
							Thread.currentThread().getId()+"");

					getResponse = true;

					if(!ackMsg.equals("ack")){
						runState = false;
					}
					else{
						commitAckCount.getAndIncrement();
					}
				
					this.output.close();
					this.input.close();
					this.socket.close();
				} catch (InterruptedException | IOException e2) {
					e2.printStackTrace();
					try {
						this.output.close();
						this.input.close();
						participants.remove(this);
					} catch (IOException e3) {
						
					}
					Logger.logServerError(e2.toString(), this.socket.getRemoteSocketAddress().toString());
				}
			}
		}

	}
}