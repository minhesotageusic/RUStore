package com.RUStore;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.Hashtable;

public class RUStoreServer_Runnable implements Runnable {

	private static class RURequest {
		public int command;
		public String key;
		public byte[] data;

		public RURequest(int command, String key, byte[] data) {
			this.command = command;
			this.key = key;
			this.data = data;
		}
	}

	// Socket communication
	private final Socket clientSocket;
	private DataOutputStream outStream;
	private DataInputStream inStream;

	/**
	 * send message to client by string
	 * 
	 * @param msg String that is going to be sent
	 */
	private void SendMsgToClient(int response, String msg) {
		if (msg == null || clientSocket == null || outStream == null)
			return;
		// send msg to client
		int lengthOfMsg = msg.length();
		try {
			outStream.writeInt(response); // send response int
			outStream.writeInt(lengthOfMsg); // send legnth of msg
			outStream.writeBytes(msg);
			outStream.flush();
		}
		// if there is an issue don't send anything
		catch (IOException e) {}
	}

	/**
	 * send message to client by bytes
	 * 
	 * @param bytes Bytes that is going to be sent
	 */
	private void SendMsgToClient(int response, byte[] bytes) {
		if (bytes == null || clientSocket == null || outStream == null)
			return;
		// send msg to client
		try {
			outStream.writeInt(response); // send response int
			outStream.writeInt(bytes.length); // send length of msg
			outStream.write(bytes, 0, bytes.length);
			outStream.flush();
		}
		// if there is an issue don't send anything
		catch (IOException e) {}
	}

	private RURequest CraftRequestWithKey(int cmd, int keyLength, int dataLength) throws IOException {
		if (keyLength <= 0 || keyLength > dataLength) {
			// key length cannot be greater than data length
			SendMsgToClient(RUStoreServer.Response.ERROR.getValue(), ""); // send error code to client
			return null;
		}
		byte[] overall_data = new byte[dataLength];
		byte[] key = new byte[keyLength];
		byte[] data = new byte[dataLength - keyLength];
		String key_s = null;

		// get || put
		// 1st read in the overall data
		// 2nd factor out the key
		// 3rd remaining is the the actual data
		// read in the overall data
		inStream.readFully(overall_data);
		// partition out the key
		for (int i = 0; i < keyLength; i++) {
			key[i] = overall_data[i];
		}
		// partition out the data
		for (int i = 0; i < data.length; i++) {
			data[i] = overall_data[i + keyLength];
		}
		// convert key into a string
		key_s = new String(key);
		// craft the request object
		return new RURequest(cmd, key_s, data);
	}

	private void HandleListCMD() {
		if (clientSocket == null || outStream == null)
			return;
		// retrieve the keys
		String[] keys = RUStoreServer.getKeys();
		if (keys == null || keys.length == 0) {
			SendMsgToClient(RUStoreServer.Response.OKAY.getValue(), new byte[0]);
			return;
		}
		try {
			// send the key's byte[] one at a time
			// with each iteration, telling the client
			// the key's length and the key itself
			outStream.writeInt(RUStoreServer.Response.LIST.getValue()); // list response
			outStream.writeInt(keys.length); // tell client number of keys are present
			for (int i = 0; i < keys.length; i++) {
				outStream.writeInt(keys[i].getBytes().length); // tell client the length of this key's byte
				outStream.writeBytes(keys[i]); // send the actual key byte
				outStream.flush(); // flush the output
			}
		} catch (IOException e) {
		}
	}

	private void HandleGetCMD(RURequest request) {
		if (clientSocket == null || outStream == null || request == null )
			return;
		if(!RUStoreServer.hasKey(request.key)) {
			SendMsgToClient(RUStoreServer.Response.NOKEY.getValue(), "");
			return;
		}
		byte [] data = RUStoreServer.getObject(request.key);
		if(data == null) {
			SendMsgToClient(RUStoreServer.Response.NOKEY.getValue(), "");
			return;
		}
		//send response
		SendMsgToClient(RUStoreServer.Response.OKAY.getValue(), data);
	}
	private void HandlePutCMD(RURequest request) {
		if(clientSocket == null || outStream == null || request == null) return;
		byte [] data = request.data;
		if(RUStoreServer.hasKey(request.key)) {
			SendMsgToClient(RUStoreServer.Response.HASKEY.getValue(), "");
			return;
		}
		if(data == null) {
			//we have no data
			SendMsgToClient(RUStoreServer.Response.ERROR.getValue(), "");
			return;
		}
		//add object to server with given key
		RUStoreServer.addObject(request.key, data);
		//tell client the add was okay
		SendMsgToClient(RUStoreServer.Response.OKAY.getValue(), "");
	}
	private void HandleRemoveCMD(RURequest request) {
		if(clientSocket == null || outStream == null || request == null) return;
	
		if(request.key == null) {
			SendMsgToClient(RUStoreServer.Response.ERROR.getValue(), "");
			return;
		}

		if(!RUStoreServer.hasKey(request.key)) {
			SendMsgToClient(RUStoreServer.Response.NOKEY.getValue(), "");
			return;
		}

		RUStoreServer.removeObject(request.key);
		SendMsgToClient(RUStoreServer.Response.OKAY.getValue(), "");
	}
	public RUStoreServer_Runnable(Socket clientSocket) {
		this.clientSocket = clientSocket;
	}

	/**
	 * Read in request from the client, parse it, and return an RURequest Object.
	 * 
	 * @return RURequest object representing the client's request
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public RURequest parseRequest() throws IOException {
		int cmdInt = 0;
		int keyLength = 0;
		int dataLength = 0;

		// return object
		RURequest requestObject = null;

		// wait for stream to open
		// may not be needed
		/*startTime = System.nanoTime();
		while (inStream.available() == 0) {
			endTime = System.nanoTime();
			elapsedTime = (long) ((endTime - startTime) / 1000000);
			// timeout
			if (elapsedTime >= timeOut) {
				SendMsgToClient(RUStoreServer.Response.ERROR.getValue(), "");
				return null;
			}
		}*/

		cmdInt = inStream.readInt();
		keyLength = inStream.readInt();
		dataLength = inStream.readInt();

		// if we dont have a command
		// then send error and return
		if (cmdInt < 0) {
			SendMsgToClient(RUStoreServer.Response.ERROR.getValue(), "");
			return null;
		}

		if (cmdInt == RUStoreServer.Command.GET.getValue() || 
			cmdInt == RUStoreServer.Command.PUT.getValue() || 
			cmdInt == RUStoreServer.Command.REMOVE.getValue()) {
			requestObject = CraftRequestWithKey(cmdInt, keyLength, dataLength);
		} else if (cmdInt == RUStoreServer.Command.LIST.getValue() || 
					cmdInt == RUStoreServer.Command.DISCONNECT.getValue() ) {
			// craft the request object
			requestObject = new RURequest(cmdInt, null, null);
		}
		return requestObject;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			RURequest clientRequest = null;

			// Create an output stream and an input stream for server to talk to client
			inStream = new DataInputStream(clientSocket.getInputStream());
			outStream = new DataOutputStream(clientSocket.getOutputStream());
			while (!clientSocket.isClosed()) {
				clientRequest = parseRequest();
				// no request break from loop?
				if (clientRequest == null) {
					break;
				}
				if (clientRequest.command == 3) { // disconnect
					break;
				}

				if(clientRequest.command == RUStoreServer.Command.GET.getValue()) { // get
					HandleGetCMD(clientRequest);
				}else if(clientRequest.command == RUStoreServer.Command.PUT.getValue()) {
					HandlePutCMD(clientRequest);
				}
				else if(clientRequest.command == RUStoreServer.Command.LIST.getValue()) {// list
					HandleListCMD();
				}else if(clientRequest.command == RUStoreServer.Command.REMOVE.getValue()) {
					HandleRemoveCMD(clientRequest);
				}
				else {// unknown command
					SendMsgToClient(RUStoreServer.Response.ERROR.getValue(), "");
				}
				RUStoreServer.printStorage();
			}
		} catch (IOException e) {

		} finally {
			// Close the client's socket and clean up
			try {
				inStream.close();
				outStream.close();
				clientSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
