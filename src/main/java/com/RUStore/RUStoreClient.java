package com.RUStore;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

public class RUStoreClient {

	/* any necessary class members here */
	private int port;
	private String host;
	private DataOutputStream out;
	private DataInputStream in;
	private Socket cs;

	/**
	 * RUStoreClient Constructor, initializes default values for class members
	 *
	 * @param host host url
	 * @param port port number
	 */
	public RUStoreClient(String host, int port) {
		// Implement here
		this.host = host;
		this.port = port;
	}

	/**
	 * Opens a socket and establish a connection to the object store server running
	 * on a given host and port.
	 *
	 * @return n/a, however throw an exception if any issues occur
	 */
	public void connect() throws UnknownHostException, IOException {
		// Implement here
		cs = new Socket(host, port); // connect to server
		out = new DataOutputStream(cs.getOutputStream()); // reference output stream
		in = new DataInputStream(cs.getInputStream()); // reference input stream
	}

	/**
	 * Sends an arbitrary data object to the object store server. If an object with
	 * the same key already exists, the object should NOT be overwritten
	 * 
	 * @param key  key to be used as the unique identifier for the object
	 * @param data byte array representing arbitrary data object
	 * 
	 * @return 0 upon success 1 if key already exists Throw an exception otherwise
	 */
	public int put(String key, byte[] data) throws IOException, NullPointerException {
		if (key == null || data == null)
			throw new NullPointerException();
		if (out == null || cs == null)
			throw new NullPointerException();

		// out
		byte[] keyByte = key.getBytes();
		byte[] overallByte = null;
		int lengthOfKey = keyByte.length;
		int dataLength = data.length;
		int command = RUStoreServer.Command.PUT.getValue();
		// in
		int response = 0;

		overallByte = new byte[lengthOfKey + dataLength];
		// write the key and data into one message
		for (int i = 0; i < overallByte.length; i++) {
			if (i < lengthOfKey) {
				overallByte[i] = keyByte[i];
			} else {
				overallByte[i] = data[i - lengthOfKey];
			}
		}
		// send request to server
		out.writeInt(command);
		out.writeInt(keyByte.length);
		out.writeInt(keyByte.length + data.length);
		out.write(overallByte, 0, overallByte.length);
		out.flush();
		// listen for server respond
		response = in.readInt();
		in.readInt(); // read in the empty data length
		in.readFully(new byte[0]); // read in empty data

		if (response == RUStoreServer.Response.HASKEY.getValue())
			return 1;
		return 0;
	}

	/**
	 * Sends an arbitrary data object to the object store server. If an object with
	 * the same key already exists, the object should NOT be overwritten.
	 * 
	 * @param key       key to be used as the unique identifier for the object
	 * @param file_path path of file data to transfer
	 * 
	 * @return 0 upon success 1 if key already exists Throw an exception otherwise
	 * @throws IOException
	 */
	public int put(String key, String file_path) throws IOException, FileNotFoundException, NullPointerException {
		if (key == null || file_path == null)
			throw new NullPointerException();
		if (cs == null || out == null || in == null)
			throw new NullPointerException();
		
		// need to prepare the file so we can 
		// get only bytes of the file
		FileInputStream fileInputStream = null;
		File file = new File(file_path);
		byte[] dataBytes = new byte[(int) file.length()];
		// retrieve data from file_path
		// read through the file and copy content as byte
		fileInputStream = new FileInputStream(file);
        fileInputStream.read(dataBytes);
        fileInputStream.close();
		return put(key, dataBytes);
	}

	/**
	 * Downloads arbitrary data object associated with a given key from the object
	 * store server.
	 * 
	 * @param key key associated with the object
	 * 
	 * @return object data as a byte array, null if key doesn't exist. Throw an
	 *         exception if any other issues occur.
	 * @throws IOException
	 */
	public byte[] get(String key) throws IOException, NullPointerException {
		if (key == null)
			throw new NullPointerException();
		if (out == null || cs == null)
			throw new NullPointerException();

		// out
		int GET = RUStoreServer.Command.GET.getValue();
		// in
		byte[] data = null;
		int responseCode = 0;
		int lengthOfData = 0;

		// send server request
		out.writeInt(GET);
		out.writeInt(key.getBytes().length);
		out.writeInt(key.getBytes().length);
		// send server the request data
		out.writeBytes(key);
		out.flush();
		// listen to response
		responseCode = in.readInt();
		lengthOfData = in.readInt();
		// return null if response code is not okay
		if (responseCode != RUStoreServer.Response.OKAY.getValue()) {
			in.readFully(new byte[0]);
			return null;
		}
		data = new byte[lengthOfData];
		in.readFully(data);
		// return the data
		return data;
	}

	/**
	 * Downloads arbitrary data object associated with a given key from the object
	 * store server and places it in a file.
	 * 
	 * @param key       key associated with the object
	 * @param file_path output file path
	 * 
	 * @return 0 upon success 1 if key doesn't exist Throw an exception otherwise
	 */
	public int get(String key, String file_path) throws IOException, FileNotFoundException {
		if (key == null || file_path == null)
			throw new NullPointerException();
		if (cs == null || out == null || in == null)
			throw new NullPointerException();

		byte[] data = get(key);
		if (data == null)
			return 1;
		//write data to file
		File fo = new File(file_path);	
		OutputStream os = new FileOutputStream(fo);
		os.write(data);
		os.close();
		
		return 0;
	}

	/**
	 * Removes data object associated with a given key from the object store server.
	 * Note: No need to download the data object, simply invoke the object store
	 * server to remove object on server side
	 * 
	 * @param key key associated with the object
	 * 
	 * @return 0 upon success 1 if key doesn't exist Throw an exception otherwise
	 */
	public int remove(String key) throws IOException {
		if (key == null)
			throw new NullPointerException();
		if (cs == null || out == null || in == null)
			throw new NullPointerException();

		// out
		int command = RUStoreServer.Command.REMOVE.getValue();
		// in
		int respond = 0;
		// send request to server
		out.writeInt(command);
		out.writeInt(key.getBytes().length);
		out.writeInt(key.getBytes().length);
		out.writeBytes(key);
		out.flush();

		// listen to server
		respond = in.readInt();
		in.readInt();
		in.readFully(new byte[0]);
		if (respond == RUStoreServer.Response.OKAY.getValue())
			return 0;
		return 1;
	}

	/**
	 * Retrieves of list of object keys from the object store server
	 * 
	 * @return List of keys as string array, null if there are no keys. Throw an
	 *         exception if any other issues occur.
	 */
	public String[] list() throws IOException {
		if (out == null || cs == null)
			return null;
		// Implement here
		String[] keys = null;
		byte[] keyByte = null;
		int numKeys = 0;
		int lengthOfKey = 0;
		int responseCode = 0;
		// send request to server
		out.writeInt(RUStoreServer.Command.LIST.getValue()); // tell server to perform list cmd
		out.writeInt(0); // tell server no key
		out.writeInt(0); // tell server no data
		out.flush();
		// listen to server
		responseCode = in.readInt();
		numKeys = in.readInt(); // read in the number of keys
		// only accept response list response
		if (responseCode != RUStoreServer.Response.LIST.getValue()) {
			// read in the byte
			in.readFully(new byte[0]);
			return null;
		}
		keys = new String[numKeys];
		for (int i = 0; i < numKeys; i++) {
			lengthOfKey = in.readInt(); // read the length of the ith key
			keyByte = new byte[lengthOfKey];
			in.readFully(keyByte); // read in the key
			keys[i] = new String(keyByte);
		}

		return keys;
	}

	/**
	 * Signals to server to close connection before closes the client socket.
	 * 
	 * @return n/a, however throw an exception if any issues occur
	 */
	public void disconnect() throws UnknownHostException, IOException {
		// Implement here

		// we want to send disconnect command to server
		out.writeInt(RUStoreServer.Command.DISCONNECT.getValue());
		out.writeInt(0);
		out.writeInt(0);
		out.flush();

		in.close();
		out.close();
		cs.close();
	}

}
