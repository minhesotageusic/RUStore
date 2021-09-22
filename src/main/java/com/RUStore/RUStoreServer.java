package com.RUStore;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Hashtable;
import java.util.Set;

/* any necessary Java packages here */

public class RUStoreServer {

	/* any necessary class members here */

	public static int port;

	//command
	public static enum Command{
		GET(0), PUT(1), LIST(2), DISCONNECT(3), REMOVE(4);
		
		private final int value;
		Command(int value){
			this.value = value;
		}
		public int getValue() {
			return value;
		}
	}
	//response
	public static enum Response{
		ERROR(-1), OKAY(0), LIST(1), NOKEY(2), HASKEY(3);

		private final int value;
		Response(int value) {
			// TODO Auto-generated constructor stub
			this.value = value;
		}
		public int getValue() {
			return value;
		}
	}
	
	// server data object
	private static Hashtable<String, byte[]> storage;

	/* any necessary helper methods here */
	public static boolean hasKey(String s) {
		if(storage == null || s == null) return false;
		return storage.containsKey(s);
	}
	/**
	 * Add a given byte array b to the storage with key s. If storage already
	 * contain the given key s, then no change will be made.
	 * 
	 * @param s String that is the key for the given b
	 * @param b byte[] to be stored at the given key s
	 * 
	 */
	public static void addObject(String s, byte[] b) {
		if (storage == null || s == null)
			return;
		if (storage.containsKey(s))
			return;
		storage.put(s, b);
	}
	/**
	 * Remove the given key s and the paired value from the storage
	 * 
	 * @param s String that is the key to be removed
	 */
	public static void removeObject(String s) {
		if (storage == null || s == null)
			return;
		if (!storage.containsKey(s))
			return;
		storage.remove(s);
	}
	/**
	 * Return the array byte associated with the given key s.
	 * Return null if no such key exist, or if the array byte is null;
	 */
	public static byte[] getObject(String s) {
		if(storage == null || s == null) return null;
		if(!storage.containsKey(s)) return null;
		return storage.get(s);
	}
	/**
	 * Return an array of keys from the storage
	 */
	public static String[] getKeys() {
		if(storage == null) return null;
		String[] arr = null;
		Set<String> e = storage.keySet();
		if(e == null) return arr;
		arr = new String[storage.size()];
		int i = 0;
		for(String key : e) {
			arr[i] = key;
			i++;
		}
		return arr;
	}
	public static void printStorage() {
		Set<String> e = storage.keySet();
		for(String key : e) {
			System.out.print(key + "\t" + new String(storage.get(key)) + "\t");
		}
	}
	
	/**
	 * RUObjectServer Main(). Note: Accepts one argument -> port number
	 */
	public static void main(String args[]) {

		// Check if at least one argument that is potentially a port number
		if (args.length != 1) {
			System.out.println("Invalid number of arguments. You must provide a port number.");
			return;
		}

		//initialize the storage
		storage = new Hashtable<String, byte[]> ();
		
		// Try and parse port # from argument
		port = Integer.parseInt(args[0]);
		//clientCount = 0;
		//threadPool = new ThreadPoolExecutor(5, 50, 60000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		// Implement here //
		try {
			System.out.println("Starting Server...");
			ServerSocket listenerSocket = new ServerSocket(port);
			while (true) {
				System.out.println("listenig for connection...");
				Socket clientSocket = listenerSocket.accept();
				
				// Create and execute handler in ThreadPool
				RUStoreServer_Runnable clientHandler = new RUStoreServer_Runnable(clientSocket);

				clientHandler.run();
			}
		} catch (IOException e) {}

	}

}
