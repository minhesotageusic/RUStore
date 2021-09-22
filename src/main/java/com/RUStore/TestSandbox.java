package com.RUStore;

/**
 * This TestSandbox is meant for you to implement and extend to 
 * test your object store as you slowly implement both the client and server.
 * 
 * If you need more information on how an RUStorageClient is used
 * take a look at the RUStoreClient.java source as well as 
 * TestSample.java which includes sample usages of the client.
 */
public class TestSandbox{

	public static void main(String[] args) {

		// Create a new RUStoreClient
		RUStoreClient client = new RUStoreClient("localhost", 12345);

		// Open a connection to a remote service
		System.out.println("Connecting to object server...");
		try {
			client.connect();
			System.out.println("Established connection to server.");
			
			String[] l = client.list();
			if(l != null) {
				for (int i = 0 ; i < l.length; i++) {
					System.out.println("l["+i+"]: " + l[i]);
				}
			}else {
				System.out.println("nothing");
			}
			byte[] b1 = client.get("string 1");
			if(b1 != null) {
				System.out.write(b1);

				System.out.println();
			}else {
				System.out.println("nothing 2");
			}
			byte [] b3 = "nice".getBytes();
			int ret = client.put("String 69", b3);
			System.out.println(ret);
			byte [] b2 = client.get("String 69" );
			if(b2 != null) {
				System.out.write(b2);
				System.out.println();
			}else {
				System.out.println("nothing 3");
			}
			int res = client.put("lofi", "./inputfiles/lofi.mp3");
			System.out.println(res);
			byte [] b4 = client.get("lofi");
			if(b4 != null) {
				System.out.write(b4);
				System.out.println();
			}else {
				System.out.println("nothing 4");
			}
			
			client.get("lofi", "./outputfiles/lofi_1.mp3");
			
			System.out.println(client.remove("lofi"));
			
			System.out.println(client.get("lofi", "./outputfiles/lofi_2.html"));
			
			client.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Failed to connect to server.");
		}

	}

}
