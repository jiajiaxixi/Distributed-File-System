package Client;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.logging.Logger;

import MServer.Chunk;
import MServer.Message;
import MServer.ServerConfiguration;

public class Client {
	public final int port = 15000;
	public int client_id;
	public HashMap<String, String> clientAddress;
	public HashMap<InetAddress, String> serverMap;
	static public final String metaServerAddress = "10.176.66.54"; 
	static public final int metaServerPort = 17000;
	private static final Logger logger = Logger.getLogger(Client.class.getName());

	public Client(int client_id, String serverPath) {
		this.client_id = client_id;
		serverMap = new ServerConfiguration(serverPath).getServerMap();
		// Establish connection with all the clients
		//		initialize_connections(config);
	}

	//	// Initialize connection with all the other clients
	//	public void initialize_connections(Configuration config) {
	//		for(String client_id : this.clientAddress.keySet()) {
	//			if(this.client_id != Integer.parseInt(client_id)) {
	//				String receiver_ip = config
	//						.getClientAddressMap()
	//						.get(client_id)
	//						.split(":")[0];
	//				int receiver_port = Integer.parseInt(config
	//						.getClientAddressMap()
	//						.get(client_id)
	//						.split(":")[1]);
	//				try (Socket socket = new Socket(receiver_ip, receiver_port)) {
	//					System.out.println("Success to establish " + client_id + " with " + this.client_id);
	//				} catch(IOException ex) {
	//					System.out.println("Fail to establish connections with " + receiver_ip + ":" + receiver_port);
	//					try {
	//						Thread.sleep(1000);
	//					} catch(InterruptedException exp) {}
	//					initialize_connections(config);
	//				}
	//			}
	//		}
	//	}

	// Execute the operation
	public void execute(String command, String fileName, String toAppendContent, int indexOfFile) throws IOException {
		if (command.equalsIgnoreCase("create")) {
			this.create(fileName);
		} else if (command.equalsIgnoreCase("read")) {
			this.read(fileName, indexOfFile);
		} else if (command.equalsIgnoreCase("append")) {
			this.append(fileName, toAppendContent);
		}
	}

	// Create operation
	public void create(String fileName) throws IOException {
		logger.info("Executing Create Operation");
		logger.info("Opening Socket");
		try {
			Socket socket = new Socket();
			SocketAddress addr = new InetSocketAddress(metaServerAddress, metaServerPort);
			socket.connect(addr);
			OutputStream outputStream = socket.getOutputStream();
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
			logger.info("Sending 'create' " + fileName + " request to the meta server");
			objectOutputStream.writeObject(new Message("create", fileName));
			ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
			Message message = (Message) objectInputStream.readObject();
			if (message.getMessage().equals("OK")) {
				logger.info("Succeed in creating " + fileName + "!");
			} else {
				logger.info(message.getMessage());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// Read operation
	public void read(String fileName, int indexOfFile) throws IOException {
		logger.info("Executing Read Operation");
		try {
			logger.info("Opening Socket");
			Socket socket = new Socket();
			SocketAddress addr = new InetSocketAddress(metaServerAddress, metaServerPort);
			socket.connect(addr);
			OutputStream outputStream = socket.getOutputStream();
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
			logger.info("Sending 'Read' request to the meta server");
			objectOutputStream.writeObject(new Message("read", fileName, 0, indexOfFile));
			outputStream.flush();
			ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
			Message message = (Message) objectInputStream.readObject();
			if (message.getMessage().equals("Server is down!")) {
				logger.info(message.getMessage());
			} else if (message.getMessage().equals("Cannot read non-existent file!")) {
				logger.info(message.getMessage());
			} else if (message.getChunk() == null) {
				logger.info(fileName + " doesn't have that much indexes!");
			} else {
				Chunk toReadChunk = message.getChunk();
				socket = new Socket();
				addr = new InetSocketAddress(toReadChunk.host_Server, Integer.valueOf(serverMap.get(toReadChunk.host_Server).split(" ")[1]));
				socket.connect(addr);
				outputStream = socket.getOutputStream();
				objectOutputStream = new ObjectOutputStream(outputStream);
				logger.info("Sending 'Read' request to the server");
				objectOutputStream.writeObject(new Message("read", fileName, 0, indexOfFile));
				outputStream.flush();
				objectInputStream = new ObjectInputStream(socket.getInputStream());
				message = (Message) objectInputStream.readObject();
				if (message.getMessage().equals("OK")) {
					logger.info("Succeed in reading " + fileName + "!");
					message = (Message) objectInputStream.readObject();
					logger.info("The information is:");
					System.out.println(message.getMessage());
				} else {
					logger.info(message.getMessage());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// Append operation
	public void append(String fileName, String toAppendContent) throws IOException {
		logger.info("Executing Append Operation");
		try {
			logger.info("Opening Socket");
			Socket socket = new Socket();
			SocketAddress addr = new InetSocketAddress(metaServerAddress, metaServerPort);
			socket.connect(addr);
			OutputStream outputStream = socket.getOutputStream();
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
			logger.info("Sending 'append' request to the meta server");
			objectOutputStream.writeObject(new Message("append", fileName, toAppendContent.getBytes().length));
			outputStream.flush();
			ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
			Message message = (Message) objectInputStream.readObject();
			if (message.getMessage().equals("Server is down!")) {
				logger.info(message.getMessage());
			} else if (message.getMessage().equals("Cannot append non-existent file!")) {
				logger.info(message.getMessage());
			} else {
				Chunk toAppendChunk = message.getChunk();
				socket = new Socket();
				addr = new InetSocketAddress(toAppendChunk.host_Server, Integer.valueOf(serverMap.get(toAppendChunk.host_Server).split(" ")[1]));
				socket.connect(addr);
				outputStream = socket.getOutputStream();
				objectOutputStream = new ObjectOutputStream(outputStream);
				logger.info("Sending 'Append' request to the server");
				objectOutputStream.writeObject(new Message("append", fileName, toAppendContent));
				outputStream.flush();
				objectInputStream = new ObjectInputStream(socket.getInputStream());
				message = (Message) objectInputStream.readObject();
				if (message.getMessage().equals("OK")) {
					logger.info("Succeed in appending content to " + fileName + "!");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
