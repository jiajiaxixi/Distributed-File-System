package Client;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class ServerConfiguration {
	private HashMap<String, String> serverMap = new HashMap<>();
    public ServerConfiguration(String path) {
	    	   try {
					BufferedReader config = new BufferedReader(new FileReader(path));
				    String line = null;
				    while ((line = config.readLine()) != null) {
				    	    String[] toProcessMessage = line.split(" ");
				    	    String server_id = toProcessMessage[0];
				        String ipAddress = toProcessMessage[1];
				        String port = toProcessMessage[2];
				        serverMap.put(server_id, ipAddress + " " + port);
				    }
				} catch (IOException e) {
					e.printStackTrace();
				} 
    }
    public HashMap<String, String> getServerMap() {		
 	         return serverMap;
    }
}
