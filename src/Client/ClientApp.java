package Client;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

public class ClientApp {

	private void runApp() throws IOException {
		Client client = new Client(5, "serverConfiguration.txt");
		while (true) {
			Scanner sc = new Scanner(System.in);
			System.out.print("Please enter the command:");
			String command = sc.nextLine();
			if (command.equalsIgnoreCase("end")) break;
			if (command.equalsIgnoreCase("create")) {
				System.out.print("Please enter the fileName:");
				String fileName = sc.nextLine();
				client.execute(command, fileName, null, 0);
			} else if (command.equalsIgnoreCase("read")) {
				System.out.print("Please enter the fileName:");
				String fileName = sc.nextLine();
				System.out.print("Please enter the indexOfFile you want to read:");
				int indexOfFile = sc.nextInt();
				client.execute(command, fileName, null, indexOfFile);
			} else if (command.equalsIgnoreCase("append")) {
				System.out.print("Please enter the fileName:");
				String fileName = sc.nextLine();
				System.out.print("Please enter the content you want to append:");
				String toAppendContent = sc.nextLine();
				client.execute(command, fileName, toAppendContent, 0);
			}
		}
		System.out.println("Succeed in executing the app!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
	}

	public static void main(String args[])
	{
		try {
			ClientApp clientApp = new ClientApp();
			clientApp.runApp();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
