import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.*;

import java.util.*;

import static java.lang.System.out;

public class DatabaseNode {
    // Parametry uruchomiania węzła sieci

    private static final String PORT_TCP = "-tcpport";
    private static String portTCP;
    private static final String RECORD = "-record";
    private static final String TERMINATE = "terminate";
    private static final String GET_VALUE = "get-value";
    private static final String SET_VALUE = "set-value";
    private static final String FIND_KEY = "find-key";
    private static final String NEW_RECORD = "new-record";

    private static Integer key;
    private static Integer value;
    private static Map<Integer, Integer> map = new HashMap<Integer, Integer>();
    private static List<Database> database = new ArrayList<>();
        static class Database{
            private int key;
            private int value;
            public Database(int key, int value){
                this.key = key;
                this.value = value;
            }
            public int getKey(){
                return key;
            }
            public int getValue(){
                return value;
            }

        }
    private static final String CONNECT = "-connect";
    private static List<String> gateways = new ArrayList<String>();
    private static final String OK  = "OK";
    private static final String ERROR = "ERROR";

    public static void main(String[] args) throws IOException {
        parseArgs(args);
        if (!gateways.isEmpty()) {
            getnodeInfo();
        }nodeOczekujacy();

    }
    public static void nodeOczekujacy() throws IOException {

            ServerSocket nodeSocket = new ServerSocket(Integer.parseInt(portTCP));
            log("Oczekiwanie na połączenie na porcie: " + portTCP);
            while(true){
                Socket socket = nodeSocket.accept();
                log("Połączono z ----> " + socket.getRemoteSocketAddress().toString());
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String command = in.readLine();
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                log("Otrzymano komendę: " + command);
                String[] commandArray = command.split(" ");
                switch(commandArray[0]){
                    case TERMINATE:
                        terminate(out);
                        String ip = socket.getRemoteSocketAddress().toString();
                        log("Klient " + ip + " rozłączony");
                        out.close();
                        socket.close();
                        break;
                    case GET_VALUE:
                        log("Pobieranie wartości");
                        getValue(out,commandArray);
                        out.close();
                        socket.close();
                        break;
                    case SET_VALUE:
                        log("Ustawianie wartości");
                        setValue(out,commandArray);
                        out.close();
                        socket.close();
                        break;
                    case FIND_KEY:
                        log("Szukanie klucza");
                        findKey(out,commandArray,socket);
                        out.close();
                        socket.close();
                        break;
                    case NEW_RECORD:
                        log("Dodawanie nowego rekordu");
                        addNewRecord(out,commandArray);
                        out.close();
                        socket.close();
                        break;
                }
        }
    }
    private static void parseArgs(String[] args) throws UnknownHostException {

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals(PORT_TCP)) {
                portTCP = args[i + 1];

            } else if (args[i].equals(RECORD)) {
                String[] record = new String(args[i + 1]).split(":");
//                    map.put(Integer.parseInt(record[0]), Integer.parseInt(record[1]));
                database.add(new Database(Integer.parseInt(record[0]), Integer.parseInt(record[1])));
            } else if (args[i].equals(CONNECT)) {
                gateways.add(args[i + 1]);

            } else if (args[i].equals(CONNECT)) {
                String[] gateway = new String(args[i + 1]).split(":");
                if (gateway[0].equals("localhost")) {
                    gateways.add(new String(InetAddress.getByName("localhost").getHostAddress() + ":" + gateway[1]));
                }
            }
        }
    }

    private static void getnodeInfo() throws IOException {
        for (int i = 0; i < gateways.size(); i++) {
            String[] connectionDetails = gateways.get(i).split(":");
            String address = connectionDetails[0];
            String port = connectionDetails[1];
            log("NodeInfo: " + address + ":" + port);
            openNode(address, port);
        }
    }

    private static void openNode(String address, String port) throws IOException {

        InetAddress adress = InetAddress.getByName(address); //adres następnego węzła
        int tport = Integer.parseInt(port); //port następnego węzła
        Socket socket = new Socket(address,tport);
        log("Node połączony z ----> " + socket.getInetAddress().getHostAddress().toString() + ":" + socket.getLocalPort());
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        out.write(socket.getInetAddress().getHostAddress() + ":" + portTCP);
        out.close();
        socket.close();

    }

    private static void terminate(PrintWriter out) throws IOException {
            out.println(OK);
            out.flush();
    }


    private static void getValue(PrintWriter out, String[] commandArray) {
        log("Przetwarzam GET_VALUE");
        value = Integer.parseInt(commandArray[1]);
        for (int i = 0; i < database.size(); i++) {
            if (database.get(i).getValue() == value) {
                log("Znaleziono klucz");
                key = database.get(i).getKey();
                log("Wartość klucza: " + key);
                out.write(key.toString() + ":" + value.toString());
                out.flush();
                break;
            } else {
                log("Nie znaleziono klucza");
                out.write(ERROR + "\n");
                out.flush();
            }
        }
    }

        private static Integer getKeyFromValue(Map<Integer, Integer> map, Integer value) {
            for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
                if (value.equals(entry.getValue())) {
                    return entry.getKey();
                }
            }
            return null;
        }

    private static void setValue(PrintWriter out, String[] commandArray) {
        log("Przetwarzam SET_VALUE");
        String[] tempCommandArray = commandArray[1].split(":");
        log("Wartość klucza: " + tempCommandArray[0]);
        log("Wartość wartości: " + tempCommandArray[1]);
        for (int i = 0; i < database.size(); i++) {
            if (database.get(i).getKey() == Integer.parseInt(tempCommandArray[0])) {
                log("Znaleziono klucz");
                log("Zmiana wartości");
                database.remove(i);
                database.add(new Database(Integer.parseInt(tempCommandArray[0]), Integer.parseInt(tempCommandArray[1])));
                log("Wartość klucza " + tempCommandArray[0]);
                log("Wartość wartości " + database.get(i).getValue());
                out.write(OK + "\n");
                out.flush();
                break;
            } else {
                log("Nie znaleziono klucza");
                out.write(ERROR + "\n");
                out.flush();
            }
        }
    }

        private static void addNewRecord(PrintWriter out, String[] commandArray){
            log("Przetwarzam RECORD");
            String[] tempCommandArray = commandArray[1].split(":");
            if(database.size() > 0){
                for (int i = 0; i < database.size(); i++) {
                    log("Klucz: " + database.get(i).getKey());
                    log("Wartość: " + database.get(i).getValue());
                    log("Usuwanie rekordu");
                    database.remove(i);
                    log("Dodawanie nowego rekordu");
                    database.add(new Database(Integer.parseInt(tempCommandArray[0]), Integer.parseInt(tempCommandArray[1])));
                    log("Dodano nowy rekord");
                    log("Klucz: " + database.get(i).getKey());
                    log("Wartość: " + database.get(i).getValue());
                    out.write(OK + "\n");
                    out.flush();
                }
            }
        }

    private static void findKey(PrintWriter out, String[] commandArray,Socket socket){
        log("Przetwarzam FIND_KEY");
        key = Integer.parseInt(commandArray[1]);
        for (int i = 0; i < database.size(); i++) {
            if (database.get(i).getKey() == key) {
                log("Znaleziono klucz");
                log("Odpowiedź: " + socket.getInetAddress().getHostAddress().toString() + ":" + socket.getLocalPort());
                String response = socket.getInetAddress().getHostAddress().toString() + ":" + socket.getLocalPort();
                out.write(response + "\n");
                out.flush();
                break;
            } else {
                log("Nie znaleziono klucza");
                out.write(ERROR + "\n");
                out.flush();
            }
        }
    }

    private static void log(String msg) {
        out.println(msg);
    }

}