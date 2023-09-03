import java.util.concurrent.ExecutionException;

public class Launcher {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        if ("data-producer".equals(args[0])) {
            UserDataProducer.start();
        } else if ("event-enricher".equals(args[0])) {
            UserEventEnricher.start();
        } else {
            System.out.println("Please specify a valid command line argument!");
            System.out.println("data-producer starts the user data producer");
            System.out.println("event-enricher starts a kafka streams enrichment application");
        }
    }
}
