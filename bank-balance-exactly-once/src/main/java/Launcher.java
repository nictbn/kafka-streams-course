public class Launcher {
    public static void main(String[] args) {
        if ("stream-application".equals(args[0])) {
            BankBalanceExactlyOnce.sTart();
        } else if ("transaction-producer".equals(args[0])) {
            BankTransactionsProducer.start();
        } else {
            System.out.println("Please specify a valid command line argument!");
            System.out.println("stream-application starts the kafka streams application");
            System.out.println("transaction-producer starts a transaction producer instance");
        }
    }
}
