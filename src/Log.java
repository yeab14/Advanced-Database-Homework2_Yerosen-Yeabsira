import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Error lines for the demos: deadlock, abort, restart, and any other failure.
 */
final class Log {
    private static final String RED = "\u001B[31m";
    private static final String RESET = "\u001B[0m";

    private Log() {
    }

    static void error(String message) {
        if (message == null) {
            message = "";
        }
        System.err.print(RED);
        System.err.print(message);
        if (!message.endsWith("\n")) {
            System.err.println();
        }
        System.err.print(RESET);
        System.err.flush();
    }

    static void error(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        error(sw.toString());
    }
}
