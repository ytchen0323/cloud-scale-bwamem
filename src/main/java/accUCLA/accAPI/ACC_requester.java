
package accUCLA.api;

import java.io.IOException;
import java.lang.InterruptedException;

public class ACC_requester {
    public static int run( ) throws IOException, InterruptedException {
        Connector2FPGA conn = new Connector2FPGA("127.0.0.1", 5000);
        conn.buildConnection( 0 );
        int port = conn.receive( );
        conn.closeConnection( );
        return port;
    }
}
