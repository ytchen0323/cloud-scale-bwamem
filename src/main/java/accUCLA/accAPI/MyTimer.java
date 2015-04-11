
package accUCLA.api;
import java.lang.System;

public class MyTimer {
    private long start;
    public MyTimer( )
    {
        start = System.nanoTime( );
    }
    public void report( )
    {
        long end = System.nanoTime();
        System.out.println("elapsed time: " + (end-start)/1e9);
        start=end;;
    }
}
