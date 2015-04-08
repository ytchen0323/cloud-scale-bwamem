
package accUCLA.api;

import java.util.*;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.net.Socket;
import java.net.InetAddress;
import java.net.ServerSocket;


public class Connector2FPGA {
    private static final int bufferSize = 16*1024*1024;
    private Socket socket;
    private DataOutputStream o2;
    private DataInputStream in;
    private final String ip;
    private final int port;
    private Boolean is_connected;

    public Connector2FPGA(String ip, int port)
    {
        this.ip = ip;
        this.port = port;
    }
    public void buildConnection( int bigData ) throws IOException
    {
        InetAddress addr = InetAddress.getByName(ip); 
        socket = new Socket(addr, port);
        if( bigData == 1 )
        {
            socket.setReceiveBufferSize( bufferSize );
            socket.setSendBufferSize( bufferSize );
        }
        o2 = new DataOutputStream(socket.getOutputStream());
        in = new DataInputStream(socket.getInputStream());
        is_connected = true;
    }

    public void send( int i ) throws IOException
    {
	//System.out.println ("ACCAPI: send " + i);
        //o2.writeInt(big2LittleEndian.Int(i));
        o2.write(ByteBuffer.allocate(4).order(ByteOrder.nativeOrder()).putInt(i).array(),0,4);
        o2.flush();
    }
    public void send( String str ) throws IOException
    {
        o2.writeBytes(str);
        o2.flush();
    }
    public void send_large_array( byte[] array ) throws IOException
    {
        int packet_size = 256*1024;
        for( int start = 0; start < array.length; start += packet_size )
        {
            if( start + packet_size > array.length ) packet_size = array.length - start;
            o2.write( array, start, packet_size );
        }
    }
    public void send_large_array( byte[] array, int length ) throws IOException
    {
        int packet_size = 256*1024;
        for( int start = 0; start < length; start += packet_size )
        {
            if( start + packet_size > length ) packet_size = length - start;
            o2.write( array, start, packet_size );
        }
    }
    public void send( float[] float_array ) throws IOException
    {
        //send_large_array(o2,big2LittleEndian.floatArray(float_array));
        int len = float_array.length;
        ByteBuffer buf = ByteBuffer.allocate( 4 * len ).order(ByteOrder.nativeOrder());
        for(int i = 0; i < len; i++)
        {
            buf.putFloat(float_array[i]);
        }
        buf.order(ByteOrder.nativeOrder()).position(0);
        if(buf.hasArray())
        {
            send_large_array(buf.array());
        }
        else
        {
            System.out.println("byte buffer not backed by byte array");
        }
        o2.flush();
    }
    public void send( float[][] float_array ) throws IOException
    {
        //send_large_array(big2LittleEndian.floatArray(float_array));
        int len1 = float_array.length;
        int len2 = float_array[0].length;
        ByteBuffer buf = ByteBuffer.allocate( 4 * len1 * len2 ).order(ByteOrder.nativeOrder());
        for(int i = 0; i < len1; i++)
        {
            for(int j = 0; j < len2; j++ )
            buf.putFloat(float_array[i][j]);
        }
        //System.out.println(buf.toString());
        if(buf.hasArray())
        {
            //System.out.println(Arrays.toString(buf.array()));
            send_large_array(buf.array());
        }
        else
        {
            System.out.println("byte buffer not backed by byte array");
        }
        o2.flush();
    }
    public void send( int[] int_array ) throws IOException
    {
        //o2.write(big2LittleEndian.IntArray(int_array));
        int len = int_array.length;
        ByteBuffer buf = ByteBuffer.allocate( 4 * len ).order(ByteOrder.nativeOrder());
        for(int i = 0; i < len; i++)
        {
            buf.putInt(int_array[i]);
        }
        buf.order(ByteOrder.nativeOrder()).position(0);
        if(buf.hasArray())
        {
            send_large_array(buf.array());
        }
        else
        {
            System.out.println("byte buffer not backed by byte array");
        }
        o2.flush();
    }
    public void send( ByteBuffer buf ) throws IOException {
	send_large_array(buf.array());
    }
    public void send( ByteBuffer buf, int length ) throws IOException {
	send_large_array(buf.array(), length);
    }
    public int receive( ) throws IOException
    {
        //return big2LittleEndian.Int(in.readInt( ))+5120;
        return ByteBuffer.allocate(4).putInt(in.readInt( )).order(ByteOrder.nativeOrder()).getInt(0);
    }
    public int[] receive_int( int len ) throws IOException
    {
        byte[] byte_array = new byte[len*4];
        in.readFully(byte_array);
        ByteBuffer buf2 = ByteBuffer.wrap(byte_array).order(ByteOrder.nativeOrder());
        int[] result = new int[len];
        for(int i = 0; i < len; i++)
        {
            result[i] = buf2.getInt();
        }
        return result;
    }
    public ByteBuffer receive_short( int len ) throws IOException
    {
        byte[] byte_array = new byte[len*2];
        in.readFully(byte_array);
        ByteBuffer buf2 = ByteBuffer.wrap(byte_array).order(ByteOrder.nativeOrder());
	return buf2;
        //short[] result = new short[len];
        //for(short i = 0; i < len; i++)
        //{
        //    result[i] = buf2.getShort();
        //}
        //return result;
    }
    public float[] receive_float( int len ) throws IOException
    {
        byte[] byte_array = new byte[len*4];
        in.readFully(byte_array);
        ByteBuffer buf2 = ByteBuffer.wrap(byte_array).order(ByteOrder.nativeOrder());
        float[] result = new float[len];
        for(int i = 0; i < len; i++)
        {
            result[i] = buf2.getFloat();
        }
        return result;
    }
    public float[][] receive_float( int len1, int len2 ) throws IOException
    {
        float[][] result = new float[len1][len2];
        float[] data = receive_float( len1 * len2 );
        for( int i = 0; i < len1; i++ )
        {
                System.arraycopy(data,i*len2,result[i],0,len2);
        }
        return result;
    }
    public void closeConnection( ) throws IOException
    {
        o2.close();
        in.close();
        socket.close();
    }
}
