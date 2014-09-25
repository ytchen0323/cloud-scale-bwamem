
package accUCLA.api;

import java.lang.Float;

public class big2LittleEndian {

    public static int Int(int i) {
      int b0,b1,b2,b3;

      b0 = (i&0xff)>>0;
      b1 = (i&0xff00)>>8;
      b2 = (i&0xff0000)>>16;
      b3 = (i&0xff000000)>>24;

      return ((b0<<24)|(b1<<16)|(b2<<8)|(b3<<0));
    }
    public static byte[] Float(float f) {
      int floatBits = Float.floatToIntBits(f);
      byte floatBytes[] = new byte[4];
      floatBytes[3] = (byte)(floatBits>>24 & 0xff);
      floatBytes[2] = (byte)(floatBits>>16 & 0xff);
      floatBytes[1] = (byte)(floatBits>>8 & 0xff);
      floatBytes[0] = (byte)(floatBits & 0xff);
      return floatBytes;
    }

    public static byte[] floatArray(float[] f) {
      int len = f.length;
      byte floatBytes[] = new byte[4*len];
      for (int i = 0; i < len; i++) {
        int floatBits = Float.floatToIntBits(f[i]);
        floatBytes[4*i + 3] = (byte)(floatBits>>24 & 0xff);
        floatBytes[4*i + 2] = (byte)(floatBits>>16 & 0xff);
        floatBytes[4*i + 1] = (byte)(floatBits>>8 & 0xff);
        floatBytes[4*i + 0] = (byte)(floatBits & 0xff);
      }
      return floatBytes;
    }

    public static byte[] IntArray(int[] f) {
      int len = f.length;
      byte intBytes[] = new byte[4*len];
      for (int i = 0; i < len; i++) {
        intBytes[4*i + 3] = (byte)(f[i]>>24 & 0xff);
        intBytes[4*i + 2] = (byte)(f[i]>>16 & 0xff);
        intBytes[4*i + 1] = (byte)(f[i]>>8 & 0xff);
        intBytes[4*i + 0] = (byte)(f[i] & 0xff);
      }
      return intBytes;
    }

    public static byte[] floatArray(float[][] f) {
      int len1 = f.length;
      int len2 = f[0].length;
      float[] a = new float[len1*len2];
      for (int i = 0; i < len1; i++)
      {
          System.arraycopy(f[i],0,a,i*len2,len2);
      }
      return floatArray(a);
    }
}
