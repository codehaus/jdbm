/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot.
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2001 (C) Alex Boisvert. All Rights Reserved.
 * Contributions are Copyright (C) 2001 by their associated contributors.
 *
 */

package jdbm.helper;


/**
 * Miscelaneous conversion utility methods.
 */
public class Conversion {

    /**
     * Convert a string into a byte array.
     */
    public static byte[] convert( String s ) {
        try {
            // see the following page for character encoding
            // http://java.sun.com/products/jdk/1.1/docs/guide/intl/encoding.doc.html
            return s.getBytes( "UTF8" );
        } catch ( java.io.UnsupportedEncodingException uee ) {
            uee.printStackTrace();
            throw new Error( "Platform doesn't support UTF8 encoding" );
        }
    }

    /**
     * Convert a byte into a byte array.
     */
    public static byte[] convert( byte n ) {
        n = (byte)(n ^ ((byte)0x80)); // flip MSB because "byte" is signed
        return new byte[] { n };
    }

    /**
     * Convert a short into a byte array.
     */
    public static byte[] convert( short n ) {
        n = (short)(n ^ ((short)0x8000)); // flip MSB because "short" is signed
        byte[] key = new byte[2];
        pack2(key, 0, n);
        return key;
    }

    /**
     * Convert an int into a byte array.
     */
    public static byte[] convert(int n) {
        n = (n ^ 0x80000000); // flip MSB because "int" is signed
        byte[] key = new byte[4];
        pack4(key, 0, n);
        return key;
    }

    /**
     * Convert a long into a byte array.
     */
    public static byte[] convert( long n ) {
        n = (n ^ 0x8000000000000000L); // flip MSB because "long" is signed
        byte[] key = new byte[8];
        pack8( key, 0, n );
        return key;
    }


    static final void pack2( byte[] data, int offs, int val ) {
        data[offs++] = (byte)(val >> 8);
        data[offs++] = (byte)val;
    }


    static final void pack4( byte[] data, int offs, int val ) {
        data[offs++] = (byte)(val >> 24);
        data[offs++] = (byte)(val >> 16);
        data[offs++] = (byte)(val >> 8);
        data[offs++] = (byte)val;
    }


    static final void pack8( byte[] data, int offs, long val ) {
        pack4( data, 0, (int)(val >> 32) );
        pack4( data, 4, (int)val );
    }
}
