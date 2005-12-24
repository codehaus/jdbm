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
 * Copyright 2000 (C) Cees de Groot. All Rights Reserved.
 * Copyright 2000-2001 (C) Alex Boisvert. All Rights Reserved.
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 * $Id: RecordManagerFactory.java,v 1.1 2002/05/31 06:33:20 boisvert Exp $
 */

package jdbm;

import java.io.IOException;
import java.util.Properties;

/**
 *  This class manages records, which are uninterpreted blobs of data. The
 *  set of operations is simple and straightforward: you communicate with
 *  the class using long "rowids" and byte[] data blocks. Rowids are returned
 *  on inserts and you can stash them away someplace safe to be able to get
 *  back to them. Data blocks can be as long as you wish, and may have
 *  lengths different from the original when updating.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @author <a href="cg@cdegroot.com">Cees de Groot</a>
 * @version $Id: RecordManagerFactory.java,v 1.1 2002/05/31 06:33:20 boisvert Exp $
 */
public final class RecordManagerFactory
{

    /**
     * Create a record manager.
     *
     * @param name Name of the record file.
     * @throw IOException if an I/O related exception occurs while creating
     *                    or opening the record manager.
     * @throw UnsupportedOperationException if some options are not supported by the
     *                                      implementation.
     * @throw IllegalArgumentException if some options are invalid.
     */
    public static RecordManager createRecordManager( String name )
        throws IOException
    {
        return createRecordManager( name, new Properties() );
    }


    /**
     * Create a record manager.
     *
     * @param name Name of the record file.
     * @param options Record manager options.
     * @throw IOException if an I/O related exception occurs while creating
     *                    or opening the record manager.
     * @throw UnsupportedOperationException if some options are not supported by the
     *                                      implementation.
     * @throw IllegalArgumentException if some options are invalid.
     */
    public static RecordManager createRecordManager( String name,
                                                     Properties options )
        throws IOException
    {
        String                 provider;
        Class                  clazz;
        RecordManagerProvider  factory;

        provider = options.getProperty( RecordManagerOptions.PROVIDER_FACTORY,
                                        "jdbm.recman.Provider" );

        try {
            clazz = Class.forName( provider );
            factory = (RecordManagerProvider) clazz.newInstance();
        } catch ( Exception except ) {
            throw new IllegalArgumentException( "Invalid record manager provider: "
                                                + provider
                                                + "\n[" + except.getClass().getName()
                                                + ": " + except.getMessage()
                                                + "]" );
        }
        return factory.createRecordManager( name, options );
    }

}