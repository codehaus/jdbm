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
 * Contributions are (C) Copyright 2000 by their associated contributors.
 *
 * $Id: JDBMHashtable.java,v 1.3 2001/06/02 14:30:04 boisvert Exp $
 */

package jdbm;

import java.io.IOException;

/**
 * This interface represents a persistent hashtable, which maps keys to
 * values.  Any non-<code>null</code> can be used as a key or value.
 *
 * Just like <code>java.util.Hashtable</code>, to successfully store
 * and retrieve objects from a hashtable, the objects used as keys
 * must implement the <code>hashCode()<code> method and the
 * <code>equals()</code> method.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: JDBMHashtable.java,v 1.3 2001/06/02 14:30:04 boisvert Exp $
 */
public interface JDBMHashtable {

    /**
     * Associates the specified value with the specified key.
     *
     * @arg key key with which the specified value is to be assocated.
     * @arg value value to be associated with the specified key.
     */
    public void put(Object key, Object value) throws IOException;

    /**
     * Returns the value which is associated with the given key. Returns
     * <code>null</code> if there is not association for this key.
     *
     * @arg key key whose associated value is to be returned
     */
    public Object get(Object key) throws IOException;

    /**
     * Remove the value which is associated with the given key.  If the
     * key does not exist, this method simply ignores the operation.
     *
     * @arg key key whose associated value is to be removed
     */
    public void remove(Object key) throws IOException;

    /**
     * Returns an enumeration of the keys contained in this hashtable.
     */
    public JDBMEnumeration keys() throws IOException;


    /**
     * Returns an enumeration of the values contained in this hashtable.
     */
    public JDBMEnumeration values() throws IOException;

    /**
     * Disposes from this hashtable instance and frees associated
     * resources.
     */
    public void dispose() throws IOException;

    /**
     * Get the persistent record identifier of this hashtable.
     */
    public long getRecid();

}
