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
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 * $Id: ObjectCache.java,v 1.2 2000/05/24 18:25:47 boisvert Exp $
 */

package jdbm.helper;

import jdbm.recman.RecordCache;
import jdbm.recman.RecordManager;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;

/**
 *  ObjectCache is a data container which caches de-serialized objects
 *  of a RecordManager.
 *  <p>
 *  It synchronizes its data with a RecordManager using the RecordCache
 *  interface.
 *
 *  @author <a href="mailto:boisvert@exoffice.com>Alex Boisvert</a>
 *  @version $Id: ObjectCache.java,v 1.2 2000/05/24 18:25:47 boisvert Exp $
 */
public class ObjectCache implements RecordCache, CachePolicyListener {

    /** RecordManager from which data is cache */
    RecordManager _recman;

    /** Policy for the cache */
    CachePolicy _policy;


    /**
     * Construct an ObjectCache linked to a given RecordManager and
     * using a given CachePolicy.
     */
    public ObjectCache(RecordManager recman, CachePolicy policy) {
        _recman = recman;
        _recman.addCache(this);
        _policy = policy;
        _policy.addListener(this);
    }


    /**
     * Fetch an object from the cache
     *
     * @arg recid Record id of the record to fetch
     */
    public Object fetchObject(long recid) 
    throws IOException, ClassNotFoundException {
        if (recid == 0) {
            throw new IllegalArgumentException("recid 0 is reserved.");
        }
        Long id = new Long(recid);
        RecordEntry entry = (RecordEntry)_policy.get(id);
        if (entry == null) {
            Object obj = _recman.fetchObject(recid);
            if (obj == null) {
                throw new Error("Persistent object cannot be null");
            }
            entry = new RecordEntry(new Long(recid), obj);
            try {
                _policy.put(entry.getRecid(), entry);
            } catch (CacheEvictionException cee) {
                Exception nested = cee.getNestedException();
                if (nested instanceof IOException) {
                    throw (IOException)nested;
                } else {
                    if (nested != null) {
                        nested.printStackTrace();
                    }
                    cee.printStackTrace();
                    throw new Error("Unhandled CacheEvictionException");
                }
            }
        }
        return entry.getValue();
    }


    /**
     * Update an object in the cache.
     *
     * WARNING:  If you "update" the same object on two different "recids"
     *           you will get the same value for both, even if you change
     *           the object's state in between the two calls.
     *
     * @arg obj Object to write back into RecordManager
     */
    public void update(long recid, Object obj) throws IOException {
        if (recid == 0) {
            throw new IllegalArgumentException("recid 0 is reserved.");
        }
        if (obj == null) {
            throw new IllegalArgumentException("Persistent object cannot be null");
        }
        Long id = new Long(recid);
        RecordEntry entry = (RecordEntry)_policy.get(id);
        if (entry == null) {
            entry = new RecordEntry(new Long(recid), obj);
            try {
                _policy.put(entry.getRecid(), entry);
            } catch (CacheEvictionException cee) {
                Exception nested = cee.getNestedException();
                if (nested instanceof IOException) {
                    throw (IOException)nested;
                } else {
                    if (nested != null) {
                        nested.printStackTrace();
                    }
                    cee.printStackTrace();
                    throw new Error("Unhandled CacheEvictionException");
                }
            }
        }
        entry.setDirty(true);
    }


    /**
     * [RecordCache interface implementation].
     * Notification to flush content related to a given record.
     */
    public void flush(long recid) throws IOException {
        Long id = new Long(recid);
        RecordEntry entry = (RecordEntry)_policy.get(id);
        if ((entry != null) && entry.isDirty()) {
            _recman.update(recid, entry);
            entry.setDirty(false);
        }
    }


    /**
     * [RecordCache interface implementation].
     * Notification to flush data all of records.
     */
    public void flushAll() throws IOException {
        Enumeration enum = _policy.elements();
        while (enum.hasMoreElements()) {
            RecordEntry entry = (RecordEntry)enum.nextElement();
            if (entry.isDirty()) {
                _recman.update(entry.getRecid().longValue(), entry);
                entry.setDirty(false);
            }
        }
    }


    /**
     * [RecordCache interface implementation].
     * Notification to invalidate content related to given record.
     */
    public void invalidate(long recid) {
        Long id = new Long(recid);
        _policy.remove(id);
    }


    /**
     * [RecordCache interface implementation].
     * Notification to invalidate content of all records.
     */
    public void invalidateAll() {
        _policy.removeAll();
    }


    /**
     * Dispose of any resource used by the cache.
     */
    public void dispose() {
        _recman.removeCache(this);
        _recman = null;
        _policy.removeListener(this);
        _policy = null;
    }

    /**
     * [Implementation of CachePolicyListener interface]
     * Notification that cache is evicting an object
     *
     * @arg obj object evited from cache
     */
    public void cacheObjectEvicted(Object obj) throws CacheEvictionException {
        RecordEntry entry = (RecordEntry)obj;
        if (entry.isDirty()) {
            try {
                _recman.update(entry.getRecid().longValue(), entry.getValue());
            } catch (IOException ioe) {
                throw new CacheEvictionException(ioe);
            }
            entry.setDirty(false);
        }
    }

}

/**
 * State information for record entries.
 */
class RecordEntry {
    private Long _recid;
    private Object _value;
    private boolean _dirty;

    RecordEntry(Long recid, Object obj) {
        _recid = recid;
        _value = obj;
    }

    Long getRecid() {
        return _recid;
    }

    boolean isDirty() {
        return _dirty;
    }

    void setDirty(boolean dirty) {
        _dirty = dirty;
    }

    Object getValue() {
        return _value;
    }

    void setValue(Object obj) {
        _value = obj;
    }
}

