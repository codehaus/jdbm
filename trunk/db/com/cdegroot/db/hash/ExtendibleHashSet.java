/*
 *  $Id: ExtendibleHashSet.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
 *
 *  Hash interface
 *
 *  Simple db toolkit.
 *  Copyright (C) 2000 Cees de Groot <cg@cdegroot.com>
 */
package com.cdegroot.db.hash;

import java.util.Set;
import java.util.Iterator;
import java.util.Collection;
import com.cdegroot.db.recman.RecordManager;

// @@PUBLIC_API@@
/**
 *  This class gives you the interface to a single hash in a file. Objects
 *  are serialized, stored through the {@link RecordManager} and made
 *  retrievable by storing the resulting recid in an extendible hash.
 */
public class ExtendibleHashSet implements Set {

    // Implementation of Set

    public boolean add(Object o) {
	throw new UnsupportedOperationException();
    }

    public boolean addAll(Collection c) {
	throw new UnsupportedOperationException();
    }
    
    public void clear() {
	throw new UnsupportedOperationException();
    }
    
    public boolean contains(Object o) {
	throw new UnsupportedOperationException();
    }
    
    public boolean containsAll(Collection c) {
	throw new UnsupportedOperationException();
    }
    
    public boolean isEmpty() {
	throw new UnsupportedOperationException();
    }
    
    public Iterator iterator() {
	throw new UnsupportedOperationException();
    }
    
    public boolean remove(Object o) {
	throw new UnsupportedOperationException();
    }
    
    public boolean removeAll(Collection c) {
	throw new UnsupportedOperationException();
    }
    
    public boolean retainAll(Collection c) {
	throw new UnsupportedOperationException();
    }
    
    public int size() {
	throw new UnsupportedOperationException();
    }
    
    public Object[] toArray() {
	throw new UnsupportedOperationException();
    }
    
    public Object[] toArray(Object[] a) {
	throw new UnsupportedOperationException();
    }
}
