/*
 *  $Id: PageCursor.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
 *
 *  Cursor to follow lists of pages
 *
 *  <one line to give the library's name and a brief idea of what it does.>
 *  Copyright (C) 1999, 2000 Cees de Groot <cg@cdegroot.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Library General Public License 
 *  as published by the Free Software Foundation; either version 2 
 *  of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Library General Public License for more details.
 *
 *  You should have received a copy of the GNU Library General Public License 
 *  along with this library; if not, write to the Free Software Foundation, 
 *  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 */
package com.cdegroot.db.recman;

import java.io.IOException;

/**
 *  This class provides a cursor that can follow lists of pages
 *  bi-directionally.
 */
final class PageCursor {
    PageManager pageman;
    long current;
    short type;
    
    /**
     *  Constructs a page cursor that starts at the indicated block.
     */
    PageCursor(PageManager pageman, long current) {
	this.pageman = pageman;
	this.current = current;
    }
    
    /**
     *  Constructs a page cursor that starts at the first block
     *  of the indicated list.
     */
    PageCursor(PageManager pageman, short type) throws IOException {
	this.pageman = pageman;
	this.type = type;
    }
    
    /**
     *  Returns the current value of the cursor.
     */
    long getCurrent() throws IOException {
	return current;
    }

    /**
     *  Returns the next value of the cursor
     */
    long next() throws IOException {
	if (current == 0)
	    current = pageman.getFirst(type);
	else
	    current = pageman.getNext(current);
	return current;
    }	

    /**
     *  Returns the previous value of the cursor
     */
    long prev() throws IOException {
	current = pageman.getPrev(current);
	return current;
    }	
}
