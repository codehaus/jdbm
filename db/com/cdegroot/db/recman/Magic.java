/*
 *  $Id: Magic.java,v 1.1 2000/04/03 12:13:48 cdegroot Exp $
 *
 *  Magic cookies
 *
 *  Simple db kit.
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

/**
 *  This interface contains magic cookies.
 */
interface Magic {
    /** Magic cookie at start of file */
    short FILE_HEADER = 0x1350;
  
    /** Magic for blocks. They're offset by the block type magic codes. */
    short BLOCK = 0x1351;
  
    /** Magics for blocks in certain lists. Offset by baseBlockMagic */
    short FREE_PAGE = 0;
    short USED_PAGE = 1;
    short TRANSLATION_PAGE = 2;
    short FREELOGIDS_PAGE = 3;
    short FREEPHYSIDS_PAGE = 4;
  
    /** Number of lists in a file */
    short NLISTS = 5;
  
    /** 
     *  Maximum number of blocks in a file, leaving room for a 16 bit
     *  offset encoded within a long.
     */
    long MAX_BLOCKS = 0x7FFFFFFFFFFFL;

    /** Magic for transaction file */
    short LOGFILE_HEADER = 0x1360;

    /** Size of an externalized short */
    int SZ_SHORT = 2;
    /** Size of an externalized int */
    int SZ_INT = 4;
    /** Size of an externalized long */
    int SZ_LONG = 8;
}
