/*
 *  $Id: BlockView.java,v 1.2 2000/04/11 06:08:08 boisvert Exp $
 *
 *  Base class for block views.
 *
 *  <one line to give the library's name and a brief idea of what it does.>
 *  Copyright (C) 2000 Cees de Groot <cg@cdegroot.com>
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
 *  This is a marker interface that is implemented by classes that
 *  interpret blocks of data by pretending to be an overlay.
 *
 *  @see BlockIo.setView()
 */
public interface BlockView {
}