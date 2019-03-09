/*********************************************************************
 * Copyright (C) George C. Tsatsanifos 2008 <gtsatsanifos@gmail.com>
 *
 * This file is part of MIDAS.
 *
 * MIDAS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MIDAS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MIDAS.  If not, see <http://www.gnu.org/licenses/>.
 *********************************************************************/

#ifndef COMMONS_H_
#define COMMONS_H_

#define THREAD_STACK_SIZE 131072

#include <iostream>
#include "ServerSocket.h"

template<class T> static void* serve (void* n) {
	static_cast <T*> (n) -> serve();
	return 0;
}

template<class T> void* handle (void* args) {
	std::pair<T*, ServerSocket*>* tuple = static_cast<std::pair<T*, ServerSocket*>*> (args);
	tuple->first->handle ( *tuple->second );
	return 0;
}

#endif
