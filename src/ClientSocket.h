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

#ifndef ClientSocket_class
#define ClientSocket_class

#include "Socket.h"

class ClientSocket : public Socket {

	ClientSocket (Socket& cs) : Socket(cs) {}

public:

	ClientSocket() {}
	ClientSocket ( std::string, int );
	~ClientSocket(){};

	const ClientSocket& operator << ( const std::string& ) const;
	const ClientSocket& operator >> ( std::string& ) const;

	bool open () { return Socket::open();}
	void close () { return Socket::close();}
};

#endif
