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

#include "ServerSocket.h"
#include <iostream>
#include <stdexcept>

ServerSocket::ServerSocket (const char* h, int p)  {
	Socket::create ( );
	Socket::bind ( p, h );
	Socket::listen ( );
}

const ServerSocket& ServerSocket::operator << ( const std::string& s ) const {
	//if ( ! Socket::send ( htonl( s.size() ) ) )
	//	throw std::runtime_error ( "Could not write to socket." );

	if ( ! Socket::send ( s ) )
		throw std::runtime_error ( "Could not write to socket." );

	return *this;
}

const ServerSocket& ServerSocket::operator >> ( std::string& s ) const {
	/*
	long size(0), temp(0);
	if ( ! Socket::recv ( temp ) )
		throw std::runtime_error ( "Could not read from socket." );
	size = ntohl ( temp );

	printf ( "MESSAGE SIZE: %ld\n", size );
	*/
	if ( ! Socket::recv ( s ) )
		throw std::runtime_error ( "Could not read from socket." );

	//printf ( "MESSAGE: %s\n", s.c_str() );

	return *this;
}

void ServerSocket::accept(ServerSocket &sock){
	Socket::accept ( sock );
}
