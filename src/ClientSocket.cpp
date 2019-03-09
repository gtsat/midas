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

#include "ClientSocket.h"
#include <iostream>
#include <stdexcept>

ClientSocket::ClientSocket ( std::string host, int port ) {
  if ( ! Socket::create() )
      throw std::runtime_error ( "Could not create client socket." );
  while (!Socket::connect ( host.c_str(), port ));
}

const ClientSocket& ClientSocket::operator << ( const std::string& s ) const {
	if ( ! Socket::send ( s ) )
		throw std::runtime_error ( "Could not write to socket." );
	return *this;
}

const ClientSocket& ClientSocket::operator >> ( std::string& s ) const {
	if ( ! Socket::recv (s) )
		throw std::runtime_error ( "Could not read from socket." );
	return *this;
}
