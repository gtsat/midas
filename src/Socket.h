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

#ifndef Socket_class
#define Socket_class

#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <netdb.h>
#include <string>


const int MAXHOSTNAME = 128;
const int MAXCONNECTIONS = 8;
const int MAXRECV = 1023;

const int RCVBUF = 1024;
const int SNDBUF = 1024;


class Socket {
	int m_sock;

	union m_addr_t {
		sockaddr_in6 m_addr6;
		sockaddr_in m_addr;
	} m_addr;

public:
	Socket ();
	Socket (const Socket&);
	~Socket ();

	// Server initialization
	bool create ();
	bool bind ( int, const char* );
	bool listen () const;
	bool accept ( Socket& ) const;

	// Client initialization
	bool connect ( const char*, const int );

	bool open ();
	void close ();

	// Data Transmission
	bool send ( const std::string ) const;
	int recv ( std::string& ) const;

	void set_non_blocking ( const bool );

	bool is_valid() const { return m_sock != -1; }
};

#endif
