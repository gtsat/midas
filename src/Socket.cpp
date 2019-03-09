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

#include "Socket.h"
#include "string.h"
#include <string.h>
#include <errno.h>
#include <fcntl.h>

#include <iostream>

extern bool ipv6;


Socket::Socket () : m_sock (-1) {
	memset ( &m_addr, 0, sizeof (m_addr) );
	set_non_blocking (false);
}

Socket::Socket (const Socket& sock){
	m_sock = sock.m_sock;
	m_addr = sock.m_addr;
	set_non_blocking (false);
}

Socket::~Socket () {
	if ( is_valid() )
		::close (m_sock);
}

bool Socket::create () {
	if (ipv6)
		m_sock = socket (AF_INET6, SOCK_STREAM, 0);
	else
		m_sock = socket (AF_INET, SOCK_STREAM, 0);

	if ( ! is_valid() )
		return false;

	// TIME_WAIT - argh
	int on = 1;
	if (ipv6) setsockopt (m_sock, IPPROTO_IPV6, IPV6_V6ONLY, (const char *)&on, sizeof(on));

	if (setsockopt (m_sock, SOL_SOCKET, SO_REUSEADDR, (const char*) &on, sizeof (on)) != 0) {
		perror ("create() failed at SO_REUSEADDR.");
		return false;
	}

	if (setsockopt (m_sock, SOL_SOCKET, SO_KEEPALIVE, (const char*) &on, sizeof (on)) != 0) {
		perror ("create() failed at SO_KEEPALIVE.");
		return false;
	}

	if (setsockopt (m_sock, SOL_SOCKET, SO_RCVBUF, (const char*) &RCVBUF, sizeof (RCVBUF)) != 0) {
		perror ("create() failed at SO_RCVBUF.");
		return false;
	}

	if (setsockopt (m_sock, SOL_SOCKET, SO_SNDBUF, (const char*) &SNDBUF, sizeof (SNDBUF)) != 0) {
		perror ("create() failed at SO_SNDBUF.");
		return false;
	}else
		return true;
}

bool Socket::bind (int p,  const char* h = 0) {
	int bind_return = 0;

	if (!is_valid())
		return false;

	if (ipv6) {
		m_addr.m_addr6.sin6_family = AF_INET6;
		m_addr.m_addr6.sin6_port = htons (p);

		if (h == 0)
			m_addr.m_addr6.sin6_addr = in6addr_any;
		else
			inet_pton (AF_INET6, h, &m_addr.m_addr6.sin6_addr);

		bind_return = ::bind (m_sock, (struct sockaddr*) &m_addr.m_addr6, sizeof (m_addr.m_addr6));
	}else{
		m_addr.m_addr.sin_family = AF_INET;
		m_addr.m_addr.sin_port = htons (p);

		if (h == 0)
			m_addr.m_addr.sin_addr.s_addr = INADDR_ANY;
		else
			inet_aton (h, &m_addr.m_addr.sin_addr);

		bind_return = ::bind (m_sock, (struct sockaddr*) &m_addr.m_addr, sizeof (m_addr));
	}

	if (bind_return == -1) {
		if (!ipv6)
			std::cerr << "\n** CRITICAL ERROR - Unable to bind " << inet_ntoa (m_addr.m_addr.sin_addr) << ":" << ntohs (m_addr.m_addr.sin_port) << ".\n";
		else
			std::cerr << "\n** CRITICAL ERROR - Unable to bind.\n";
		return false;
	}else
		return true;
}

bool Socket::connect (const char* h, const int p) {
	if (!is_valid())
		return false;

	if (ipv6) {
		m_addr.m_addr6.sin6_family = AF_INET6;
		m_addr.m_addr6.sin6_port = htons (p);

		if ( inet_pton (AF_INET6, h, &m_addr.m_addr6.sin6_addr) == 0 ) {
			perror ("inet_pton() failed");
			return false;
		}
	}else{
		m_addr.m_addr.sin_family = AF_INET;
		m_addr.m_addr.sin_port = htons (p);

		if ( inet_aton (h, &m_addr.m_addr.sin_addr) == 0 ) {
			perror ("inet_pton() failed");
			return false;
		}
	}

	if (errno == EAFNOSUPPORT)
		return false;

	if (::connect ( m_sock, (sockaddr*) &m_addr, sizeof (m_addr) ) < 0) {
		perror ("connect() failed");
		return false;
	}else
		return true;
}

bool Socket::listen () const {
	if (!is_valid())
		return false;

	int listen_return = ::listen (m_sock, MAXCONNECTIONS);

	if (listen_return == -1){
		if(!ipv6)
			std::cerr << "\n** CRITICAL ERROR - Unable to listen on " << inet_ntoa (m_addr.m_addr.sin_addr) << ":" << ntohs (m_addr.m_addr.sin_port) << ".\n";
		else
			std::cerr << "\n** CRITICAL ERROR - Unable to listen.\n";

		return false;
	}else
		return true;
}

bool Socket::accept (Socket& new_socket) const {
	int addr_length = sizeof (m_addr);
	new_socket.m_sock = ::accept (m_sock, (sockaddr*) &m_addr, (socklen_t*) &addr_length);

	if ( new_socket.m_sock <= 0 )
		return false;
	else
		return true;
}

bool Socket::send (const std::string s) const {
	int status = ::send (m_sock, s.c_str(), s.size(), 0);
	if (status == -1)
		return false;
	else
		return true;
}

int Socket::recv (std::string& s) const {
	char buf [ MAXRECV + 1 ];
	memset ( buf, '\0', MAXRECV + 1 );

	int status = ::recv (m_sock, buf, MAXRECV , 0);

  	if (status == -1){
		std::cout << "status == -1   errno == " << errno << "  in Socket::recv\n";
		return 0;
	}else if (status == 0){
		return 0;
	}else{
		s.clear();
		s = buf;
    		return status;
  	}
}

bool Socket::open () {
	if ( ! is_valid() )
		return false;

	if ( ::connect ( m_sock, ( sockaddr * ) &m_addr, sizeof ( m_addr ) ) < 0 ) {
		perror ("connect() failed");
		return false;
	}else
		return true;
}

void Socket::close () {
	if ( is_valid() )
		::close ( m_sock );
}

void Socket::set_non_blocking (const bool b) {
	int opts = fcntl ( m_sock, F_GETFL );

	if ( opts < 0 )
		return;

	if ( b )
		opts = ( opts | O_NONBLOCK );
	else
		opts = ( opts & ~O_NONBLOCK );

	fcntl ( m_sock, F_SETFL,opts );
}

