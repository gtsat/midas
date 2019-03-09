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

#ifndef NODE_H_
#define NODE_H_

#include "ServerSocket.h"
#include "ClientSocket.h"
#include "Pool.h"
#include <pthread.h>
#include <sched.h>
#include <map>

template<class T> class Node {

	std::string host;
	int port;

	int dims;

	/* split history */
	std::vector<bool> hist;

	/* split points */
	std::vector<T> pts;

	/* data pool */
	Pool<T> pool;

	//pthread_mutex_t pool_lock;


	/* link to a peer of the opposite side for each split. */
	std::vector<std::string> frontlink_hosts;
	std::vector<int> frontlink_ports;

	//pthread_mutex_t backlink_lock;

	/* serving connected node clients with threads */
	std::vector<pthread_t*> server_threads;
	std::vector<ClientSocket*> skip;
	std::vector<Node<T>*> cached;

public:

	Node (std::string &hst, int prt,
			int dms, T const* lo, T const* hi)
		: host (hst), port (prt), dims(dms), pool (dms,lo,hi) {
		//pthread_mutex_init (&pool_lock, 0);
		//pthread_mutex_init (&backlink_lock, 0);
	}

	~Node () {
		unlink();
		//pthread_mutex_destroy (&pool_lock);
		//pthread_mutex_destroy (&backlink_lock);
	}

	/* node construction from migration protocol */
	Node (std::string& hst,int prt,int dms,std::string& msg);
	Node (int dms,std::string& msg);

	void initialize (std::string&);

	/* node server functionality */
	void serve ();
	void handle (ServerSocket& sock);

	int process_async_msg (std::string&);

private:

	/* return index of the most relevant link */
	int forward_to (T key[]) const;
	int forward_cache (T key[]) const;

	/* client interface implementation */

	/*** asynchronous ***/
	int process_insert_msg (std::string&);
	int process_append_msg (std::string&);
	int process_lookup_msg (std::string&);
	int process_range_msg (std::string&);
	int process_nearest_msg (std::string&);

	/*** synchronous ***/
	int process_merge_msg (std::string&);

public:

	/* overlay functionality */
	Node& split ();

	int merge_lo (Node&);
	int merge_hi (Node&);

	int depart();

	std::string marshalize (bool print_data) const;

	/* returns distributed binary search tree path in {0,1}* */
	std::string get_id () const;

	unsigned get_data_load () const {return pool.get_size();}
	double get_volume_load () const;

	T* get_lo () const {return pool.get_lo();}
	T* get_hi () const {return pool.get_hi();}

	void container_links_increment ();
	void container_links_decrement ();

	/* connectivity functionality */
	void unlink ();
	void link ();

	std::string get_host () const {return host;}
	int get_port () const {return port;}
};

#endif
