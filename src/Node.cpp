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

#include <sys/time.h>
#include <stdexcept>
#include <typeinfo>
#include <algorithm>
#include <sstream>
#include <cassert>
#include <climits>
#include <cfloat>

#include "common.h"
#include "Node.h"
#include "Pool.cpp"

//#define __TIMING__
#define MIN(a,b) (a)<(b)?(a):(b)

template<class T> static void stream2vec (std::istream& in,
					void* vec,
					int dims,
					char delimiter) {
	for (int j = 0; j < dims; ++j) {
		if (typeid (T) == typeid (char)){
			char temp;
			in >> temp;
			reinterpret_cast<char*> (vec)[j] = temp;
		}else if (typeid (T) == typeid (short int)){
			short int temp;
			in >> temp;
			reinterpret_cast<short int*> (vec)[j] = temp;
		}else if (typeid (T) == typeid (unsigned short int)){
			unsigned short int temp;
			in >> temp;
			reinterpret_cast<unsigned short int*> (vec)[j] = temp;
		}else if (typeid (T) == typeid (int)){
			int temp;
			in >> temp;
			reinterpret_cast<int*> (vec)[j] = temp;
		}else if (typeid (T) == typeid (unsigned int)){
			unsigned int temp;
			in >> temp;
			reinterpret_cast<unsigned int*> (vec)[j] = temp;
		}else if (typeid (T) == typeid (long int)){
			long int temp;
			in >> temp;
			reinterpret_cast<long int*> (vec)[j] = temp;
		}else if (typeid (T) == typeid (unsigned long int)){
			unsigned long int temp;
			in >> temp;
			reinterpret_cast<unsigned long int*> (vec)[j] = temp;
		}else if (typeid (T) == typeid (float)){
			float temp;
			in >> temp;
			reinterpret_cast<float*> (vec)[j] = temp;
		}else if (typeid (T) == typeid (double)){
			double temp;
			in >> temp;
			reinterpret_cast<double*> (vec)[j] = temp;
		}else
			throw std::runtime_error("** CRITICAL ERROR - Unexpected index type used.");

		if (delimiter != '\n' && delimiter != '\r' && delimiter != '\t' && delimiter != ' ') {
			if (j < dims - 1) {
				char symbol;
				in >> symbol;

				if (symbol != delimiter)
					throw std::runtime_error(" Bad request message. Expected "+delimiter);
			}
		}
	}
}

template<class T> static void vec2stream (std::ostream& out,
					void* vec,
					int dims,
					char delimiter) {
	for (int j = 0; j < dims; ++j) {
		if (typeid(T) == typeid(char))
			out << reinterpret_cast<char*> (vec)[j];
		else if (typeid(T) == typeid(short int))
			out << reinterpret_cast<short int*> (vec)[j];
		else if (typeid(T) == typeid(unsigned short int))
			out << reinterpret_cast<unsigned short int*> (vec)[j];
		else if (typeid(T) == typeid(int))
			out << reinterpret_cast<int*> (vec)[j];
		else if (typeid(T) == typeid(unsigned int))
			out << reinterpret_cast<unsigned int*> (vec)[j];
		else if (typeid(T) == typeid(long int))
			out << reinterpret_cast<long int*> (vec)[j];
		else if (typeid(T) == typeid(unsigned long int))
			out << reinterpret_cast<unsigned long int*> (vec)[j];
		else if (typeid(T) == typeid(float))
			out << reinterpret_cast<float*> (vec)[j];
		else if (typeid(T) == typeid(double))
			out << reinterpret_cast<double*> (vec)[j];
		else
			throw std::runtime_error("** CRITICAL ERROR - Unexpected index type.");

		if (j < dims - 1)
			out << delimiter;
	}
}

template<class T> Node<T>::Node (int dms,std::string& msg) : dims(dms), pool(dms) {
	initialize(msg);
}

template<class T> Node<T>::Node (std::string& hst,
				int prt,
				int dms,
				std::string& msg)
				: dims(dms), pool(dms) {
	initialize(msg);
	host = hst;
	port = prt;
}

template<class T> void Node<T>::initialize (std::string &msg) {
	if (pool.get_size() > 0)
		return;

	std::cerr << "** Initialization of new node begins.\nArea: (";
	std::stringstream in (msg, std::stringstream::in);

	/* consume #AREA header */
	std::string junk;
	in >> junk;
	assert ( junk.compare ("#AREA") == 0 );

	T lo [dims];
	::stream2vec<T> (in, lo, dims, '\n');
	::vec2stream<T> (std::cerr, lo, dims, ',');
	std::cerr << "),(";

	T hi [dims];
	::stream2vec<T> ( in, hi, dims, '\n' );
	::vec2stream<T> (std::cerr, hi, dims, ',');
	std::cerr << ")\n";

	pool.set_lo (lo);
	pool.set_hi (hi);

	/* consume #NODE header */
	in >> junk;

	assert ( junk.compare ("#NODE") == 0 );

	std::string initial_host;
	int initial_port;
	in >> initial_host;
	in >> initial_port;

	host = initial_host;
	port = initial_port;

	std::string id;
	in >> id;

	std::cerr << "ID: "<< id << "\n";

	for (std::string::const_iterator si = id.begin(); si != id.end(); ++si)
		if (*si == '1')
			hist.push_back(true);
		else if (*si == '0')
			hist.push_back(false);
		else
			throw std::runtime_error("** ERROR - Invalid split history at reconstruction protocol.\n");

	T temp [hist.size()];
	::stream2vec<T> ( in, temp, hist.size(), '\n');

	for (unsigned j = 0; j < hist.size(); ++j)
		pts.push_back ( temp [j] );

	for (unsigned j = 0; j < hist.size(); ++j) {
		std::string link_host;
		in >> link_host;

		int link_port;
		in >> link_port;

		frontlink_hosts.push_back(link_host);
		frontlink_ports.push_back(link_port);
	}

	in >> junk;
	if ( junk.compare ("#TUPLES") == 0 ) {
		int quantity = 0;
		in >> quantity;
		assert (quantity >= 0);

		std::cerr << "** "<< id <<"@" << port << " is loading " << quantity << " tuples.\n";

		for (int j = 0; j < quantity; ++j) {
			T* key = new T [dims];

			::stream2vec<T> (in, key, dims, ',');
			::vec2stream<T> (std::cerr, key, dims, ',');
			std::cerr << std::endl;

			std::string value;
			in >> value;

			char* buffer = new char [value.size() + 1];
			strcpy(buffer, value.c_str());

			pool.push(key, buffer);
		}
	}
}

template<class T> void Node<T>::serve () {
	ServerSocket ss(host.c_str(), port);

	std::cerr<<"** "<<get_id()<<" is waiting for connections at "<<host<<":"<<port<<"\n";

	pthread_attr_t attr;
	pthread_attr_init (&attr);
	pthread_attr_setdetachstate (&attr, PTHREAD_CREATE_DETACHED );
	pthread_attr_setstacksize (&attr, THREAD_STACK_SIZE );

	while (true) {
		try {
			ServerSocket* new_sock = new ServerSocket;
			ss.accept(*new_sock);

			pthread_t* thread = new pthread_t;
			std::pair<Node<T>*, ServerSocket*> *args = new std::pair<Node<T>*,
					ServerSocket*>(this, new_sock);

			if (pthread_create(thread, &attr, ::handle<Node<T> >, static_cast<void*> (args)) != 0)
				throw std::runtime_error("** ERROR - Unable to create a new thread.");

			server_threads.push_back(thread);
		} catch (std::exception &e) {
			std::cerr << "** " << get_id() << "@" << port << " Server has caught an exception.\n";
		}
	}
}

template<class T> void Node<T>::handle ( ServerSocket& sock ) {
	while (true){
		try{
			/**
			 * process client messages
			 */
			std::string data;
			sock >> data;

data_processing:
			if (data.find("#ACK") != std::string::npos) {
				/* get response message */
				std::string answer;
				answer = data;
				while (answer.find("#END") == std::string::npos) {
					std::string new_data;
					sock >> new_data;
					answer += new_data;
				}
				answer += data.substr(0, data.find("#END")+5);

				std::cerr<<"** "<<get_id()<<"@"<<port<<" received answer:\n"<<answer<<"\n";
				//data.clear();
				answer.clear();
			}else{
				unsigned pos = data.find ('\n');
				if (pos != std::string::npos) {
					std::string msg;
					msg = data.substr (0, pos+1);
					data = data.substr (pos+1, data.size()-pos+1);

					if (msg.size() == 0)
						continue;

					char symbol = msg.at (0);

					if (symbol == 'W'){
						sock << marshalize(false);
					}else if (symbol == 'M'){
						sock << marshalize(true);
					}else if (symbol == 'O'){
						msg += data;
						while (msg.find ("#END\n") == std::string::npos) {
							std::string temp;
							sock >> temp;
							msg += temp;
						}

						unsigned sender_id_end_pos = msg.find ("\n");
						std::string sender_id = msg.substr(2,sender_id_end_pos);

						std::cerr << "** " << get_id() << "@" << port << " Received overlay maintenance message from node " << sender_id << ".\n";

						unsigned msg_end_pos = msg.find ("#END\n") + 4;
						std::string current;
						current = msg.substr(sender_id_end_pos+1,msg_end_pos+1);

						data.clear();
						if (msg_end_pos + 1 <= msg.size ()) {
							data = msg.substr(msg_end_pos+1,msg.size()-msg_end_pos);
						}

						Node<T> marshalized (dims,current);

						unsigned lcp_length = 0;
						for (std::vector<bool>::const_iterator vi=hist.begin(); vi!=hist.end(); ++vi) {
							if ((*vi && marshalized.get_id().at(lcp_length) == '1')
							|| (!*vi && marshalized.get_id().at(lcp_length) == '0')) {
								++lcp_length;
							}else{
								break;
							}
						}

						std::cerr << "** " << get_id() << "@" << port << " Substituting skip-link #" << lcp_length
								<< " from " << frontlink_hosts[lcp_length] << ":" << frontlink_ports[lcp_length]
								<< " to " << marshalized.host << ":" << marshalized.port << ".\n";

						frontlink_hosts[lcp_length] = marshalized.host;
						frontlink_ports[lcp_length] = marshalized.port;
						skip [lcp_length] = new ClientSocket (marshalized.host,marshalized.port);

						lcp_length = 0;
						for (std::vector<bool>::const_iterator vi=hist.begin(); vi!=hist.end(); ++vi) {
							if (*vi == sender_id.at(lcp_length)) {
								++lcp_length;
							}else{
								break;
							}
						}

						for (unsigned i=lcp_length+2; i<skip.size(); ++i) {
							try{
								std::string response;
								std::string link_id;
								for (unsigned j=0; j<i; ++j) {
									link_id.push_back (hist.at(j) ? '1' : '0');
								}
								link_id.push_back (hist.at(i) ? '0' : '1');

								std::cerr << "** " << get_id() << "@" << port << " Forwarding maintenance message to link#" << i << " with id " << link_id << "X.";

								*skip.at(i) << "O " << get_id() << "\n" << current;
								*skip.at(i) >> response;

								if (response.compare("O OK\n") != 0){
									std::cerr << "** " << get_id() << "@" << port << " Failed response from overlay maintenance message: " << msg;
								}
							}catch(std::exception &e){
								std::cerr << "** " << get_id() << "@" << port << " Removing skip-link #" << i << "\n";
								skip [i] = 0;
							}
						}
						sock << "O OK\n";
					}else if (symbol == 'Q'){
						std::cerr << "** " << get_id() << "@" << port
							<< " now quitting...\n";
						depart();
						unlink();
						sock << "Q OK\n";
						//pthread_exit(0);
						exit(0);
					}else if (symbol == 'G'){
						msg += data;
						while (msg.find ("#END\n") == std::string::npos) {
							std::string temp;
							sock >> temp;
							msg += temp;
						}

						unsigned msg_end_pos = msg.find ("#END\n") + 4;
						std::string current;
						current = msg.substr(0,msg_end_pos+1);

						data.clear();
						if (msg_end_pos + 1 <= msg.size ()) {
							data = msg.substr(msg_end_pos+1,msg.size()-msg_end_pos);
						}

						if (process_merge_msg (current) == 0) {
							sock << "G OK\n";
						}else{
							sock << "G BAD\n";
						}
					}else if (symbol == 'S') {
						std::stringstream in (msg,std::stringstream::in);

						std::string remote_host;
						std::string remote_port;

						in >> symbol;
						in >> remote_host;
						in >> remote_port;

						if (cached.empty()) {
							Node& sibling = split();
							sibling.host = remote_host;
							sibling.port = std::atoi(remote_port.c_str());

							sock << sibling.marshalize(true);

							std::string response;
							sock >> response;

							if (response.compare("S OK\n") != 0) {
								if (hist.back()) merge_lo (sibling);
								else merge_hi (sibling);
							}else{
								frontlink_hosts.back() = remote_host;
								frontlink_ports.back() = std::atoi(remote_port.c_str());
								link();
							}
							delete &sibling;
						}else{
							Node<T>* stray = cached.back();
							stray->host = remote_host;
							stray->port = std::atoi(remote_port.c_str());

							sock << stray->marshalize(true);
							std::string response;
							sock >> response;
							if (response.compare("S OK\n") == 0) {
								cached.pop_back();
								delete stray;
							}
						}
					}else{
						std::string response;
						response += symbol;
						if (process_async_msg(msg)<0) response += " BAD\n";
						else response += " OK\n";
						sock << response;
					}
					goto data_processing;
				}else{
					std::string new_data;
					sock >> new_data;
					data += new_data;
					goto data_processing;
				}
			}
		}catch (std::exception &e){
			if (hist.empty())
				pthread_exit (0);

#ifdef __TIMING__
			timeval tim;
			gettimeofday (&tim, NULL);
			double t1 = tim.tv_sec + (tim.tv_usec/1000000.0);
#endif

			std::cerr << "** " << get_id() << "@" << port << " Handler has caught an exception. (" << e.what() << ")\n";
			unsigned i = 0;
			for (std::vector<ClientSocket*>::iterator vi=skip.begin(); vi!=skip.end(); ++vi) {
				try{
					if ((*vi)->is_valid()) {
						std::string whois_response;
						*skip.at(i) << "W\n";
						*skip.at(i) >> whois_response;
						std::cerr << "** " << get_id() << "@" << port << " Confirmed status of skip-link #" << i << "\n";
					}
				}catch(std::exception &e){
					std::cerr << "** " << get_id() << "@" << port << " Removing skip-link #" << i << "\n";
					*vi = 0;
				}
				++i;
			}

#ifdef __TIMING__
				gettimeofday (&tim, NULL);
				double t2 = tim.tv_sec + (tim.tv_usec/1000000.0);

				std::cerr << "SELF-ACTIVATION RESPONSE TIME: " << t2-t1 << " seconds elapsed.\n";
#endif

			pthread_exit (0);
		}
	}
}

template<class T> int Node<T>::process_async_msg ( std::string& msg ) {
	char req_type = msg.at (0);

	switch (req_type){

	/* update (key,value) pair */
	case 'U':
		return process_insert_msg (msg);

	/* update (key,old_value+new_value) pair */
	case 'A':
		return process_append_msg (msg);

	/* lookup key */
	case 'L':
		return process_lookup_msg (msg);

	/* lookup key range */
	case 'R':
		return process_range_msg (msg);

	/* nearest neighbor request */
	case 'N':
		return process_nearest_msg (msg);

	default:
		std::cerr << "** " << get_id() << "@" << port
			<< " met unknown message format.\n-- UNKNOWN FORMAT START --\n"
			<< msg << "\n-- UNKNOWN FORMAT END --\n";
		throw std::runtime_error("Bad message. Unable to process.");
	}
	return -1;
}

/**
 * U(.5,.5)dummy_value_string\n
 */
template<class T> int Node<T>::process_insert_msg ( std::string& msg ) {
	std::stringstream in (msg, std::stringstream::in);

	char symbol;
	in >> symbol;
	if (symbol != 'U')
		throw std::runtime_error (" Bad update query header.\n");

	in >> symbol;
	if (symbol != '(')
		throw std::runtime_error (" Bad update request message. Opening parenthesis expected.\n");

	T *key = new T [dims];
	::stream2vec<T> (in, key, dims, ',');

	in >> symbol;
	if (symbol != ')')
		throw std::runtime_error (" Bad update request message. Closing parenthesis expected.\n");

	std::string value = msg.substr (in.tellg(), msg.size()-in.tellg()-1);

	if (!pool.isRelevant(key)){
		int dest_link = forward_to(key);

		if (dest_link == -1)
			throw std::runtime_error(" Unable to forward update request.\n");

		try{
			if (skip.at(dest_link) == 0) {
				for (typename std::vector<Node<T>*>::const_iterator vi=cached.begin(); vi!=cached.end(); ++vi) {
					if ((*vi)->pool.isRelevant(key)) {
						std::cerr << "** " << (*vi)->get_id() << "@" << (*vi)->port << " is indexing in cached node value '" << value << "' by key ";
						::vec2stream<T>(std::cerr,key,dims,',');
						std::cerr << "\n";

						char* buffer = (char*) malloc (value.size() + 1);
						strcpy (buffer, value.c_str());

						//pthread_mutex_lock (&(*vi)->pool_lock);
						(*vi)->pool.update(key, buffer);
						//pthread_mutex_unlock (&(*vi)->pool_lock);
						return 0;
					}
				}
			}else{
				//skip.at(dest_link)->open();
				*skip.at(dest_link) << msg;

				std::string response;
				*skip.at(dest_link) >> response;

				if (response.compare("U OK\n") != 0){
					std::cerr << "** " << get_id() << "@" << port << " failed to forward message: " << msg;
					return -1;
				}
			}
		}catch (std::exception &e){
			std::cerr << "** " << get_id() << "@" << port << " traced that link#" << dest_link << " has failed.\n";
			skip.at(dest_link) = 0;
			return -1;
		}
		std::cerr << "** Server@" << port << " forwarding to link#" << dest_link << " message " << msg;
		return 1;
	}else{
		std::cerr << "** " << get_id() << "@" << port << " is indexing locally value '" << value << "' by key ";
		::vec2stream<T>(std::cerr,key,dims,',');
		std::cerr << "\n";

		char* buffer = (char*) malloc (value.size() + 1);
		strcpy (buffer, value.c_str());

//		pthread_mutex_lock (&pool_lock);
		pool.update(key, buffer);
//		pthread_mutex_unlock (&pool_lock);
		return 0;
	}
}

/**
 * A(.5,.5)dummy_value_string\n
 */
template<class T> int Node<T>::process_append_msg ( std::string& msg ) {
	std::stringstream in(msg, std::stringstream::in);

	char symbol;
	in >> symbol;
	if (symbol != 'A')
		throw std::runtime_error(" Bad append request header.\n");

	in >> symbol;
	if (symbol != '(')
		throw std::runtime_error(" Bad append request message. Opening parenthesis expected.\n");

	T* key = new T[dims];
	::stream2vec<T> ( in, key, dims, ',' );

	in >> symbol;
	if ( symbol != ')' )
		throw std::runtime_error (" Bad append request message. Closing parenthesis expected.\n");

	std::string value = msg.substr(in.tellg(), msg.size() - in.tellg() - 1);

	if (!pool.isRelevant(key)) {
		int dest_link = forward_to(key);

		if (dest_link == -1)
			throw std::runtime_error(" Unable to forward update request.\n");

		try{
			if (skip.at(dest_link) == 0) {
				for (typename std::vector<Node<T>*>::const_iterator vi=cached.begin(); vi!=cached.end(); ++vi) {
					if ((*vi)->pool.isRelevant(key)) {
						std::cerr << "** " << (*vi)->get_id() << "@" << (*vi)->port << " is appending in cache locally value: " << value << "\n";

						char *buffer = new char[value.size() + 1];
						strcpy(buffer, value.c_str());

						//pthread_mutex_lock (&pool_lock);
						(*vi)->pool.concatenate(key, buffer);
						//pthread_mutex_unlock (&pool_lock);
						return 0;
					}
				}
			}else{
				//skip.at(dest_link)->open();
				*skip.at(dest_link) << msg;

				std::string response;
				*skip.at(dest_link) >> response;

				if (response.compare("A OK\n") != 0){
					std::cerr << "** " << get_id() << "@" << port << " failed to forward message: " << msg;
					return -1;
				}
			}
		}catch (std::exception &e){
			std::cerr << "** " << get_id() << "@" << port << " traced that link#" << dest_link << " has failed.\n";
			skip.at(dest_link) = 0;
			return -1;
		}
		std::cerr << "** Server@" << port << " forwarding to link#" << dest_link << " message " << msg;
		return 1;
	}else{
		std::cerr << "** " << get_id() << "@" << port << " is appending indexed locally value: " << value << "\n";

		char *buffer = new char[value.size() + 1];
		strcpy(buffer, value.c_str());

		//pthread_mutex_lock (&pool_lock);
		pool.concatenate(key, buffer);
		//pthread_mutex_unlock (&pool_lock);
		return 0;
	}
}

/**
 * L(.5,.5) 127.0.0.1 50000 0\n
 */
template<class T> int Node<T>::process_lookup_msg ( std::string& msg ) {
	std::stringstream in (msg, std::stringstream::in);

	char symbol;
	in >> symbol;
	if (symbol != 'L')
		throw std::runtime_error(" Bad lookup query header.\n");

	in >> symbol;
	if (symbol != '(')
		throw std::runtime_error(" Bad lookup request message. Opening parenthesis expected.\n");

	T key[dims];
	::stream2vec<T> (in, key, dims, ',');

	in >> symbol;
	if (symbol != ')')
		throw std::runtime_error(" Bad lookup request message. Closing parenthesis expected.\n");

	std::string dest_host;
	in >> dest_host;

	int dest_port(0);
	in >> dest_port;

	int hops(0);
	in >> hops;
	++hops;

	if (!pool.isRelevant(key)) {
		int dest_link = forward_to(key);

		if (dest_link == -1)
			throw std::runtime_error(" Unable to forward lookup request.\n");

		if (skip.at(dest_link) == 0) {
			for (typename std::vector<Node<T>*>::const_iterator vi=cached.begin(); vi!=cached.end(); ++vi) {
				if ((*vi)->pool.isRelevant(key)) {
					std::pair<T*, char*>& tuple = (*vi)->pool.lookup(key);

					std::cerr << "** " << (*vi)->get_id() << "@" << (*vi)->port << " looked up message " << msg;

					std::stringstream out (std::stringstream::out);
					out << "#ACK\n#QUERY: " << msg << "#HOPS: " << hops << "\n#HOST: "
						<< host << ":" << (*vi)->port << "\n#ID: " << (*vi)->get_id() << "\n";

					out << "(key(";
					::vec2stream<T> ( out, key, dims, ',');

					if (&tuple != 0)
						out << "),[" << tuple.second << "])\n#END\n";
					else
						out << "),[])\n#END\n";

					ClientSocket cs(dest_host, dest_port);
					cs << out.str();
					return 0;
				}
			}
		}else{
			std::stringstream out (std::stringstream::out);
			out << "L(";
			::vec2stream<T> ( out, key, dims, ',');
			out << ") " << dest_host << " " << dest_port << " " << hops << "\n";

			try{
				//skip.at(dest_link)->open();
				*skip.at(dest_link) << out.str();
			}catch (std::exception &e){
				std::cerr << "** " << get_id() << "@" << port << " traced that link#" << dest_link << " has failed.\n";
				skip [dest_link] = 0;
				return -1;
			}
			std::cerr << "** " << get_id() << "@" << port << " is forwarding to link " << dest_link << " message: " << out.str();
			return 1;
		}
	}else{
		std::pair<T*, char*>& tuple = pool.lookup(key);

		std::cerr << "** " << get_id() << "@" << port << " looked up message " << msg;

		std::stringstream out (std::stringstream::out);
		out << "#ACK\n#QUERY: " << msg << "#HOPS: " << hops << "\n#HOST: "
			<< host << ":" << port << "\n#ID: " << get_id() << "\n";

		out << "(key(";
		::vec2stream<T> ( out, key, dims, ',');

		if (&tuple != 0)
			out << "),[" << tuple.second << "])\n#END\n";
		else
			out << "),[])\n#END\n";

		ClientSocket cs(dest_host, dest_port);
		cs << out.str();
		return 0;
	}
}

/**
 * R((.4,.4),(.6,.6)) 127.0.0.1 50000 0\n
 */
template<class T> int Node<T>::process_range_msg ( std::string& msg ) {
	std::stringstream in(msg, std::stringstream::in);

	char symbol;
	in >> symbol;
	if (symbol != 'R')
		throw std::runtime_error(" Bad range query processing.\n");

	in >> symbol;
	if (symbol != '(')
		throw std::runtime_error(" Bad range request message. Tuple open parenthesis expected.\n");

	T key [2][dims];

	/* lo-point */
	in >> symbol;
	if (symbol != '(')
		throw std::runtime_error(" Bad range request message. Lo-point open parenthesis expected.\n");

	::stream2vec<T> ( in, key[0], dims, ',' );

	in >> symbol;
	if ( symbol != ')' )
		throw std::runtime_error (" Bad range request message. Lo closing parenthesis expected.\n");

	in >> symbol;
	if (symbol != ',')
		throw std::runtime_error(" Bad range request message. Lo-Hi comma expected.\n");

	/* hi-point */
	in >> symbol;
	if (symbol != '(')
		throw std::runtime_error(" Bad range request message. Hi-point open parenthesis expected.\n");

	::stream2vec<T> ( in, key[1], dims, ',' );

	in >> symbol;
	if ( symbol != ')' )
		throw std::runtime_error (" Bad range request message. Hi closing parenthesis expected.\n");

	in >> symbol;
	if (symbol != ')')
		throw std::runtime_error(" Bad range request message. Tuple closing parenthesis expected\n");

	std::string dest_host;
	in >> dest_host;

	int dest_port(-1);
	in >> dest_port;

	int hops(-1);
	in >> hops;
	++hops;

	/**
	 * local processing
	 */
	std::vector<std::pair<T*, char*>*> ans;
	pool.range(key[0], key[1], ans);

	if (!ans.empty()) {
		std::stringstream out (std::stringstream::out);
		out << "#ACK\n#QUERY: " << msg << "#HOPS: " << hops << "\n#HOST: "
			<< host << ":" << port << "\n#ID: " << get_id() << "\n";

		for (unsigned i = 0; i < ans.size(); ++i) {
			out << "(key(";
			::vec2stream<T>(out, ans.at(i)->first, dims, ',');
			out << "),[" << ans.at(i)->second << "])\n";
		}
		out << "#END\n";

		std::cerr << "** " << get_id() << "@" << port
			<< " returning answer of size " << ans.size() << " to "
			<< dest_host << ":" << dest_port << "\n";

		ClientSocket cs(dest_host, dest_port);
		cs << out.str();

		out.clear();
		ans.clear();
	}

	/**
	 * forwarding request for remote processing
	 */
	if (!pool.encloses(key[0], key[1])) {
		for (unsigned j = 0; j < hist.size(); ++j) {
			int d = j % dims;

			/* if there is no relevance between the split area the range query */
			if (!hist[j] && key[0][d] < pts[j] && key[1][d] < pts[j])
				continue;
			if (hist[j] && key[0][d] > pts[j] && key[1][d] > pts[j])
				continue;
			if (key[0][d] == key[1][d])
				continue;

			/**
			 * query fragmenting to relevant part of the answer before
			 * forwarding according to split points.
			 */
			T subquery[2][dims];

			/**
			 * creating relevant sub-query
			 */
			for (int i = 0; i < dims; ++i) {
				if (i == d) {
					if (!hist[j]) {
						subquery[0][i] = (key[0][i] < pts[j] ? pts[j] : key[0][i]);
						subquery[1][i] = key[1][i];
						key[1][i] = subquery[0][i];
					}else{
						subquery[0][i] = key[0][i];
						subquery[1][i] = (key[1][i] > pts[j] ? pts[j] : key[1][i]);
						key[0][i] = subquery[1][i];
					}
				}else{
					subquery[0][i] = key[0][i];
					subquery[1][i] = key[1][i];
				}
			}

			/**
			 * creating request message
			 */
			std::stringstream lo_req (std::stringstream::out);
			std::stringstream hi_req (std::stringstream::out);

			lo_req << "((";
			::vec2stream<T>(lo_req, subquery[0], dims, ',');
			lo_req << "),";

			hi_req << "(";
			::vec2stream<T>(hi_req, subquery[1], dims, ',');
			hi_req << "))";

			std::stringstream tail_req (std::stringstream::out);
			tail_req << dest_host << " " << dest_port << " " << hops << "\n";

			std::string request = "R" + lo_req.str() + hi_req.str() + " "
					+ tail_req.str();

			for (int i = 0; i < dims; ++i)
				if (subquery[0][i] >= subquery[1][i]) {
					request.clear();
					break;
				}

			if (!request.empty()) {
				std::cerr << "** Server@"<< port << " forwarding to link " << j << " request: " << request ;
				try{
					//skip.at(j)->open();
					*skip.at(j) << request;

					std::string response;
					*skip.at(j) >> response;

					if (response.compare("R OK\n") != 0){
						std::cerr << "** " << get_id() << "@" << port << " failed to forward message: " << msg;
						return -1;
					}
				}catch (std::exception &e){
					std::cerr << "** " << get_id() << "@" << port << " traced that link#" << j << " has failed.\n";
					skip [j] = 0;
					return -1;
				}
			}
		}
	}
	return 0;
}

/**
 * N((q1,..,qD),K,Rmax) host_IP port_no hops depth\n
 */
template<class T> int Node<T>::process_nearest_msg ( std::string& msg ) {
	std::stringstream in (msg, std::stringstream::in);

	char symbol;
	in >> symbol;
	if (symbol != 'N')
		throw std::runtime_error(" Bad nearest neighbor query processing.\n");

	in >> symbol;
	if (symbol != '(')
		throw std::runtime_error(" Bad nearest neighbor request message. Tuple open parenthesis expected.\n");

	in >> symbol;
	if (symbol != '(')
		throw std::runtime_error(" Bad nearest neighbor request message. Query center open parenthesis expected.\n");

	T key [dims];

	::stream2vec<T> ( in, key, dims, ',' );

	in >> symbol;
	if ( symbol != ')' )
		throw std::runtime_error (" Bad nearest neighbor request message. Query center closing parenthesis expected.\n");

	in >> symbol;
	if (symbol != ',')
		throw std::runtime_error(" Bad nearest neighbor request message. Query center - K comma expected.\n");

	int K;
	in >> K;

	in >> symbol;
	if (symbol != ',')
		throw std::runtime_error(" Bad nearest neighbor request message. K - Rmax comma expected.\n");

	double Rmax;
	in >> Rmax;

	if (Rmax < 0) {
		if (pool.isRelevant (key)){
			Rmax = DBL_MAX;
		}else{
			unsigned relevant = forward_to(key);
			try{
				*skip.at(relevant) << msg;

				std::string response;
				*skip.at(relevant) >> response;

				if (response.compare("N OK\n") != 0){
					std::cerr << "** " << get_id() << "@" << port << " failed to forward message: " << msg;
					return -1;
				}
				return 0;
			}catch (std::exception &e){
				std::cerr << "** " << get_id() << "@" << port << " traced that link#" << relevant << " has failed.\n";
				skip [relevant] = 0;
				return -1;
			}
		}
	}

	in >> symbol;
	if (symbol != ')')
		throw std::runtime_error(" Bad nearest neighbor request message. Tuple closing parenthesis expected\n");

	std::string dest_host;
	in >> dest_host;

	int dest_port = 0;
	in >> dest_port;

	int hops = 0;
	in >> hops;
	++hops;

	int depth = 0;
	in >> depth;

	/**
	 * local processing
	 */
	std::vector<std::pair<double, std::pair<T*, char*>*>*> ans;
	double Rlocal = pool.nearest(key, K, Rmax, ans);

	if (Rlocal < Rmax)
		Rmax = Rlocal;

	if (!ans.empty()) {
		std::stringstream out (std::stringstream::out);
		out << "#ACK\n#QUERY: " << msg << "#HOPS: " << hops << "\n#HOST: "
				<< host << ":" << port << "\n#ID: " << get_id() << "\n";

		for (unsigned i = 0; i < ans.size(); ++i) {
			out << "(distance(" << ans.at(i)->first << "),key(";
			::vec2stream<T> (out, ans.at(i)->second->first, dims, ',');
			out << "),[";
			out << ans.at(i)->second->second;
			out << "])\n";
		}
		out << "#END\n";

		std::cerr << "** " << get_id() << "@" << port
				<< " returning answer of size " << ans.size() << " to "
				<< dest_host << ":" << dest_port << "\n";

		ClientSocket cs (dest_host, dest_port);
		cs << out.str();
	}

	/**
	 * forwarding request for remote processing
	 */
	std::stringstream general_req_stream (std::stringstream::out);
	general_req_stream << "N((";

	::vec2stream<T>(general_req_stream, key, dims, ',');

	general_req_stream << ")," << K << "," << Rmax << ") ";
	general_req_stream << dest_host << " " << dest_port << " " << hops << " ";

	T lo[dims];
	T hi[dims];

	for (int j = 0; j < dims; ++j) {
		lo[j] = key[j] - Rmax;
		hi[j] = key[j] + Rmax;
	}

	if (!pool.encloses(lo, hi)) {
		for (unsigned j = depth; j<hist.size(); ++j) {
			int d = j % dims;

			if (!hist[j] && lo[d]<pts[j] && hi[d]<pts[j])
				continue;
			if (hist[j] && lo[d]>pts[j] && hi[d]>pts[j])
				continue;

			std::stringstream current_req_stream (std::stringstream::out);
			current_req_stream << general_req_stream.str() << j + 1 << "\n";

			std::cerr << "** " << get_id() << "@"<< port << " forwarding to link " << j << " request: " << current_req_stream.str() ;

			try{
				//skip.at(j)->open();
				*skip.at(j) << current_req_stream.str();

				std::string response;
				*skip.at(j) >> response;

				if (response.compare("N OK\n") != 0){
					std::cerr << "** " << get_id() << "@" << port << " failed to forward message: " << msg;
					return -1;
				}
				return 0; //////////////////////////////////////////////////////////////////// should this be here?????
			}catch (std::exception &e){
				std::cerr << "** " << get_id() << "@" << port << " traced that link#" << j << " has failed.\n";
				skip [j] = 0;
				return -1;
			}
		}
	}
	return 0;
}

template<class T> Node<T>& Node<T>::split () {
	int splt_dim = hist.size() % dims;

	T lo1 [dims];
	T hi0 [dims];

	for (int j = 0; j < dims; ++j) {
		if (j == splt_dim) {
			lo1[j] = pool.get_median (splt_dim);
			hi0[j] = lo1[j];
		} else {
			lo1[j] = pool.get_lo()[j];
			hi0[j] = pool.get_hi()[j];
		}
	}

	/* old pool */
	Pool<T>* lo_pool = new Pool<T> (dims, pool.get_lo(), hi0);

	std::vector<std::pair<T*, char*>*> data;
	pool.range (pool.get_lo(), hi0, data);
	random_shuffle (data.begin(), data.end());
	for (unsigned j = 0; j < data.size(); ++j)
		lo_pool->push (data.at(j)->first, data.at(j)->second);
	data.clear();

	/* new node */
	int new_node_port = port + (1<<hist.size());

	Node<T> *new_node = new Node<T> (host,
			new_node_port,
			dims,
			lo1,
			pool.get_hi());

	pool.range (lo1, pool.get_hi(), data);
	random_shuffle (data.begin(), data.end());
	for (unsigned j = 0; j < data.size(); ++j)
		new_node->pool.push (data.at(j)->first, data.at(j)->second);
	data.clear();

	new_node->hist.insert (new_node->hist.end(), hist.begin(), hist.end());
	new_node->hist.push_back (true);
	hist.push_back (false);

	pts.push_back ( lo1[splt_dim] );
	new_node->pts.insert (new_node->pts.end(), pts.begin(), pts.end());

	new_node->frontlink_hosts.insert (new_node->frontlink_hosts.end(),
			frontlink_hosts.begin(),
			frontlink_hosts.end());

	new_node->frontlink_ports.insert (new_node->frontlink_ports.end(),
			frontlink_ports.begin(),
			frontlink_ports.end());

	new_node->frontlink_hosts.push_back (host);
	new_node->frontlink_ports.push_back (port);

	frontlink_hosts.push_back (host);
	frontlink_ports.push_back (new_node_port);

	std::cerr << "** " << get_id() << "@" << port << " was split on dim#" << splt_dim << " on value ";
	::vec2stream<T> ( std::cerr, lo1 + splt_dim, 1, '\n');
	std::cerr << "\n";

	pool = *lo_pool;

	return *new_node;
}

template<class T> inline int Node<T>::forward_to (T key[]) const {
	for (unsigned j = 0; j < hist.size(); ++j) {
		int dim = j % dims;
		if ((hist.at(j) && key[dim] < pts.at(j)) || (!hist.at(j) && key[dim] >= pts.at(j)))
			return j;
	}
	return -1;
}

template<class T> void Node<T>::unlink () {
	for (std::vector<ClientSocket*>::const_iterator vi=skip.begin(); vi!=skip.end(); ++vi) {
		(*vi)->close();
		delete *vi;
	}
	skip.clear();

	void **ret = 0;

	unsigned thread_counter = 0;
	for (std::vector<pthread_t*>::iterator vi = server_threads.begin(); vi!=server_threads.end(); ++vi) {
		std::cerr << "** " << get_id() << "@" << port
			<< " joining node handling thread#" << ++thread_counter << "/"
			<< server_threads.size() << "\n";
		pthread_join(**vi, ret);
		delete *vi;
	}
	server_threads.clear();
}

template<class T> void Node<T>::link () {
	assert (frontlink_ports.size() == frontlink_hosts.size());

	for (unsigned j = skip.size(); j < frontlink_ports.size(); ++j) {
		std::cerr << "** " << get_id() << "@" << port << " establishes connection with link#" << j << " @" << frontlink_hosts.at(j) << ":"<<frontlink_ports.at(j)<<".\n";

		ClientSocket *cs = 0;
		try{
			cs = new ClientSocket (frontlink_hosts.at(j), frontlink_ports.at(j));
			skip.push_back (cs);
		}catch (std::exception &e){
			std::cerr << "** " << get_id() << "@" << port << " unable to establish connection with link#" << j << " @" << frontlink_hosts.at(j) << ":"<<frontlink_ports.at(j)<<".\n";
			skip.push_back (0);
		}
	}
}

template<class T> int Node<T>::merge_hi (Node &hi) {
	if (hist.empty()) return -1;

	if (hist.size() != hi.hist.size()) {
		std::cerr << "** " << get_id() << "@" << port
			<< " unable to merge with " << hi.get_id()
			<< " of different split history\n";
		return -1;
	}

	int last_splt = (hist.size()-1) % dims;
	for (int j = 0; j < dims; ++j) {
		if (j == last_splt) {
			if (hi.pool.get_lo()[j] != pool.get_hi()[j]) {
				std::cerr << "** ERROR - " << get_id() << "@" << port
					<< " is unable to merge with hi " << hi.get_id() << "@"
					<< hi.port << " on split dim#" << j << ".\n";
				return -1;
			}
		}else{
			if (hi.pool.get_lo()[j] != pool.get_lo()[j] || hi.pool.get_hi()[j]
					!= pool.get_hi()[j]) {
				std::cerr << "** ERROR - " << get_id() << "@" << port
					<< " is unable to merge with hi " << hi.get_id() << "@"
					<< hi.port << " on dim#" << j << ".\n";
				return -1;
			}
		}
	}

	pool.set_hi (hi.pool.get_hi());

	std::vector<std::pair<T*, char*>*> data;

	hi.pool.range(hi.pool.get_lo(), hi.pool.get_hi(), data);
	random_shuffle(data.begin(), data.end());
	for (unsigned j = 0; j < data.size(); ++j) {
		T* key = (T*) calloc (dims, dims*sizeof(T));
		memcpy (key, data.at(j)->first, dims*sizeof(T));

		char* buffer = (char*) malloc (strlen(data.at(j)->second) + 1);
		strcpy (buffer, data.at(j)->second);

		pool.push (key, buffer);
	}

	hist.pop_back ();
	skip.pop_back ();
	pts.pop_back ();

	frontlink_hosts.pop_back ();
	frontlink_ports.pop_back ();

	std::cerr << "** " << get_id() << "0@" << port << " was merged with "
		<< hi.get_id() << " on dim#" << last_splt << "\n";
	return 0;
}

template<class T> int Node<T>::merge_lo (Node &lo) {
	if (hist.empty()) return -1;

	if (hist.size() != lo.hist.size()) {
		std::cerr << "** " << get_id() << "@" << port
			<< " unable to merge with " << lo.get_id()
			<< " of different split history\n";
		return -1;
	}

	int last_splt = (hist.size()-1) % dims;
	for (int j = 0; j < dims; ++j) {
		if (j == last_splt) {
			if (pool.get_lo()[j] != lo.pool.get_hi()[j]) {
				std::cerr << "** ERROR - " << get_id() << "@" << port
					<< " is unable to merge with lo " << lo.get_id() << "@"
					<< lo.port << " on split dim#" << j << ".\n";
				return -1;
			}
		} else {
			if (pool.get_lo()[j] != lo.pool.get_lo()[j] || 
				pool.get_hi()[j] != lo.pool.get_hi()[j]) {
					std::cerr << "** ERROR - " << get_id() << "@" << port
						<< " is unable to merge with lo " << lo.get_id() << "@"
						<< lo.port << " on dim#" << j << ".\n";
				return -1;
			}
		}
	}

	pool.set_lo (lo.pool.get_lo());

	std::vector<std::pair<T*, char*>*> data;

	lo.pool.range(lo.pool.get_lo(), lo.pool.get_hi(), data);
	random_shuffle(data.begin(), data.end());
	for (unsigned j = 0; j < data.size(); ++j) {
		T* key = (T*) calloc (dims, dims*sizeof(T));
		memcpy (key, data.at(j)->first, dims*sizeof(T));

		char* buffer = (char*) malloc (strlen(data.at(j)->second) + 1);
		strcpy (buffer, data.at(j)->second);

		pool.push (key, buffer);
	}

	hist.pop_back ();
	skip.pop_back ();
	pts.pop_back ();

	frontlink_hosts.pop_back ();
	frontlink_ports.pop_back ();

	std::cerr << "** " << get_id() << "1@" << port << " was merged with "
		<< lo.get_id() << " on dim#" << last_splt << "\n";
	return 0;
}

template<class T> int Node<T>::depart () {
	if (hist.empty()) {
		std::cerr << "**  @" << port << " parent node cannot merge!\n";
		unlink();
		return 0;
	}

	assert (frontlink_hosts.size() == frontlink_ports.size());
	ClientSocket cs (frontlink_hosts.back(), frontlink_ports.back());

	cs << "G " + get_id().substr(0, hist.size()-1) + (hist.back() ? "0" : "1") + "\n" + marshalize(true);

	std::string aggr_response;
	std::string response;
	cs >> response;

	if (response.compare ("G OK\n") != 0){
		aggr_response = "G BAD " + get_id();
	}

	for (typename std::vector<Node<T>*>::const_iterator vi=cached.begin(); vi!=cached.end(); ++vi) {
		cs << "G " + get_id().substr(0, hist.size()-1) + (hist.back() ? "0" : "1") + "\n" + marshalize(true);
		cs >> response;

		if (response.compare ("G OK\n") != 0){
			aggr_response += get_id() + " ";
		}
	}
	aggr_response += '\n';

	if (response.empty()){
		return 0;
	}else{
		std::cerr << "** " << get_id() << "@" << port << " received negative reply message: " << aggr_response << "\n";
		return -1;
	}
}

template<class T> int Node<T>::process_merge_msg (std::string& msg) {
	std::stringstream in (msg, std::stringstream::in);

	char symbol;
	in >> symbol;
	if (symbol != 'G')
		throw std::runtime_error (" Bad merge message header.\n");

	std::string id;
	in >> id;

	std::string construct_msg = in.str();
	construct_msg = construct_msg.substr(construct_msg.find("\n")+1,
			construct_msg.size()-construct_msg.find("\n"));

	Node<T>* to_mrg = new Node<T>(host, 0, dims, construct_msg);

	if (id.compare (get_id()) == 0) {
		if (!hist.back()){
			if (merge_hi (*to_mrg) != 0){
				std::cerr << "** " << get_id() << "@" << port
					<< " unable to merge with " << to_mrg->get_id() << ".\n";
				return -1;
			}
		}else{
			if (merge_lo (*to_mrg) != 0){
				std::cerr << "** " << get_id() << "@" << port
					<< " unable to merge with " << to_mrg->get_id() << ".\n";
				return -1;
			}
		}
		delete to_mrg;
		return 0;
	}else{
		std::cerr << "** ERROR - Unauthorized merge message received by "
			<< get_id() << " instead of " << id << "\n";
		cached.push_back (to_mrg);
		to_mrg->port = port;
		to_mrg->host = host;

		unsigned i = 0;
		unsigned bad = 0;
		unsigned failed = 0;
		for (std::vector<ClientSocket*>::const_iterator vi=skip.begin(); vi!=skip.end(); ++vi) {
			std::string link_id;
			for (unsigned j=0; j<i; ++j) {
				link_id.push_back (hist.at(j) ? '1' : '0');
			}
			link_id.push_back (hist.at(i) ? '0' : '1');

			if (to_mrg->get_id().compare(link_id) != 0) {
				try{
					std::cerr << "** " << get_id() << "@" << port << " To forward maintenance message to link#" << i << " corresponding to node " << link_id << "X.\n";

					std::string response;
					*skip.at(i) << "O " << get_id() << "\n" << to_mrg->marshalize(false);
					*skip.at(i) >> response;

					if (response.compare("O OK\n") != 0){
						std::cerr << "** " << get_id() << "@" << port << " failed response from overlay maintenance message: " << msg;
						++bad;
					}
				}catch (std::exception &e){
					std::cerr << "** " << get_id() << "@" << port << " traced that link#" << i << " has failed.\n";
					skip [i] = 0;
					++failed;
				}
			}else{
				std::cerr << "** " << get_id() << "@" << port << " Omitting maintenance message to link#" << i << " corresponding to node " << link_id << "X.\n";
			}
			++i;
		}
		return -bad-failed;
	}
}

template<class T> std::string Node<T>::marshalize (bool print_data) const {
	std::stringstream out (std::stringstream::out);

	out << "#AREA\n";

	::vec2stream<T> ( out, pool.get_lo(), dims, '\n' );
	out << "\n";

	::vec2stream<T> ( out, pool.get_hi(), dims, '\n' );
	out << "\n";

	out << "#NODE\n";
	out << host << " " << port << " " << "\n";

	/* split history */
	out << get_id() << "\n";

	/* split points */
	T temp [ pts.size() ];

	for (unsigned j = 0; j < pts.size(); ++j)
		temp [j] = pts.at(j);

	::vec2stream<T> ( out, temp, pts.size(), '\n' );
	out << "\n";

	/* links */
	for (unsigned j = 0; j < hist.size(); ++j)
		out << frontlink_hosts.at(j) << " " << frontlink_ports.at(j) << "\n";

	if (print_data) {
		/* ( key, value ) tuple array */
		std::vector<std::pair<T*, char*>*> ans;
		pool.range(pool.get_lo(), pool.get_hi(), ans);
		out << "#TUPLES " << ans.size() << "\n";

		for (unsigned i = 0; i < ans.size(); ++i) {
			::vec2stream<T> ( out, ans.at(i)->first, dims, ',' );
			out << " " << ans.at(i)->second << "\n";
		}
	}

	out << "#END\n";

	return out.str();
}

template<class T> inline std::string Node<T>::get_id () const {
	std::string id;
	for (std::vector<bool>::const_iterator vi = hist.begin(); vi != hist.end(); ++vi) {
		id.push_back (*vi ? '1' : '0');
	}
	return id;
}

template<class T> double Node<T>::get_volume_load () const {
	double volume = 1;
	for (int j = 0; j < dims; ++j)
		volume *= pool.get_hi()[j] - pool.get_lo()[j];
	return volume;
}
