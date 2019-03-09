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

#include "Node.cpp"
#include "common.h"
#include <pthread.h>
#include <getopt.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <csignal>

typedef double index_t;

bool ipv6 = false;
Node<index_t> *local_node = 0;

void build_overlay (std::string &host, int port, int dims, double lo, double hi , int splits);

void print_usage ( char*program ) {

	std::cerr << "%% Usage:\n\t" << program << " option parameter\n";

	std::cerr << "\t\t-d --dims\n";
	std::cerr << "\t\t-l --low\n";
	std::cerr << "\t\t-g --high\n";
	std::cerr << "\t\t-h --host\n";
	std::cerr << "\t\t-p --port\n";
	//std::cerr << "\t\t-s --splits\n";
	std::cerr << "\t\t-r --remote\n";
	std::cerr << "\t\t-a --at\n";
	std::cerr << "\t\t-6 --ipv6\n";
}

void signalDepart (int signum) {
   std::cerr << "%% Interrupt signal (" << signum << ") received.\n";
   local_node->depart();
   local_node->unlink();
   delete local_node;
   exit (EXIT_SUCCESS);
}

int main ( int argc, char* argv[] ) {
    struct sigaction action;
    memset(&action, 0, sizeof(struct sigaction));
    action.sa_handler = signalDepart;
	sigaction (SIGTERM,&action,NULL);
	sigaction (SIGINT,&action,NULL);

	int next_option;
	const char* const short_options="ud:l:g:h:p:r:a:6"; //s:
	const struct option long_options[]={
		{"usage",0,NULL,'u'},
		{"dims",1,NULL,'d'},
		{"low",1,NULL,'l'},
		{"high",1,NULL,'g'},
		{"host",1,NULL,'h'},
		{"port",1,NULL,'p'},
		//{"splits",1,NULL,'s'},
		{"remote",1,NULL,'r'},
		{"at",1,NULL,'a'},
		{"ipv6",1,NULL,'6'},
		{NULL,0,NULL,0}
	};

	int dims=0;
	//int splits=0;
	int local_port=0, remote_port=0;
	std::string local_host, remote_host;
	double low=0, high=1;

	do{
		next_option = getopt_long (argc,argv,short_options,long_options,NULL);

		switch (next_option) {
		case 'u':
			print_usage (argv[0]);
			return -1;
		case 'd':
			dims = std::atoi (optarg);
			break;
		case 'l':
			low = std::atof (optarg);
			break;
		case 'g':
			high = std::atof (optarg);
			break;
		case 'h':
			local_host = optarg;
			break;
		case 'p':
			local_port = std::atoi (optarg);
			break;
		case 's':
			//splits = std::atoi (optarg);
			break;
		case 'r':
			remote_host = optarg;
			break;
		case 'a':
			remote_port = std::atoi (optarg);
			break;
		case '6':
			ipv6 = true;
			break;
		case '?':
			break;
		case -1:
			break;
		default:
			abort();
		}
	}while( next_option != -1 );

	if ( dims == 0 ) {
		std::cerr << "** ERROR - Dimensionality should also be defined.\n";
		print_usage(argv[0]);
		return -1;
	}
	srand(time(0));

	if ( local_port > 1024 && remote_port <= 1024) { // && splits >= 0
		build_overlay (local_host, local_port, dims, low, high, 0); //splits
	}else{
		std::stringstream request;
		request << "S " << local_host << " " << std::to_string(local_port) << "\n";
		ClientSocket cs (remote_host,remote_port);
		cs << request.str();
		std::string response;
		cs >> response;
		local_node = new Node<index_t> (local_host,local_port,dims,response);
		local_node->link();
		cs << "S OK\n";
		local_node->serve();
	}
	return 0;
}

void build_overlay (std::string &host, int port, int dims, double lo, double hi , int splits) {
	pthread_attr_t attr;
	pthread_attr_init (&attr);
	pthread_attr_setdetachstate (&attr, PTHREAD_CREATE_DETACHED);
	pthread_attr_setstacksize (&attr, THREAD_STACK_SIZE);

	index_t lo_vec [dims];
	index_t hi_vec [dims];

	for ( int j=0; j<dims; ++j ) {
		lo_vec[j] = lo;
		hi_vec[j] = hi;
	}

	std::vector<Node<index_t>*> nodes;
	std::vector<pthread_t*> node_threads;

	local_node = new Node<index_t>(host, port , dims, lo_vec, hi_vec);
	nodes.push_back (local_node);
	local_node->serve();
	/*
	pthread_t *thread = new pthread_t;
	if ( pthread_create ( thread, &attr, ::serve<Node<index_t> >, nodes.back() ) != 0 )
		throw std::runtime_error("%% ERROR - Unable to create a new thread.");

	node_threads.push_back (thread);
	nodes.back()->link();

	for (unsigned j=0; j<splits; ++j) {
		unsigned nsize = nodes.size();
		for (unsigned i=0; i<nsize; ++i) {
			nodes.push_back (&nodes.at(i)->split());
			pthread_t *thread = new pthread_t;
			if (pthread_create (thread, &attr, ::serve<Node<index_t> >, nodes.back()) != 0)
				throw std::runtime_error("%% ERROR - Unable to create a new thread.");

			node_threads.push_back (thread);

			nodes.at(i)->link ();
			nodes.back()->link ();
		}
	}*/
}
/*
int read_data(const char* infname)
{
  std::ifstream fin(infname, std::ios::in);
  char sep[10];

  if ( !fin.is_open() ) {
	  std::cerr << "** ERROR - Unable to open file " <<  infname << ".\n" ;
      return -1;
  }

  while( !fin.eof() ) {
	double *x = new double[dims];
    try{
      for(int i=0;i<dims;i++) {
        if(i>0)
          fin.getline(sep, 15, ',');
        if(!(fin >> x[i]))
          throw -1;
      }
      fin.getline(sep, 15);
      fin >> std::skipws ;
      data.push_back(x);
    } catch(int n) {
      std::cerr << "** ERROR - Exception caught while loading file " <<  infname << ".\n" ;
      //exit(EXIT_FAILURE);
      delete x;
    }
  }
  std::cerr << "Read " << data.size() << " elements." << std::endl;
  return 0;
}
*/
