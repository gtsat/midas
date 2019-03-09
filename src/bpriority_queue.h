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

#ifndef BPRIORITY_QUEUE_H_
#define BPRIORITY_QUEUE_H_

#include <queue>

template<class T> class bpair_comparison {

	int indx_dim;
	bool reverse;

public:

	bpair_comparison ( const bool& revparam=false, const int dim=0 ) {
		reverse=revparam;
		indx_dim=dim;
	}

	bool operator () ( const std::pair<T*,char*>* left, const std::pair<T*,char*>* right ) const {
	  if (reverse)
		  return (left->first [indx_dim] > right->first [indx_dim]);
	  else
		  return (left->first [indx_dim] < right->first [indx_dim]);
	}
};

template<class T> class dist_comparison {

	bool reverse;
public:

	dist_comparison ( const bool& revparam=false) {
		reverse=revparam;
	}

	bool operator () ( const std::pair<double,std::pair<T*,char*>*>* left, const std::pair<double,std::pair<T*,char*>*>* right ) const {
	  if (reverse)
		  return ( left->first > right->first );
	  else
		  return ( left->first < right->first );
	}
};

#endif
