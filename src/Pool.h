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

#ifndef POOL_H_
#define POOL_H_

#include "Dtree.h"

template<class T> class Pool {
	Dtree<T> dtree;

	int dims;

	T* lo;
	T* hi;

public:
	Pool (int dms) : dtree(dms), dims(dms) {lo=0; hi=0;}

	Pool ( int dms, T const* lo_key, T const* hi_key ) : dtree(dms), dims(dms) {
		lo = (T*) calloc (dims, dims*sizeof(T));
		hi = (T*) calloc (dims, dims*sizeof(T));
		memcpy(lo, lo_key, dims*sizeof(T));
		memcpy(hi, hi_key, dims*sizeof(T));
	}

	~Pool () {free(lo);free(hi);}

	bool isRelevant ( T *key ) const {
		for (int j=0; j<dims; ++j)
			if ( key[j] < lo[j] || key[j] >= hi[j])
				return false;
		return true;
	}

	bool encloses ( T *lo_key, T *hi_key ) const {
		for (int j=0; j<dims; ++j)
			if (lo_key[j] < lo[j] || hi_key[j] > hi[j] )
				return false;
		return true;
	}

	/* indexes a (key, value) pair */
	void push ( T *key , char* val );

	/* removes and returns from index a (key, value) pair or NULL if non-existent */
	char* pop ( T *key );

	/* updates the value of an already indexed key */
	void update ( T *key , char* val );

	/* concatenates the value of an already indexed key with the passed */
	void concatenate ( T *key , char* val );

	std::pair<T*,char*>& lookup ( T *key ) const;

	void range ( T *lo, T *hi, std::vector<std::pair<T*,char*>*>& ans ) const;

	double nearest ( T *key, int K, double radius, std::vector<std::pair<double,std::pair<T*,char*>*>*> &ans ) const;

	std::vector<std::pair<T*,char*>*>* split ( int dim ) const;

	void set_lo (T* lo_key) {
		if (lo==0) lo=(T*)calloc(dims,dims*sizeof(T));
		memcpy (lo,lo_key,dims*sizeof(T));
	}

	void set_hi (T* hi_key) {
		if(hi==0) hi=(T*)calloc(dims,dims*sizeof(T));
		memcpy(hi,hi_key,dims*sizeof(T));
	}

	T* get_lo () const {return lo;}
	T* get_hi () const {return hi;}

	T get_median ( int dim ) const;

	unsigned get_size () const {return dtree.get_size();}
};

#endif
