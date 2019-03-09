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

#include "Pool.h"
#include "Dtree.cpp"

template<class T> inline void Pool<T>::push ( T *key , char* val ) {
	return dtree.push( key, val );
}

template<class T> inline char* Pool<T>::pop ( T *key ) {
	return dtree.pop( key );
}

template<class T> inline std::pair<T*,char*>& Pool<T>::lookup ( T *key ) const {
	TreeNode<T> const* ret = dtree.search( key );
	if ( ret != 0 )
		return *( new std::pair<T*,char*> (key, ret->val) );
	return *(std::pair<T*,char*>*)0;
}

template<class T> void Pool<T>::range ( T *lo, T *hi, std::vector<std::pair<T*,char*>*>& ans ) const {
	dtree.range ( lo, hi, ans );
}

template<class T> double Pool<T>::nearest ( T *key, int K, double radius, std::vector<std::pair<double,std::pair<T*,char*>*>*> &ans ) const {
	return (radius < 0 ? radius : dtree.nearest(key, K, radius, ans));
}

template<class T> void Pool<T>::update ( T *key, char* val ) {
	TreeNode<T>* ans = dtree.search( key );

	if ( ans == 0 )
		return dtree.push ( key, val );

	delete[] ans->val;
	ans->val = val;
}

template<class T> void Pool<T>::concatenate (T *key, char* val) {
	TreeNode<T>* ans = dtree.search ( key );

	if ( ans == 0 ) {
		return dtree.push ( key, val );
	}else{
		char* buffer = (char*) malloc (strlen(ans->val)+strlen(val)+1);

		strcpy (buffer, ans->val);
		strcat (buffer, val);

		free (ans->val);
		free (val);

		ans->val = buffer;
	}
}

template<class T> T Pool<T>::get_median ( int dim ) const {
	if ( dtree.get_size() == 0 ) {
		return (get_hi()[dim]-get_lo()[dim])/2 + get_lo()[dim];
	}else{
		std::vector<std::pair<T*,char*>*> &temp = dtree.order ( dim );
		return temp.at( temp.size()>>1 ) -> first [dim];
	}
}

template<class T> std::vector<std::pair<T*,char*>*>* Pool<T>::split ( int splt_dim ) const {
	std::vector<std::pair<T*,char*>*>* pool_pair = new std::vector<std::pair<T*,char*>*>[2];

	unsigned target_sz = (dtree.get_size()>>1);
	std::vector<std::pair<T*,char*>*> &temp = dtree.order ( splt_dim );

	pool_pair[1].assign (temp.begin(), temp.begin() + target_sz);
	pool_pair[0].assign (temp.begin() + target_sz, temp.end());

	assert ( pool_pair[0].size() == pool_pair[1].size() );

	return pool_pair;
}
