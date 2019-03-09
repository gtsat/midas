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

#include "Dtree.h"

#include <cassert>
#include <cfloat>
#include <cmath>
#include <iostream>
#include <typeinfo>

template<class T> bool TreeNode<T>::operator == ( T *nd_key ) const {
	for (int j=0; j<dims ; ++j)
		if (key[j] != nd_key[j])
			return false;
	return true;
}

template<class T> inline bool TreeNode<T>::operator != ( T *nd_key ) const {
	return !(*this == nd_key);
}

template<class T> inline bool TreeNode<T>::operator >= ( T *nd_key ) const {
	for (int j=0; j<dims ; ++j)
		if (key[j] < nd_key[j])
			return false;
	return true;
}

template<class T> inline bool TreeNode<T>::operator <= ( T *nd_key ) const {
	for (int j=0; j<dims ; ++j)
		if (key[j] > nd_key[j])
			return false;
	return true;
}

template<class T> double TreeNode<T>::dist ( T *nd_key ) const {
	double dist = 0.0;

	assert (sizeof(char)==1);
	char *key_hack=reinterpret_cast<char*>(key);
	char *nd_key_hack=reinterpret_cast<char*>(nd_key);

	for (int j=0; j<dims ; ++j)
		if ( typeid (T) == typeid (char) ||
			typeid (T) == typeid (short int) ||
			typeid (T) == typeid (unsigned short int) ||
			typeid (T) == typeid (int) ||
			typeid (T) == typeid (unsigned int) ||
			typeid (T) == typeid (long int) ||
			typeid (T) == typeid (unsigned long int) ||
			typeid (T) == typeid (float) ||
			typeid (T) == typeid (double) ) {
			dist += pow ( *(double*)(key_hack+j*sizeof(T)) - *(double*)(nd_key_hack+j*sizeof(T)), 2);
		}else
			return 0;

	return sqrt (dist);
}


/*
 * returns balance
 */
template<class T> inline int TreeNode<T>::operator % ( T *nd_key ) const {
	int cmp = 0;
	int offset = 1;

	for (int j=0; j<dims; ++j) {
		if ( nd_key [j] >= key [j] )
			cmp += offset;
		offset = (offset<<1);
	}
	return cmp;
}

template<class T> inline bool TreeNode<T>::isCompatible ( TreeNode &nd ) const {
	for (int j=0; j<sons; ++j)
		if ( this->son[j] != 0 && nd % this->son[j]->key != j )
			return false;
	return true;
}

template<class T> Dtree<T>::~Dtree<T>() {
	if ( root != 0 )
		dropTree ( *root );
}

template<class T> inline void Dtree<T>::dropTree( TreeNode<T> &nd ) const {
	for (int j=0; j<sons; ++j)
		if ( nd.son[j] != 0 )
			dropTree ( *nd.son[j] );

	delete &nd;
}

template<class T> TreeNode<T>* Dtree<T>::search ( T* query ) const {
	if ( root == 0 )
		return 0;

	for (TreeNode<T>*ptr = root; ptr != 0; ptr = ptr->son[ *ptr % query ])
		if ( *ptr == query )
			return ptr;
	return 0;
}

template<class T> void Dtree<T>::push ( T *new_key , char* val) {
	TreeNode<T> *new_node = new TreeNode<T> ( dims, new_key, val );

	++node_counter;

	if (root == 0) {
		root = new_node;
		return;
	}

	TreeNode<T> *ptr=root;
	int cmp = *ptr % new_key;

	while ( ptr -> son [cmp] != 0 ) {
		ptr = ptr -> son [cmp];
		cmp = *ptr % new_key;
	}

	new_node -> parent = ptr;
	ptr -> son [cmp] = new_node;
}

template<class T> char* Dtree<T>::pop ( T *query ) {
	TreeNode<T>* nd = search ( query );
	if ( nd == 0 )
		return 0;

	char* ret_val = nd->val;

	TreeNode<T>* substitute = min_leaf ( nd->son[sons-1] );
	if ( substitute == 0 || !nd->isCompatible(*substitute) )
		for (int j=sons-2; j>=0; --j) {
			substitute =  max_leaf ( nd->son[j] );
			if ( substitute != 0 && nd->isCompatible(*substitute) )
				break;
		}

	if ( substitute != 0 ) {
		for (int j=0; j<dims; ++j)
			nd->key[j] = substitute->key[j];

		for (int j=0; j<sons; ++j)
			if ( substitute->parent->son[j] == substitute ) {
				substitute->parent->son[j] = 0;
				break;
			}
		//substitute->parent->son [ *substitute->parent % substitute->key ] = 0;
		delete substitute;
	}else{
		if ( nd == root ) {
			root = 0;
		}else{
			for (int j=0; j<sons; ++j)
				if (nd->parent->son[j] == nd) {
					nd->parent->son[j] = 0;
					break;
				}
			//nd->parent->son [ *nd->parent % query ] = 0;
		}
		delete nd;
	}
	--node_counter;
	return ret_val;
}

template<class T> void Dtree<T>::range ( T *lo, T *hi, std::vector<std::pair<T*,char*>*>& ans ) const {
	if ( root != 0 )
		range ( lo, hi, ans, *root );
}

template<class T> inline void Dtree<T>::range ( T *lo, T *hi, std::vector<std::pair<T*,char*>*>& ans, TreeNode<T>& subtree ) const {
	if ( subtree >= lo && subtree <= hi )
		ans.push_back( new std::pair<T*,char*> ( subtree.key, subtree.val ) );

	for (int j = subtree % lo; j <= subtree % hi; ++j)
		if ( subtree.son[j] != 0 )
			range ( lo, hi, ans, *subtree.son[j]);
}

template<class T> double Dtree<T>::nearest ( T *center, unsigned K, double radius, std::vector<std::pair<double,std::pair<T*,char*>*>*> &ans ) const {
	if ( root == 0 )
		return radius;

	std::priority_queue<std::pair<double,std::pair<T*,char*>*>*, std::vector<std::pair<double,std::pair<T*,char*>*>*>, dist_comparison<T> > sorted ( dist_comparison<T>(false) );

	nearest ( center, K, radius, sorted, *root );  // recursive

	if ( sorted.empty() )
		return radius;

	double Rmax = 0;

	while ( ! sorted.empty() ) {
		ans.push_back( new std::pair<double,std::pair<T*,char*>*> ( sorted.top()->first, sorted.top()->second ) );

		if ( sorted.top()->first > Rmax )
			Rmax =  sorted.top()->first;

		sorted.pop();
	}

	return Rmax;
}

template<class T> void Dtree<T>::nearest ( T *center,
					unsigned K,
					double radius,
					std::priority_queue <std::pair<double,std::pair<T*,char*>*>*,std::vector<std::pair<double,std::pair<T*,char*>*>*>,dist_comparison<T> >& sorted,
					TreeNode<T>& subtree ) const {

	bool is_node_qualified = false;
	double node_distance = subtree.dist ( center );

	if ( node_distance <= radius ) {
		if ( sorted.size() < K ) {
			sorted.push ( new std::pair <double,std::pair<T*,char*>*> ( node_distance, new std::pair<T*,char*> ( subtree.key, subtree.val ) ) );
			is_node_qualified = true;
		}else if ( ! sorted.empty() && node_distance < sorted.top()->first ) {
			sorted.pop ();
			sorted.push ( new std::pair <double,std::pair<T*,char*>*> ( node_distance, new std::pair<T*,char*> ( subtree.key, subtree.val ) ) );
			is_node_qualified = true;
		}
	}

	int opp_pos = sons - 1 - subtree % center;

	for ( int j=0; j<sons; ++j ) {
		if ( subtree.son[j] != 0 ) {
			if ( ! is_node_qualified && opp_pos == j )
				continue;

			nearest ( center, K, radius, sorted, *subtree.son[j] );
		}
	}
}

template<class T> std::vector<std::pair<T*,char*>*>& Dtree<T>::order ( int dim ) const {
	std::priority_queue<std::pair<T*,char*>*, std::vector<std::pair<T*,char*>*>, bpair_comparison<T> > queue ( bpair_comparison<T>(false, dim) );
	traverse ( *root, queue );

	std::vector<std::pair<T*,char*>*> *ans = new std::vector<std::pair<T*,char*>*>;
	for (unsigned j=0; j<node_counter; ++j) {
		ans->push_back ( queue.top() );
		queue.pop ();
	}
	return *ans;
}

template<class T> inline void Dtree<T>::traverse ( TreeNode<T>& nd,
		std::priority_queue<std::pair<T*,char*>*, std::vector<std::pair<T*,char*>*>, bpair_comparison<T> >& queue ) const {

	for ( int j=sons-1; j>=0; --j )
		if ( nd.son[j] != 0 )
			traverse ( *nd.son[j] , queue );
	std::pair<T*,char*> *tuple = new std::pair<T*,char*> ( nd.key , nd.val);
	queue.push( tuple );
	/*
	for (int j=0; j<dims; ++j)
		std::cerr << nd.key[j] << " ";
	std::cerr << "\n";
	*/
}

template<class T> inline TreeNode<T>* Dtree<T>::min_leaf ( TreeNode<T>* subtree ) const {
	if ( subtree == 0 )
		return 0;

	for (int j=0; j<sons; ++j)
		if ( subtree->son[j] != 0 )
			return min_leaf ( subtree->son[j] );
	return subtree;
}

template<class T> inline TreeNode<T>* Dtree<T>::max_leaf ( TreeNode<T>* subtree ) const {
	if ( subtree == 0 )
		return 0;

	for (int j=sons-1; j>=0; --j)
		if ( subtree->son[j] != 0 )
			return max_leaf ( subtree->son[j] );
	return subtree;
}
