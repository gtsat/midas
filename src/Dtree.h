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

#ifndef DTREE_H_
#define DTREE_H_

#include "bpriority_queue.h"
#include <cstdlib>
#include <cstring>
#include <vector>

template<class T> struct TreeNode;

class comparison;

template<class T> class Dtree {

	TreeNode<T> *root;
	unsigned node_counter;

	int dims;
	int sons;

public:
	Dtree (int d) : root(0), node_counter(0), dims(d), sons(1<<d) {}
	~Dtree ();

	/* returns depth of new tree-node */
	void push ( T *key , char* val );

	/* returns value of the key or NULL */
	char* pop ( T *key );

	TreeNode<T>* search ( T* query ) const;

	void range ( T *lo, T *hi, std::vector<std::pair<T*,char*>*>& ans ) const;
	double nearest ( T *center, unsigned K, double Rmax, std::vector<std::pair<double,std::pair<T*,char*>*>*> &ans ) const;

private:
	void range ( T *lo, T *hi, std::vector<std::pair<T*,char*>*>& ans, TreeNode<T>& subtree ) const;
	void nearest ( T *center,
			unsigned K,
			double radius,
			std::priority_queue <std::pair<double,std::pair<T*,char*>*>*, std::vector<std::pair<double,std::pair<T*,char*>*>*>, dist_comparison<T> >& sorted,
			TreeNode<T> &subtree ) const;

public:

	/* auxiliary */
	unsigned get_size () const {return node_counter;}
	std::vector<std::pair<T*,char*>*>& order ( int dim ) const;

	TreeNode<T>* max_leaf () const {return max_leaf ( root );}
	TreeNode<T>* min_leaf () const {return min_leaf ( root );}

private:
	void dropTree ( TreeNode<T>& subtree ) const;
	void traverse ( TreeNode<T>& subtree,
			std::priority_queue<std::pair<T*,char*>*, std::vector<std::pair<T*,char*>*>, bpair_comparison<T> >& q ) const;

	TreeNode<T>* max_leaf(TreeNode<T>* subtree) const;
	TreeNode<T>* min_leaf(TreeNode<T>* subtree) const;
};

template<class T> struct TreeNode {
	T* key;

	TreeNode<T>** son;
	TreeNode<T>* parent;

	char* val;

	int dims;
	int sons;

	TreeNode ( int d, T* new_key, char* new_val ) : key(new_key), son(0), parent(0), val(new_val), dims(d), sons(1<<d) {
		son = (TreeNode<T>**) calloc (sons, sons*sizeof(TreeNode<T>*));
		//memset (son, 0, sons * sizeof(TreeNode<T>*));
	}

	~TreeNode () {free(key); free(val); free (son);}

	bool operator == (T *nd_key) const;
	bool operator != (T *nd_key) const;
	bool operator >= (T *nd_key) const;
	bool operator <= (T *nd_key) const;

	/*
	 * returns key distance
	 */
	double dist ( T *nd_key ) const;

	/*
	 *  returns balance
	 */
	int operator % ( T *nd_key ) const;

	bool isCompatible ( TreeNode& nd ) const;
};

#endif
