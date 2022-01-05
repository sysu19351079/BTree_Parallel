#include <iostream>
#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <string>
#include <set>
#include <fstream>
#include <sstream>

#include "def.h"
#include "util.h"
#include "random.h"
#include "pri_queue.h"
#include "b_node.h"
#include "b_tree.h"

using namespace std;

void print_tree(BTree* trees) {
	char print_file[200];
	strncpy(print_file, "./result/print_tree.txt", sizeof(print_file));
	printf("print_file = %s\n", print_file);

	FILE *fp = fopen(print_file, "w+");
	fclose(fp);
	fp = fopen(print_file, "a");

	BIndexNode *cur_node = NULL;
	BIndexNode *nxt_node = NULL;
	bool first_node;
	int num_entries = 0;

	int first_son_block = -1;
	// print the index nodes
	cur_node = new BIndexNode();
	cur_node->init_restore(trees, trees->root_);
	while (cur_node->get_level() != 0) {
		first_node = true;
		while (cur_node) {
			// print every index node in the tree
			if (cur_node->get_block() == trees->root_) {
				fprintf(fp, "Root: ");
			}
			fprintf(fp, "Block %d\n", cur_node->get_block());
			fprintf(fp, "\tlevel: %d\tnum_entries: %d\n", cur_node->get_level(), cur_node->get_num_entries());
			num_entries = cur_node->get_num_entries();
			for (int i = 0; i < num_entries; i++) {
				fprintf(fp, "\t\tkey: %d\tson: %d\n", (int)cur_node->get_key(i), cur_node->get_son(i));
			}

			// get first node in each level
			if (first_node) {
				first_node = false;
				first_son_block = cur_node->get_son(0);
			}
			nxt_node = cur_node->get_right_sibling();
			delete cur_node; cur_node = nxt_node;
		}
		if (first_son_block == 1) break;  // when meet with leaf node, break
		cur_node = new BIndexNode();
		cur_node->init_restore(trees, first_son_block);
	}
	if (cur_node) {
		delete cur_node; cur_node = NULL;
	}

	// print the leaf nodes
	BLeafNode *leaf_node = NULL;
	BLeafNode *next_node = NULL;
	int leaf_num_entries = 0;
	int leaf_num_keys = 0;
	leaf_node = new BLeafNode();
	leaf_node->init_restore(trees, 1);
	while (leaf_node) {
		fprintf(fp, "Leaf Block %d\n", leaf_node->get_block());
		fprintf(fp, "\tlevel: %d\tnum_keys: %d\tnum_entries: %d\n", leaf_node->get_level(), leaf_node->get_num_keys(), leaf_node->get_num_entries());
		leaf_num_entries = leaf_node->get_num_entries();
		leaf_num_keys = leaf_node->get_num_keys();
		for (int i = 0; i < leaf_num_entries; i++) {
			if (i%16 == 0) {
				fprintf(fp, "\t\tentry_id: %d\tkey: %d\n", leaf_node->get_entry_id(i), (int)leaf_node->get_key(i/16));
			}
			else {
				fprintf(fp, "\t\tentry_id: %d\n", leaf_node->get_entry_id(i));
			}
		}

		next_node = leaf_node->get_right_sibling();
		delete leaf_node; leaf_node = next_node;
	}
	if (leaf_node) {
		delete leaf_node; leaf_node = NULL;
	}

	fclose(fp);
}

// -----------------------------------------------------------------------------
int main(int argc, char **args)
{    
	int num_workers = atoi(args[1]);
	char data_file[200];
	char tree_file[200];
	int  B_ = 512; // node size
	int n_pts_ = atoi(args[2]);

	strncpy(data_file, "./data/dataset.csv", sizeof(data_file));
	strncpy(tree_file, "./result/B_tree", sizeof(tree_file));
	printf("data_file   = %s\n", data_file);
	printf("tree_file   = %s\n", tree_file);

	Result *table = new Result[n_pts_]; 
	ifstream fp(data_file); 
	string line;
	int i=0;
	while (getline(fp,line)){ 
        string number;
        istringstream readstr(line); 
        
		getline(readstr,number,','); 
		table[i].key_ = atof(number.c_str()); 

		getline(readstr,number,','); 
		table[i].id_ = atoi(number.c_str());    
        i++;
    }
	fp.close();

	timeval start_t;  
    timeval end_t;

	gettimeofday(&start_t,NULL);
	BTree* trees_ = new BTree();
	trees_->init(B_, tree_file);
	//对这个函数进行并行
	if(num_workers == 0){
		if(trees_->bulkload(n_pts_, table)) return 1;
	}
	else{
		if (trees_->bulkload_parallel(n_pts_, table, num_workers)) return 1;
	}
	
	delete[] table; table = NULL;

	gettimeofday(&end_t, NULL);

	float run_t1 = end_t.tv_sec - start_t.tv_sec + 
						(end_t.tv_usec - start_t.tv_usec) / 1000000.0f;
	printf("运行时间: %f  s\n", run_t1);
	
	print_tree(trees_);

	return 0;
}
