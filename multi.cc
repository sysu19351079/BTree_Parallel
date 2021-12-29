#include "b_tree.h"


// pthread function
// each worker builds a subtree
static void* works(void* arg){
    thread_arg* argument = (thread_arg*)arg;
    int num_entries = argument->num_entries;
    int start_entry = argument->start_entry;
    int end_entry = start_entry + num_entries;
    const Result* table = argument->table;
    BTree* tree = argument->tree;
    pthread_mutex_t* lock = argument->lock;
    ret_arg* ret = (ret_arg*)malloc(sizeof(ret_arg));   //returns of each thread

    BIndexNode *index_child   = NULL;
	BIndexNode *index_prev_nd = NULL;
	BIndexNode *index_act_nd  = NULL;
	BLeafNode  *leaf_child    = NULL;
	BLeafNode  *leaf_prev_nd  = NULL;
	BLeafNode  *leaf_act_nd   = NULL;

    int   id    = -1;
	int   block = -1;
	float key   = MINREAL;

    bool first_node  = true;		// determine relationship of sibling
	int  start_block = 0;			// position of first node
	int  end_block   = 0;			// position of last node

    printf("loading data: %d ~ %d\n",&start_entry, &end_entry);
    for (int i = start_entry; i < end_entry; ++i) {
		id  = table[i].id_;
		key = table[i].key_;

		if (!leaf_act_nd) {
			leaf_act_nd = new BLeafNode();
            pthread_mutex_lock(lock);
			leaf_act_nd->init(0, tree);
            pthread_mutex_unlock(lock);

			if (first_node) {
				first_node  = false; // init <start_block>
				start_block = leaf_act_nd->get_block();
			}
			else {					// label sibling
				leaf_act_nd->set_left_sibling(leaf_prev_nd->get_block());
				leaf_prev_nd->set_right_sibling(leaf_act_nd->get_block());

				delete leaf_prev_nd; leaf_prev_nd = NULL;
			}
			end_block = leaf_act_nd->get_block();
		}							
		leaf_act_nd->add_new_child(id, key); // add new entry

		if (leaf_act_nd->isFull()) {// change next node to store entries
			leaf_prev_nd = leaf_act_nd;
			leaf_act_nd  = NULL;
		}
	}
    if (leaf_prev_nd != NULL) {
		delete leaf_prev_nd; leaf_prev_nd = NULL;
	}
	if (leaf_act_nd != NULL) {
		delete leaf_act_nd; leaf_act_nd = NULL;
	}
    ret->start.push_back(start_block);
    ret->end.push_back(end_block);

    int current_level    = 1;		// current level (leaf level is 0)
	int last_start_block = start_block;	// build b-tree level by level
	int last_end_block   = end_block;	// build b-tree level by level

	while (last_end_block > last_start_block) {
		first_node = true;
		for (int i = last_start_block; i <= last_end_block; ++i) {
			block = i;				// get <block>
			if (current_level == 1) {
				leaf_child = new BLeafNode();
                pthread_mutex_lock(lock);
				leaf_child->init_restore(tree, block);
                pthread_mutex_unlock(lock);
				key = leaf_child->get_key_of_node();

				delete leaf_child; leaf_child = NULL;
			}
			else {
				index_child = new BIndexNode();
                pthread_mutex_lock(lock);
				index_child->init_restore(tree, block);
                pthread_mutex_unlock(lock);
				key = index_child->get_key_of_node();

				delete index_child; index_child = NULL;
			}

			if (!index_act_nd) {
				index_act_nd = new BIndexNode();
                pthread_mutex_lock(lock);
				index_act_nd->init(current_level, tree);
                pthread_mutex_unlock(lock);

				if (first_node) {
					first_node = false;
					start_block = index_act_nd->get_block();
				}
				else {
					index_act_nd->set_left_sibling(index_prev_nd->get_block());
					index_prev_nd->set_right_sibling(index_act_nd->get_block());

					delete index_prev_nd; index_prev_nd = NULL;
				}
				end_block = index_act_nd->get_block();
			}						
			index_act_nd->add_new_child(key, block); // add new entry

			if (index_act_nd->isFull()) {
				index_prev_nd = index_act_nd;
				index_act_nd = NULL;
			}
		}
		if (index_prev_nd != NULL) {// release the space
			delete index_prev_nd; index_prev_nd = NULL;
		}
		if (index_act_nd != NULL) {
			delete index_act_nd; index_act_nd = NULL;
		}

		ret->start.push_back(start_block);
        ret->end.push_back(end_block);
		last_start_block = start_block;// update info
		last_end_block = end_block;	// build b-tree of higher level
		++current_level;
	}
    ret->levels = current_level;
	ret->root = last_start_block;
	if (index_prev_nd != NULL) delete index_prev_nd; 
	if (index_act_nd  != NULL) delete index_act_nd;
	if (index_child   != NULL) delete index_child;
	if (leaf_prev_nd  != NULL) delete leaf_prev_nd; 
	if (leaf_act_nd   != NULL) delete leaf_act_nd; 	
	if (leaf_child    != NULL) delete leaf_child; 

    pthread_exit((void*)ret);
}


int BTree::bulkload_parallel(
    int n,
    const Result *table,
    int num_workers
)
{
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t* lock = &mutex;
    pthread_t* threads = (pthread_t*)malloc(num_workers * sizeof(pthread_t));
    thread_arg* args = (thread_arg*)malloc(num_workers * sizeof(thread_arg));

    if (threads == NULL || args == NULL){
        printf("create threads failed\n");
        return -1;
    }
    pthread_mutex_lock(lock);
    for (int i = 0; i < num_workers; i++){
        args[i].table = table;
        args[i].tree = this;
        args[i].lock = lock;
        args[i].start_entry = ceil(float(n) / num_workers) * i;
        if(i != num_workers - 1){
            args[i].num_entries = ceil(float(n) / num_workers);
        }
        else{
            args[i].num_entries = n - (num_workers - 1) * ceil(float(n) / num_workers);
        }

        if(pthread_create(&(threads[i]), NULL, &works, 
            (void*)(args + i)) != 0){
                printf("create failed!");
                return -1;
            }
    }
    pthread_mutex_unlock(lock);

    ret_arg* ret = (ret_arg*)malloc(num_workers * sizeof(ret_arg));
    for(int i = 0; i < num_workers; i++){
        pthread_join(threads[i], (void **)&(ret[i]));
    }

    int levels = ret[0].levels;
    for(int i = 0; i < num_workers; i++){
        assert(ret[i].levels == levels);
    }

    //connect the leftmost nodes and rightmost nodes
    for(int i = 0; i <= levels; i++){
        if(i == 0){
            BLeafNode* cur_first;
            BLeafNode* cur_last;
            BLeafNode* pre_first;
            BLeafNode* pre_last;
            for(int j = 0; j <= num_workers; j++){
                cur_first = new BLeafNode();
                cur_first->init_restore(this, ret[j].start[i]);
                cur_last = new BLeafNode();
                cur_last->init_restore(this, ret[j].end[i]);
                if(j != 0){
                    cur_first->set_left_sibling(pre_last->get_block());
                    pre_last->set_right_sibling(cur_first->get_block());
                    delete pre_first;
                    delete pre_last;
                }
                pre_first = cur_first;
                pre_last = cur_last;
            }
            delete pre_first;   pre_first = NULL;
            delete pre_last;    pre_last = NULL;
        }
        else{
            BIndexNode* cur_first;
            BIndexNode* cur_last;
            BIndexNode* pre_first;
            BIndexNode* pre_last;
            for (int j = 0; j <= num_workers; j++){
                cur_first = new BIndexNode();
                cur_first->init_restore(this, ret[j].start[i]);
                cur_last = new BIndexNode();
                cur_last->init_restore(this, ret[j].end[i]);
                if(j != 0){
                    cur_first->set_left_sibling(pre_last->get_block());
                    pre_last->set_right_sibling(cur_first->get_block());
                    delete pre_first;
                    delete pre_last;
                }
                pre_first = cur_first;
                pre_last = cur_last;
            }
            delete pre_first;   pre_first = NULL;
            delete pre_last;    pre_last = NULL;
        }
    }
    BIndexNode* root = new BIndexNode();
    root->init(levels + 1, this);
    for(int i = 0; i < num_workers; i++){
        BIndexNode* son = new BIndexNode();
        son->init_restore(this, ret[i].start[levels]);
        root->add_new_child(son->get_key_of_node(), son->get_block());
        delete son;
    }
    root_ = root->get_block();
    delete root; root = NULL;
    return 0;
}