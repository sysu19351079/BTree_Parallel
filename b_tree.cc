#include "b_tree.h"

// -----------------------------------------------------------------------------
//  BTree: b-tree to index hash values produced by qalsh
// -----------------------------------------------------------------------------
BTree::BTree()						// default constructor
{
	root_     = -1;
	file_     = NULL;
	root_ptr_ = NULL;
}

// -----------------------------------------------------------------------------
BTree::~BTree()						// destructor
{
	char *header = new char[file_->get_blocklength()];
	write_header(header);			// write <root_> to <header>
	file_->set_header(header);		// write back to disk
	delete[] header; header = NULL;

	if (root_ptr_ != NULL) {
		delete root_ptr_; root_ptr_ = NULL;
	}
	if (file_ != NULL) {
		delete file_; file_ = NULL;
	}
}

// -----------------------------------------------------------------------------
void BTree::init(					// init a new tree
	int   b_length,						// block length
	const char *fname)					// file name
{
	FILE *fp = fopen(fname, "r");
	if (fp) {						// check whether the file exist
		fclose(fp);					// ask whether replace?
		// printf("The file \"%s\" exists. Replace? (y/n)", fname);

		// char c = getchar();			// input 'Y' or 'y' or others
		// getchar();					// input <ENTER>
		// assert(c == 'y' || c == 'Y');
		remove(fname);				// otherwise, remove existing file
	}			
	file_ = new BlockFile(b_length, fname); // b-tree stores here

	// -------------------------------------------------------------------------
	//  init the first node: to store <blocklength> (page size of a node),
	//  <number> (number of nodes including both index node and leaf node), 
	//  and <root> (address of root node)
	// -------------------------------------------------------------------------
	root_ptr_ = new BIndexNode();
	root_ptr_->init(0, this);
	//返回BIndexNode中的变量block_
	root_ = root_ptr_->get_block();
	//释放root-ptr的内存
	delete_root();
}

// -----------------------------------------------------------------------------
void BTree::init_restore(			// load the tree from a tree file
	const char *fname)					// file name
{
	FILE *fp = fopen(fname, "r");	// check whether the file exists
	if (!fp) {
		printf("tree file %s does not exist\n", fname);
		exit(1);
	}
	fclose(fp);

	// -------------------------------------------------------------------------
	//  it doesn't matter to initialize blocklength to 0.
	//  after reading file, <blocklength> will be reinitialized by file.
	// -------------------------------------------------------------------------
	file_ = new BlockFile(0, fname);
	root_ptr_ = NULL;

	// -------------------------------------------------------------------------
	//  read the content after first 8 bytes of first block into <header>
	// -------------------------------------------------------------------------
	char *header = new char[file_->get_blocklength()];
	file_->read_header(header);		// read remain bytes from header
	read_header(header);			// init <root> from <header>

	delete[] header; header = NULL;
}

// -----------------------------------------------------------------------------
int BTree::bulkload(				// bulkload a tree from memory
	int   n,							// number of entries
	const Result *table)				// hash table
{
	BIndexNode *index_child   = NULL;
	BIndexNode *index_prev_nd = NULL;
	BIndexNode *index_act_nd  = NULL;
	BLeafNode  *leaf_child    = NULL;
	BLeafNode  *leaf_prev_nd  = NULL;
	BLeafNode  *leaf_act_nd   = NULL;

	int   id    = -1;
	int   block = -1;
	float key   = MINREAL;

	// -------------------------------------------------------------------------
	//  build leaf node from <_hashtable> (level = 0)
	// -------------------------------------------------------------------------
	bool first_node  = true;		// determine relationship of sibling
	int  start_block = 0;			// position of first node
	int  end_block   = 0;			// position of last node

	for (int i = 0; i < n; ++i) {
		id  = table[i].id_;
		key = table[i].key_;

		if (!leaf_act_nd) {
			leaf_act_nd = new BLeafNode();
			leaf_act_nd->init(0, this);

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

	// -------------------------------------------------------------------------
	//  stop condition: lastEndBlock == lastStartBlock (only one node, as root)
	// -------------------------------------------------------------------------
	int current_level    = 1;		// current level (leaf level is 0)
	int last_start_block = start_block;	// build b-tree level by level
	int last_end_block   = end_block;	// build b-tree level by level

	while (last_end_block > last_start_block) {
		first_node = true;
		for (int i = last_start_block; i <= last_end_block; ++i) {
			block = i;				// get <block>
			if (current_level == 1) {
				leaf_child = new BLeafNode();
				leaf_child->init_restore(this, block);
				key = leaf_child->get_key_of_node();

				delete leaf_child; leaf_child = NULL;
			}
			else {
				index_child = new BIndexNode();
				index_child->init_restore(this, block);
				key = index_child->get_key_of_node();

				delete index_child; index_child = NULL;
			}

			if (!index_act_nd) {
				index_act_nd = new BIndexNode();
				index_act_nd->init(current_level, this);

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
		
		last_start_block = start_block;// update info
		last_end_block = end_block;	// build b-tree of higher level
		++current_level;
	}
	root_ = last_start_block;		// update the <root>

	if (index_prev_nd != NULL) delete index_prev_nd; 
	if (index_act_nd  != NULL) delete index_act_nd;
	if (index_child   != NULL) delete index_child;
	if (leaf_prev_nd  != NULL) delete leaf_prev_nd; 
	if (leaf_act_nd   != NULL) delete leaf_act_nd; 	
	if (leaf_child    != NULL) delete leaf_child; 

	return 0;
}

// -----------------------------------------------------------------------------
void BTree::load_root() 		// load root of b-tree
{	
	if (root_ptr_ == NULL) {
		root_ptr_ = new BIndexNode();
		root_ptr_->init_restore(this, root_);
	}
}

// -----------------------------------------------------------------------------
void BTree::delete_root()		// delete root of b-tree
{
	if (root_ptr_ != NULL) { delete root_ptr_; root_ptr_ = NULL; }
}



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
	std::set <int> myblocks;
	int device = end_entry / num_entries;

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
	int start = 0;
	int end = 0;
    printf("loading data: %d ~ %d\n",start_entry, end_entry);
    for (int i = start_entry; i < end_entry; ++i) {
		id  = table[i].id_;
		key = table[i].key_;
		if (!leaf_act_nd) {
			leaf_act_nd = new BLeafNode();
            pthread_mutex_lock(lock);
			leaf_act_nd->init(0, tree);
            pthread_mutex_unlock(lock);
			myblocks.insert(leaf_act_nd->get_block());	//insert block

			if (first_node) {
				first_node  = false; // init <start_block>
				start_block = leaf_act_nd->get_block();
			}
			else {					// label sibling
				leaf_act_nd->set_left_sibling(leaf_prev_nd->get_block());
				leaf_prev_nd->set_right_sibling(leaf_act_nd->get_block());
				pthread_mutex_lock(lock);
				delete leaf_prev_nd; leaf_prev_nd = NULL;
            	pthread_mutex_unlock(lock);
				
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
		pthread_mutex_lock(lock);
		delete leaf_prev_nd; leaf_prev_nd = NULL;		
        pthread_mutex_unlock(lock);
	}
	if (leaf_act_nd != NULL) {
		pthread_mutex_lock(lock);
		delete leaf_act_nd; leaf_act_nd = NULL;		
        pthread_mutex_unlock(lock);
	}
    ret->start[start++] = start_block;
    ret->end[end++] = end_block;

    int current_level    = 1;		// current level (leaf level is 0)
	int last_start_block = start_block;	// build b-tree level by level
	int last_end_block   = end_block;	// build b-tree level by level
	
	while (last_end_block > last_start_block) {
		
		first_node = true;


		for (int i = last_start_block; i <= last_end_block; ++i) {
			if(myblocks.find(i)==myblocks.end()) continue;
			block = i;				// get <block>
			if (current_level == 1) {
				leaf_child = new BLeafNode();
                pthread_mutex_lock(lock);
				leaf_child->init_restore(tree, block);
				key = leaf_child->get_key_of_node();
				delete leaf_child; leaf_child = NULL;
            	pthread_mutex_unlock(lock);
				
			}
			else {
				index_child = new BIndexNode();
                pthread_mutex_lock(lock);
				index_child->init_restore(tree, block);
				key = index_child->get_key_of_node();
				delete index_child; index_child = NULL;
            	pthread_mutex_unlock(lock);
				
			}

			if (!index_act_nd) {
				index_act_nd = new BIndexNode();
                pthread_mutex_lock(lock);
				index_act_nd->init(current_level, tree);
                pthread_mutex_unlock(lock);
				myblocks.insert(index_act_nd->get_block());
				if (first_node) {
					first_node = false;
					start_block = index_act_nd->get_block();
				}
				else {
					index_act_nd->set_left_sibling(index_prev_nd->get_block());
					index_prev_nd->set_right_sibling(index_act_nd->get_block());
					pthread_mutex_lock(lock);
					delete index_prev_nd; index_prev_nd = NULL;
            		pthread_mutex_unlock(lock);
					
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
			pthread_mutex_lock(lock);
			delete index_prev_nd; index_prev_nd = NULL;	
            pthread_mutex_unlock(lock);
			
		}
		if (index_act_nd != NULL) {
			pthread_mutex_lock(lock);
			delete index_act_nd; index_act_nd = NULL;	
            pthread_mutex_unlock(lock);
			
		}

		ret->start[start++] = start_block;
        ret->end[end++] = end_block;
		last_start_block = start_block;// update info
		last_end_block = end_block;	// build b-tree of higher level
		++current_level;
	}

    ret->levels = current_level;
	ret->root = last_start_block;
	pthread_mutex_lock(lock);
	if (index_prev_nd != NULL) delete index_prev_nd; 
	if (index_act_nd  != NULL) delete index_act_nd;
	if (index_child   != NULL) delete index_child;
	if (leaf_prev_nd  != NULL) delete leaf_prev_nd; 
	if (leaf_act_nd   != NULL) delete leaf_act_nd; 	
	if (leaf_child    != NULL) delete leaf_child; 			
    pthread_mutex_unlock(lock);
	
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
        return 1;
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
                return 1;
            }
    }
    pthread_mutex_unlock(lock);

    void* ret[16];
	ret_arg* ra[16];
    for(int i = 0; i < num_workers; i++){
        pthread_join(threads[i], &(ret[i]));
    }
	printf("threads complete\n");
	for(int i = 0; i < num_workers; i++) ra[i] = (ret_arg*)ret[i];
    int levels = ra[0]->levels;
    for(int i = 0; i < num_workers; i++){
        assert(ra[i]->levels == levels);
    }


    //connect the leftmost nodes and rightmost nodes
    for(int i = 0; i < levels; i++){
		
        if(i == 0){
            BLeafNode* cur_first;
            BLeafNode* cur_last;
            BLeafNode* pre_first;
            BLeafNode* pre_last;
            for(int j = 0; j < num_workers; j++){
                cur_first = new BLeafNode();
                cur_first->init_restore(this, ra[j]->start[i]);
                cur_last = new BLeafNode();
                cur_last->init_restore(this, ra[j]->end[i]);
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
            for (int j = 0; j < num_workers; j++){
                cur_first = new BIndexNode();
                cur_first->init_restore(this, ra[j]->start[i]);
                cur_last = new BIndexNode();
                cur_last->init_restore(this, ra[j]->end[i]);
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
    root->init(levels, this);
	
    for(int i = 0; i < num_workers; i++){
        BIndexNode* son = new BIndexNode();
        son->init_restore(this, ra[i]->start[levels]);
        root->add_new_child(son->get_key_of_node(), son->get_block());
        delete son;
    }
    root_ = root->get_block();
    delete root; root = NULL;
    return 0;
}