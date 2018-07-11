#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

#define RED 1
#define BLACK 0


struct rbtNode {
    struct rbtNode *p;      // pointer to the father
    struct rbtNode *left;   // left son, smaller elements
    struct rbtNode *right;  // right son, greater elements
    bool color;
    int key;
};

struct rbTree {
    struct rbtNode *root;
    char *indexTree;
};

typedef struct rbtNode *node;
typedef struct rbTree * tree;


void leftRotate(tree T, node x);
void rightRotate(tree T, node x);
void rbtInsert(tree T, node z);
int rbtSearch(tree T, int key);
void rbtPrintKeys(tree T);
int nodeSearch(node temp, int key);
void nodePrintKeys(node temp);
void rbInsertFixup(tree T, node z);
node createNode(int key);


/* return nonzero if key is present in tree */
int rbtSearch(tree T, int key) {
    if (!T)
        return 0;
    return nodeSearch(T->root, key);
}

node createNode(int key) {
    node x;
    if (!(x = (node)malloc(sizeof(struct rbtNode)))) {
        return NULL;
    }
    x->key = key;
    return x;
}

/* insert a new element into a tree */
/* note *t is actual tree */
void rbtInsert(tree T, node z) {
    node y = NULL;
    node x = T->root;
    while (x != NULL) {
        y = x;
        if (z->key < x->key)
            x = x->left;
        else
            x = x->right;
    }
    z->p = y;
    if (y == NULL)
        T->root = z;
    else
        if (z->key < y->key)
            y->left = z;
        else
            y->right = z;
    z->left = NULL;
    z->right = NULL;
    z->color = RED;
    rbInsertFixup(T, z);
}

/* print all keys of the tree in order */
void rbtPrintKeys(tree T) {
    if (!T)
        return;
    nodePrintKeys(T->root);
}

int nodeSearch(node temp, int key) {
    // no result
    if (!temp)
        return 0;
    // node found
    if (temp->key == key)
        return 1;
    else
        // key is greater than current key: I have to look into the right child
        if (temp->key > key)
            return nodeSearch(temp->right, key);
    // key is smaller than current key: I have to look into the left child
        else
            return nodeSearch(temp->left, key);
}

void nodePrintKeys(node temp) {
    if (!temp)
        return;
    nodePrintKeys(temp->left);
    printf("%d\t", temp->key);
    nodePrintKeys(temp->right);
}

bool leftRotate(tree T, node x) {
    if( !T || !z ){return false;}
    node y = x->right;
    x->right = y->left;
    if (y->left != NULL)
        y->left->p = x;
    y->p = x->p;
    if (x->p == NULL)
        T->root = y;
    else
        if (x == x->p->left)
            x->p->left = y;
        else
            x->p->right = y;
    y->left = x;
    x->p = y;
}

bool rightRotate(tree T, node x) {
    if( !T || !z ){return false;}

    node y = x->left;
    x->left = y->right;
    if (y->right != NULL)
        y->right->p = x;
    y->p = x->p;
    if (x->p == NULL)
        T->root = y;
    else
        // x was the left son of his father
        if (x == x->p->left)
            x->p->left = y;
    // x was the right son of his father
        else
            x->p->right = y;
    y->right = x;
    x->p = y;
}

void rbInsertFixup(tree T, node z) {
    node y;

    while (z->p && z->p->color == RED) {
        if (z->p == z->p->p->left) {
            y = z->p->p->right;
            if (y && y->color == RED) {
                z->p->color = BLACK;
                y->color = BLACK;
                z->p->p->color = RED;
                z = z->p->p;
            }
            else {
                if (z == z->p->right) {
                    z = z->p;
                    leftRotate(T, z);
                }
                z->p->color = BLACK;
                z->p->p->color = RED;
                rightRotate(T, z->p->p);
            }
        }
        else {
            y = z->p->p->left;
            if (y && y->color == RED) {
                z->p->color = BLACK;
                y->color = BLACK;
                z->p->p->color = RED;
                z = z->p->p;
            }
            else {
                if (z == z->p->left) {
                    z = z->p;
                    rightRotate(T, z);
                }
                z->p->color = BLACK;
                z->p->p->color = RED;
                leftRotate(T, z->p->p);
            }
        }
    }
    T->root->color = BLACK;
}

int main() {
    //  #define _CRT_SECURE_NO_WARNINGS
    //  #define _CRT_SECURE_NO_WARNINGS_GLOBALS
    int temp;
    tree T;
    if (!(T = (tree)malloc(sizeof(struct rbTree)))) {
        return -1;
    }
    T->root = NULL;
    T->indexTree = NULL;
    while (true) {
        rbtPrintKeys(T);
        printf("\n");
        printf("Number: ");
        fflush(stdin);
        scanf("%d", &temp);
        rbtInsert(T, createNode(temp));
        system("clear");
    }
};