# network-mergeSort

This is a program that sorts a given set of numbers over a network using merge sort algorithm. The hosts in the network are connected in a **ring topoology**.

Virtually, the hosts are considered as the nodes of a binomial tree with a root node(based on root-node-id). Each node receives a number from the set and sends the sorted subset produced by the subtree with itself as the root to its parent.

The root node(root-node-id) runs **coordinator.c** while every other node runs **node.c**. All I/O multiplexing is done using the select() call.

For further details, look at **DesignDoc.pdf**.

To execute:  
$ gcc node.c -o node  
$ gcc coordinator.c  
$ ./a.out <total-num-of-nodes> <root-node-id> <input-file>
