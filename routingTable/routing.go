package routingTable

import (
	"dht/remoteNode"
	"dht/util"
)

// DHT routing using a binary tree and no buckets.
//
// Nodes have IDs of 20-bytes. When looking up an util.InfoHash for itself or for a
// remote host, the nodes have to look in its routing table for the closest
// nodes and return them.
//
// The distance between a node and an util.InfoHash is the XOR of the respective
// strings. This means that 'sorting' nodes only makes sense with an util.InfoHash
// as the pivot. You can't pre-sort nodes in any meaningful way.
//
// Most bittorrent/kademlia DHT implementations use a mix of bit-by-bit
// comparison with the usage of buckets. That works very well. But I wanted to
// try something different, that doesn't use buckets. Buckets have a single ID
// and one calculates the distance based on that, speeding up lookups.
//
// I decIDed to lay out the routing table in a binary tree instead, which is
// more intuitive. At the moment, the implementation is a real tree, not a
// free-list, but it's performing well.
//
// All nodes are inserted in the binary tree, with a fixed height of 160 (20
// bytes). To lookup an util.InfoHash, I do an inorder traversal using the util.InfoHash
// bit for each level.
//
// In most cases the lookup reaches the bottom of the tree without hitting the
// target util.InfoHash, since in the vast majority of the cases it's not in my
// routing table. Then I simply continue the in-order traversal (but then to
// the 'left') and return after I collect the 8 closest nodes.
//
// To speed things up, I keep the tree as short as possible. The path to each
// node is compressed and later uncompressed if a collision happens when
// inserting another node.
//
// I don't know how slow the overall algorithm is compared to a implementation
// that uses buckets, but for what is worth, the routing table lookups don't
// even show on the CPU profiling anymore.

type nTree struct {
	zero, one *nTree
	value     *remoteNode.RemoteNode
}

// recursive version of node insertion.
func (n *nTree) Insert(newNode *remoteNode.RemoteNode) {
	n.Put(newNode, 0)
}

func (n *nTree) BranchOut(n1, n2 *remoteNode.RemoteNode, i int) {
	// Since they are branching out it's guaranteed that no other nodes
	// exist below this branch currently, so just create the respective
	// nodes until their respective bits are different.
	chr := byte(n1.ID[i/8])
	bitPos := byte(i % 8)
	bit := (chr << bitPos) & 128

	chr2 := byte(n2.ID[i/8])
	bitPos2 := byte(i % 8)
	bit2 := (chr2 << bitPos2) & 128

	if bit != bit2 {
		n.Put(n1, i)
		n.Put(n2, i)
		return
	}

	// IDentical bits.
	if bit != 0 {
		n.one = &nTree{}
		n.one.BranchOut(n1, n2, i+1)
	} else {
		n.zero = &nTree{}
		n.zero.BranchOut(n1, n2, i+1)
	}
}

func (n *nTree) Put(newNode *remoteNode.RemoteNode, i int) {
	if i >= len(newNode.ID)*8 {
		// Replaces the existing value, if any.
		n.value = newNode
		return
	}

	if n.value != nil {
		if n.value.ID == newNode.ID {
			// Replace existing compressed value.
			n.value = newNode
			return
		}
		// Compression collision. Branch them out.
		old := n.value
		n.value = nil
		n.BranchOut(newNode, old, i)
		return
	}

	chr := byte(newNode.ID[i/8])
	bit := byte(i % 8)
	if (chr<<bit)&128 != 0 {
		if n.one == nil {
			n.one = &nTree{value: newNode}
			return
		}
		n.one.Put(newNode, i+1)
	} else {
		if n.zero == nil {
			n.zero = &nTree{value: newNode}
			return
		}
		n.zero.Put(newNode, i+1)
	}
}

func (n *nTree) Lookup(ID util.InfoHash) []*remoteNode.RemoteNode {
	ret := make([]*remoteNode.RemoteNode, 0, util.KNodes)
	if n == nil || ID == "" {
		return nil
	}
	return n.Traverse(ID, 0, ret, false)
}

func (n *nTree) LookupFiltered(ID util.InfoHash) []*remoteNode.RemoteNode {
	ret := make([]*remoteNode.RemoteNode, 0, util.KNodes)
	if n == nil || ID == "" {
		return nil
	}
	return n.Traverse(ID, 0, ret, true)
}

func (n *nTree) Traverse(ID util.InfoHash, i int, ret []*remoteNode.RemoteNode, filter bool) []*remoteNode.RemoteNode {
	if n == nil {
		return ret
	}
	if n.value != nil {
		if !filter || n.IsOK(ID) {
			return append(ret, n.value)
		}
	}
	if i >= len(ID)*8 {
		return ret
	}
	if len(ret) >= util.KNodes {
		return ret
	}

	chr := byte(ID[i/8])
	bit := byte(i % 8)

	// This is not needed, but it's clearer.
	var left, right *nTree
	if (chr<<bit)&128 != 0 {
		left = n.one
		right = n.zero
	} else {
		left = n.zero
		right = n.one
	}

	ret = left.Traverse(ID, i+1, ret, filter)
	if len(ret) >= util.KNodes {
		return ret
	}
	return right.Traverse(ID, i+1, ret, filter)
}

// cut goes down the tree and deletes the children nodes if all their leaves
// became empty.
func (n *nTree) Cut(ID util.InfoHash, i int) (cutMe bool) {
	if n == nil {
		return true
	}
	if i >= len(ID)*8 {
		return true
	}
	chr := byte(ID[i/8])
	bit := byte(i % 8)

	if (chr<<bit)&128 != 0 {
		if n.one.Cut(ID, i+1) {
			n.one = nil
			if n.zero == nil {
				return true
			}
		}
	} else {
		if n.zero.Cut(ID, i+1) {
			n.zero = nil
			if n.one == nil {
				return true
			}
		}
	}

	return false
}

func (n *nTree) IsOK(ih util.InfoHash) bool {
	if n.value == nil || n.value.ID == "" {
		return false
	}
	r := n.value

	if len(r.PendingQueries) > util.MaxNodePendingQueries {
		return false
	}

	return !r.WasContactedRecently(ih)
}

func CommonBits(s1, s2 string) int {
	// copied from jch's dht.cc.
	ID1, ID2 := []byte(s1), []byte(s2)

	i := 0
	for ; i < 20; i++ {
		if ID1[i] != ID2[i] {
			break
		}
	}

	if i == 20 {
		return 160
	}

	xor := ID1[i] ^ ID2[i]

	j := 0
	for (xor & 0x80) == 0 {
		xor <<= 1
		j++
	}
	return 8*i + j
}
