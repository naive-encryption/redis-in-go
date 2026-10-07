package store

import (
	"fmt"
	"math"
	"math/rand/v2"
)

const zskiplistMaxLevel = 32

type Node struct {
	member string
	score  float64

	forward []*Node
	span    []int
}

type SkipList struct {
	head     *Node
	maxLevel int
	length   int

	dictionary map[string]*Node
}

func NewSkipList() *SkipList {
	head := &Node{score: math.Inf(-1), forward: make([]*Node, zskiplistMaxLevel), span: make([]int, zskiplistMaxLevel)}
	return &SkipList{head: head, maxLevel: zskiplistMaxLevel, dictionary: make(map[string]*Node)}
}

func (sl *SkipList) determNodeMaxLevel() int {
	level := 1
	for rand.Float64() < 0.5 && level < sl.maxLevel {
		level++
	}
	return level
}

func (sl *SkipList) NewNode(member string, score float64, maxLevel int) *Node {
	nodeMaxLevel := sl.determNodeMaxLevel()
	return &Node{member: member, score: score, forward: make([]*Node, nodeMaxLevel), span: make([]int, nodeMaxLevel)}
}

func (sl *SkipList) RemoveElementByName(member string) {
	element, exists := sl.dictionary[member]
	if !exists {
		return
	}

	update := make([]*Node, sl.maxLevel)
	curr := sl.head

	for i := sl.maxLevel - 1; i >= 0; i-- {
		for curr.forward[i] != nil && before(curr.forward[i], element) {
			curr = curr.forward[i]
		}
		update[i] = curr
	}

	for i := 0; i < sl.maxLevel-1; i++ {
		if update[i].forward[i] == element {
			update[i].span[i] += element.span[i] - 1
			update[i].forward[i] = element.forward[i]
		} else {
			update[i].span[i]--
		}
	}

	sl.length--
	delete(sl.dictionary, member)
}

func before(a, b *Node) bool {
	return a.score < b.score || (a.score == b.score && a.member < b.member)
}

func (sl *SkipList) InsertElement(member string, score float64) {
	var newNode *Node

	if oldNode, exists := sl.dictionary[member]; exists {
		newNode = oldNode
		newNode.score = score
		return
	} else {
		newNode = sl.NewNode(member, score, sl.maxLevel)
		sl.length++
	}

	nodeHeight := len(newNode.forward)

	update := make([]*Node, sl.maxLevel)
	rank := make([]int, sl.maxLevel)

	curr := sl.head
	for i := sl.maxLevel - 1; i >= 0; i-- {
		if i == sl.maxLevel-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for curr.forward[i] != nil && (curr.forward[i].score < score || (curr.forward[i].score == score && curr.forward[i].member < member)) {
			rank[i] += curr.span[i]
			curr = curr.forward[i]
		}
		update[i] = curr
	}

	for i := 0; i < sl.maxLevel; i++ {
		if i < nodeHeight {
			if update[i].forward[i] == nil {
				update[i].span[i] = sl.length + 1 - rank[i]
			}
			newNode.span[i] = update[i].span[i] - (rank[0] - rank[i])
			update[i].span[i] = rank[0] - rank[i] + 1

			newNode.forward[i] = update[i].forward[i]
			update[i].forward[i] = newNode
		} else {
			update[i].span[i]++
		}
	}

	sl.dictionary[member] = newNode
	// sl.PrintList()
}

func (sl SkipList) FindElementRank(member string) int {
	node, exists := sl.dictionary[member]
	if !exists {
		return 0
	}

	targetScore := node.score
	rank := 0
	curr := sl.head

	for i := sl.maxLevel - 1; i >= 0; i-- {
		for curr.forward[i] != nil && (curr.forward[i].score < targetScore || (curr.forward[i].score == targetScore && curr.forward[i].member <= member)) {
			rank += curr.span[i]
			curr = curr.forward[i]
		}
	}
	return rank - 1
}

func (sl SkipList) GetElement(rank int) *Node {
	rank++ // to account for 1-based indexing
	if rank < 0 || rank >= sl.length {
		return nil
	}

	curr := sl.head
	currRank := 0

	for i := sl.maxLevel - 1; i >= 0; i-- {
		for curr.forward[i] != nil && (currRank+curr.span[i]) <= rank {
			currRank += curr.span[i]
			curr = curr.forward[i]
		}
	}
	return curr
}

func (sl *SkipList) PrintList() {
	fmt.Println("\n--- Skip List Structure ---")
	for i := sl.maxLevel - 1; i >= 0; i-- {
		fmt.Printf("Level %d: head -> ", i)
		curr := sl.head.forward[i]
		for curr != nil {
			fmt.Printf("[%s: %.2f] -> ", curr.member, curr.score)
			curr = curr.forward[i]
		}
		fmt.Println("nil")
	}
	fmt.Println("---------------------------")
}
