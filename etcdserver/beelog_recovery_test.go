package etcdserver

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"go.etcd.io/etcd/raft/raftpb"
)

func TestWALNameAscSor(t *testing.T) {
	testCases := []struct {
		name     string
		walNames []string
		expected []string
	}{
		{
			"3-size slice",
			[]string{
				"2-5-2.log",
				"6-9-3.log",
				"1-1-1.log",
			},
			[]string{
				"1-1-1.log",
				"2-5-2.log",
				"6-9-3.log",
			},
		},
		{
			"already sorted slice",
			[]string{
				"1-1-1.log",
				"2-304-101.log",
				"305-604-274.log",
				"605-1199-512.log",
			},
			[]string{
				"1-1-1.log",
				"2-304-101.log",
				"305-604-274.log",
				"605-1199-512.log",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sort.Sort(SortByWALNameAsc(tc.walNames))
			if !reflect.DeepEqual(tc.walNames, tc.expected) {
				t.Fatal("expected slice is different")
			}
		})
	}
}

func TestWALNameDescSor(t *testing.T) {
	testCases := []struct {
		name     string
		walNames []string
		expected []string
	}{
		{
			"3-size slice",
			[]string{
				"2-5-2.log",
				"6-9-3.log",
				"1-1-1.log",
			},
			[]string{
				"6-9-3.log",
				"2-5-2.log",
				"1-1-1.log",
			},
		},
		{
			"already sorted slice",
			[]string{
				"605-1199-512.log",
				"305-604-274.log",
				"2-304-101.log",
				"1-1-1.log",
			},
			[]string{
				"605-1199-512.log",
				"305-604-274.log",
				"2-304-101.log",
				"1-1-1.log",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sort.Sort(SortByWALNameDesc(tc.walNames))
			if !reflect.DeepEqual(tc.walNames, tc.expected) {
				t.Fatal("expected slice is different")
			}
		})
	}
}

func TestReverseEntrySlice(t *testing.T) {
	testCases := []struct {
		name     string
		input    []raftpb.Entry
		expected []raftpb.Entry
	}{
		{
			"odd size slice",
			[]raftpb.Entry{
				{Index: 1},
				{Index: 2},
				{Index: 3},
				{Index: 4},
				{Index: 5},
			},
			[]raftpb.Entry{
				{Index: 5},
				{Index: 4},
				{Index: 3},
				{Index: 2},
				{Index: 1},
			},
		},
		{
			"even size slice",
			[]raftpb.Entry{
				{Index: 4},
				{Index: 3},
				{Index: 2},
				{Index: 1},
			},
			[]raftpb.Entry{
				{Index: 1},
				{Index: 2},
				{Index: 3},
				{Index: 4},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reverseEntrySlice(tc.input)
			if !reflect.DeepEqual(tc.input, tc.expected) {
				t.Fatal("expected slice is different")
			}
		})
	}
}

func BenchmarkSortAlgorithmsOnWALNamesDesc(b *testing.B) {
	input := getRandAscWALNames(300, 100000)

	b.Run("desc", func(b *testing.B) {
		sort.Sort(SortByWALNameDesc(input))
	})

	b.Run("stable desc", func(b *testing.B) {
		sort.Stable(SortByWALNameDesc(input))
	})

	b.Run("asc reversed", func(b *testing.B) {
		sort.Sort(sort.Reverse(SortByWALNameAsc(input)))
	})

	b.Run("asc stable reversed", func(b *testing.B) {
		sort.Stable(sort.Reverse(SortByWALNameAsc(input)))
	})
}

func getRandAscWALNames(batchSize, n int) []string {
	names := make([]string, 0, n)
	cur := 0
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("%d-%d-%d.log", cur+1, cur+batchSize, rand.Intn(batchSize))
		names = append(names, name)
		cur += batchSize
	}
	return names
}
