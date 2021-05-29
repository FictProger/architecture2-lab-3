package main

import (
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestBalancer(c *C) {
	dataset := []struct {
		PoolTraffic []int
		PoolAlive   []bool
		ResultIndex int
		IsError     bool
	}{
		{
			PoolTraffic: []int{0, 0, 0},
			PoolAlive:   []bool{true, true, true},
			ResultIndex: 0,
			IsError:     false,
		},
		{
			PoolTraffic: []int{10, 0, 0},
			PoolAlive:   []bool{true, true, true},
			ResultIndex: 1,
			IsError:     false,
		},
		{
			PoolTraffic: []int{10, 10, 0},
			PoolAlive:   []bool{true, true, true},
			ResultIndex: 2,
			IsError:     false,
		},
		{
			PoolTraffic: []int{10, 10, 0},
			PoolAlive:   []bool{false, true, true},
			ResultIndex: 2,
			IsError:     false,
		},
		{
			PoolTraffic: []int{10, 10, 0},
			PoolAlive:   []bool{false, true, false},
			ResultIndex: 1,
			IsError:     false,
		},
		{
			PoolAlive: []bool{false, false, false},
			IsError:   true,
		},
	}

	for _, v := range dataset {
		res, err := chooseServer(v.PoolTraffic, &v.PoolAlive)
		c.Assert(err != nil, Equals, v.IsError)
		c.Log(res)
		if !v.IsError {
			c.Assert(res, Equals, v.ResultIndex)
		}
	}
}
