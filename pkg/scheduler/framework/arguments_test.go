/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package framework

import (
	"testing"
)

type GetIntTestCases struct {
	arg         Arguments
	key         string
	baseValue   int
	expectValue int
}

type GetBoolTestCases struct {
	arg         Arguments
	key         string
	baseValue   bool
	expectValue bool
}

func TestArgumentsGetInt(t *testing.T) {
	key1 := "intkey"

	cases := []GetIntTestCases{
		{
			arg: Arguments{
				"anotherkey": "12",
			},
			key:         key1,
			baseValue:   10,
			expectValue: 10,
		},
		{
			arg: Arguments{
				key1: "15",
			},
			key:         key1,
			baseValue:   10,
			expectValue: 15,
		},
		{
			arg: Arguments{
				key1: "errorvalue",
			},
			key:         key1,
			baseValue:   11,
			expectValue: 11,
		},
		{
			arg: Arguments{
				key1: "",
			},
			key:         key1,
			baseValue:   0,
			expectValue: 0,
		},
	}

	for index, c := range cases {
		baseValue := c.baseValue
		c.arg.GetInt(nil, c.key)
		c.arg.GetInt(&baseValue, c.key)
		if baseValue != c.expectValue {
			t.Errorf("index %d, value should be %v, but not %v", index, c.expectValue, baseValue)
		}
	}
}

func TestArgumentsGetBool(t *testing.T) {
	key := "boolkey"

	cases := []GetBoolTestCases{
		{
			arg: Arguments{
				"anotherkey": "false",
			},
			key:         key,
			baseValue:   false,
			expectValue: false,
		},
		{
			arg: Arguments{
				"anotherkey": "false",
			},
			key:         key,
			baseValue:   true,
			expectValue: true,
		},
		{
			arg: Arguments{
				key: "false",
			},
			key:         key,
			baseValue:   false,
			expectValue: false,
		},
		{
			arg: Arguments{
				key: "false",
			},
			key:         key,
			baseValue:   true,
			expectValue: false,
		},
		{
			arg: Arguments{
				key: "true",
			},
			key:         key,
			baseValue:   false,
			expectValue: true,
		},
		{
			arg: Arguments{
				key: "true",
			},
			key:         key,
			baseValue:   true,
			expectValue: true,
		},
		{
			arg: Arguments{
				key: "nonbool",
			},
			key:         key,
			baseValue:   false,
			expectValue: false,
		},
		{
			arg: Arguments{
				key: "nonbool",
			},
			key:         key,
			baseValue:   true,
			expectValue: true,
		},
		{
			arg: Arguments{
				key: "",
			},
			key:         key,
			baseValue:   false,
			expectValue: false,
		},
		{
			arg: Arguments{
				key: "",
			},
			key:         key,
			baseValue:   true,
			expectValue: true,
		},
	}

	for index, c := range cases {
		baseValue := c.baseValue
		c.arg.GetBool(nil, c.key)
		c.arg.GetBool(&baseValue, c.key)
		if baseValue != c.expectValue {
			t.Errorf("index %d, value should be %v, but not %v", index, c.expectValue, baseValue)
		}
	}
}
