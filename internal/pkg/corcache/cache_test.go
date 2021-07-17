package corcache

import "testing"

func Test_InitCache(t *testing.T) {
	tests := []struct{
		input CacheSystemType
	}{ {Local}, }

	for i, test := range tests {
		cs,err := NewCacheSystem(test.input)
		if err != nil{
			t.Fatalf("failed to init cache for test %d with type %d",i,test.input)
		}
		if cs == nil{
			t.Fatalf("Expected non-nil cache return for test %d",i)
		}
	}
}

func Test_CacheSystemTypes(t *testing.T) {
	tests := []struct{
		impl CacheSystem
		expected CacheSystemType
	}{
		{NewLocalInMemoryProvider(10),Local},
		//TODO: add each new CachImpl here
	}

	for i,test := range tests {
		if CacheSystemTypes(test.impl) != test.expected {
			t.Fatalf("failed test %d could not match %+v to %d",i,test.impl,test.expected)
		}
	}
}
