package corcache

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewLocalInMemoryProvider(t *testing.T) {
	local := NewLocalInMemoryProvider(100*64)

	assert.EqualValues(t,100*64, local.maxSize,)
	assert.Zero(t,local.size)
	assert.NotNil(t,local.pool)
}


func TestLocalInMemoryProvider(t *testing.T) {
	local := NewLocalInMemoryProvider(100*64)

	RunTestCacheSystem(t,local)
}

func TestLocalCache_Init(t *testing.T) {
	local := NewLocalInMemoryProvider(100*64)

	err := local.Init()
	assert.Nil(t,err)
}

//we need to test prepare before we can test any of the other functions
func TestLocalCache_OpenWriter(t *testing.T) {
	c := NewLocalInMemoryProvider(100*64)
	local := prepare(t,c,"/")

	files, err := local.ListFiles("/*")
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, len(files), 10)
	for _, file := range files {
		assert.EqualValues(t, 5*8, file.Size)
	}


	w,err := local.OpenWriter("/test0")
	assert.Nil(t, err)
	binary.Write(w,binary.LittleEndian,uint64(0xc0ffee))
	w.Close()

	assert.EqualValues(t, 6*8, c.pool["/test0"].Len())
}

func TestLocalCache_WriteToMuch(t *testing.T) {
	local := NewLocalInMemoryProvider(10*5*8+1)
	local.Init()

	writeTo(local, "/", t, "")

	w,err := local.OpenWriter("/test_test")
	assert.Nil(t, err)

	_,err = w.Write(make([]byte,200))
	assert.NotNil(t, err)
}



func TestLocalCache_Undeploy(t *testing.T) {
	c := NewLocalInMemoryProvider(100*64)
	local := prepare(t,c,"/")
	err := local.Undeploy()
	assert.Nil(t, err)
	assert.Nil(t, c.pool)

	_,err = local.ListFiles("/*")
	assert.NotNil(t, err)

	_,err = local.OpenReader("/",0)
	assert.NotNil(t, err)
	_,err = local.OpenWriter("/")
	assert.NotNil(t, err)
	err = local.Delete("/")
	assert.NotNil(t, err)
	_,err = local.Stat("/")
	assert.NotNil(t, err)
}

func TestLocalCache_Deploy(t *testing.T) {
	c := NewLocalInMemoryProvider(100*64)
	local := prepare(t,c,"/")
	err := local.Deploy()
	assert.Nil(t, err)
}
