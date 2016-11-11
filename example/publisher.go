package main

import (
	"context"
	"fmt"
	"github.com/wayt/tq/async"
)

var helloWorld = async.Func("hello-world", func(ctx context.Context, name string) {
	fmt.Println("Hello World!", name)
})

func main() {

	id, err := helloWorld.Call("Albert")
	fmt.Println(id, err)
}
