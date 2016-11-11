package main

import (
	"context"
	"fmt"
	"github.com/wayt/tq"
	"github.com/wayt/tq/async"
	// "os"
	"time"
)

var helloWorld = async.Func("hello-world", func(ctx context.Context, name string) error {
	fmt.Println("Hello World!", name)
	return nil
})

func main() {

	for {
		// ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
		tasks, err := tq.Get(context.Background(), 10)
		if err != nil {
			fmt.Println("err:", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if tasks == nil {
			// fmt.Println("no task")
			time.Sleep(1 * time.Second)
			continue
		}

		fmt.Printf("fetched %d task(s)\n", len(tasks))

		for _, task := range tasks {
			fmt.Printf("running %d (%s)\n", task.ID, task.Name)

			if err := async.RunTask(context.Background(), task); err != nil {
				task.Nack(err)
				fmt.Println(task.ID, err)
			} else {
				task.Ack()
			}
		}

	}

}
