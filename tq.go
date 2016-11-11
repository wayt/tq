package tq

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"time"
)

var db *sql.DB

func init() {
	var err error

	if db, err = sql.Open("mysql", "root:toto42@tcp(192.168.99.100:3306)/schedule?parseTime=true"); err != nil {
		panic(err)
	}
}

const (
	StatusTODO   = "todo"
	StatusDoing  = "doing"
	StatusError  = "error"
	StatusDone   = "done"
	StatusCancel = "cancel"

	DefaultRetryCount = 5
	LockName          = "tasks"
)

type Task struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Args      string    `json:"args"`
	Status    string    `json:"status"`
	LastError string    `json:"last_error"`
	Retry     int       `json:"retry"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func NewTask(name, args string) *Task {

	return &Task{
		ID:        0,
		Name:      name,
		Args:      args,
		Status:    StatusTODO,
		LastError: "",
		Retry:     DefaultRetryCount,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

}

func Add(t *Task) error {

	result, err := db.Exec("INSERT INTO `tasks` (`name`, `args`, `status`, `last_error`, `retry`, `created_at`, `updated_at`) VALUES (?, ?, ?, ?, ?, ?, ?)", t.Name, t.Args, t.Status, t.LastError, t.Retry, t.CreatedAt, t.UpdatedAt)
	if err != nil {
		return err
	}

	lastId, err := result.LastInsertId()
	if err != nil {
		return err
	}

	t.ID = lastId

	return nil
}

func getLock(ctx context.Context) (bool, error) {
	var lockWait int = -1

	at, ok := ctx.Deadline()
	if ok {
		lockWait = int(at.Sub(time.Now()).Seconds())
		// log.Printf("lockWait: %d\n", lockWait)
	}

	// Get lock
	// log.Printf("get lock (%d timeout)\n", lockWait)
	result := db.QueryRow("SELECT GET_LOCK(?, ?)", LockName, lockWait)
	var hasLock int = 0
	if err := result.Scan(&hasLock); err != nil {
		log.Printf("result.Scan(&hasLock); FAIL: %v\n", err)
		return false, err
	}

	if hasLock == 0 {
		log.Printf("dont have lock, abort\n")
		return false, nil
	}

	return true, nil
}

func releaseLock() error {
	// log.Printf("releasing lock...")
	_, err := db.Exec("SELECT RELEASE_LOCK(?)", LockName)
	return err
}

func Get(ctx context.Context, count int) ([]*Task, error) {

	ok, err := getLock(ctx)
	if !ok || err != nil {
		return nil, err
	}
	// defer lock release request
	defer releaseLock()

	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	result, err := tx.Query("SELECT `id`, `name`, `args`, `status`, `retry`, `created_at`, `updated_at` FROM `tasks` WHERE `status` = ? ORDER BY `created_at` ASC LIMIT ?", StatusTODO, count)
	if err != nil {
		log.Printf("SELECT FROM `tasks`; FAIL: %v\n", err)
		tx.Rollback()
		return nil, err
	}

	var tasks []*Task
	var ids []int64
	for result.Next() {

		t := new(Task)
		if err := result.Scan(&t.ID, &t.Name, &t.Args, &t.Status, &t.Retry, &t.CreatedAt, &t.UpdatedAt); err != nil {
			tx.Rollback()
			return nil, err
		}

		tasks = append(tasks, t)
		ids = append(ids, t.ID)
	}

	for _, task := range tasks {

		if _, err := tx.Exec("UPDATE `tasks` SET `status` = ?, `updated_at` = ? WHERE `id` = ?", StatusDoing, time.Now().UTC(), task.ID); err != nil {
			tx.Rollback()
			return nil, err
		}
	}

	if tx.Commit(); err != nil {
		return nil, err
	}

	return tasks, nil
}

func (t *Task) Ack() error {

	_, err := db.Exec("UPDATE `tasks` SET `status` = ?, `updated_at` = ? WHERE `id` = ?", StatusDone, time.Now().UTC(), t.ID)
	return err
}
func (t *Task) Nack(lastError error) error {

	status := StatusTODO

	t.Retry -= 1
	if t.Retry <= 0 {
		status = StatusError
	}

	_, err := db.Exec("UPDATE `tasks` SET `status` = ?, `retry` = ?, `updated_at` = ?, `last_error` = ? WHERE `id` = ?", status, t.Retry, time.Now().UTC(), lastError.Error(), t.ID)
	return err
}
