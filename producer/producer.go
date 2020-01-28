package main

import (
    "encoding/json"
    "github.com/streadway/amqp"
    "log"
    "math/rand"
    "time"
)

type AddTask struct {
    Number1 int
    Number2 int
}

var AMQPConnectionURL = "amqp://guest:guest@192.168.188.25:30672"

func main() {
    conn, err := amqp.Dial(AMQPConnectionURL)
    if err != nil {
        log.Fatalf("%s: %s", "Can't connect to AMQP", err)
    }
    defer conn.Close()

    amqpChannel, err := conn.Channel()
    if err != nil {
        log.Fatalf("%s: %s", "Can't create a amqpChannel", err)
    }
    defer amqpChannel.Close()

    queue, err := amqpChannel.QueueDeclare("add", true, false, false, false, nil)
    if err != nil {
        log.Fatalf("%s: %s", "Could not declare `add` queue", err)
    }

    rand.Seed(time.Now().UnixNano())

    addTask := AddTask{Number1: rand.Intn(999), Number2: rand.Intn(999)}
    body, err := json.Marshal(addTask)
    if err != nil {
        log.Fatalf("%s: %s", "Error encoding JSON", err)
    }

    err = amqpChannel.Publish("", queue.Name, false, false, amqp.Publishing{
        DeliveryMode: amqp.Persistent,
        ContentType:  "text/plain",
        Body:         body,
    })

    if err != nil {
        log.Fatalf("Error publishing message: %s", err)
    }

    log.Printf("AddTask: %d+%d", addTask.Number1, addTask.Number2)

}
