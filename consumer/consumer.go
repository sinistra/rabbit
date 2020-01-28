package main

import (
    "encoding/json"
    "log"
    "os"

    "github.com/streadway/amqp"
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

    err = amqpChannel.Qos(1, 0, false)
    if err != nil {
        log.Fatalf("%s: %s", "Could not configure QoS", err)
    }

    messageChannel, err := amqpChannel.Consume(
        queue.Name,
        "",
        false,
        false,
        false,
        false,
        nil,
    )
    if err != nil {
        log.Fatalf("%s: %s", "Could not register consumer", err)
    }

    stopChan := make(chan bool)

    go func() {
        log.Printf("Consumer ready, PID: %d", os.Getpid())
        for d := range messageChannel {
            log.Printf("Received a message: %s", d.Body)

            addTask := &AddTask{}

            err := json.Unmarshal(d.Body, addTask)
            if err != nil {
                log.Printf("Error decoding JSON: %s", err)
            }

            log.Printf("Result of %d + %d is : %d", addTask.Number1, addTask.Number2, addTask.Number1+addTask.Number2)

            if err := d.Ack(false); err != nil {
                log.Printf("Error acknowledging message : %s", err)
            } else {
                log.Printf("Acknowledged message")
            }

        }
    }()

    // Stop for program termination
    <-stopChan
}
