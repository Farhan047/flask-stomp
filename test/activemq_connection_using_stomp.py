import stomp
import time
import json


class MyListener(stomp.ConnectionListener):
    def on_message(self, headers, message):
        print('MyListener:\nreceived a message "{}"\n'.format(message))
        # global read_messages
        # read_messages.append({'id': headers['message-id'], 'subscription': headers['subscription']})


read_messages = []

activemq_queue = "/queue/some_queue"


def main():
    connection = stomp.Connection([('localhost', 61613)])
    # connection.subscribe("/queue/some_queue", id=123)
    connection.set_listener('my_listener', MyListener())
    connection.connect(wait=True)

    # connection.subscribe(destination=activemq_queue, id=1, ack='client-individual')

    for each in range(100):
        time.sleep(0.2)

        message = {
            "msg_id": each
        }

        connection.send(body=json.dumps(message), destination=activemq_queue)
        print(f"Sent {message}")

    print("Message sent")
    # connection.send(body="A Test message From Farhan", destination=activemq_queue)
    # while True:
    #     time.sleep(1)
    #     for msg in read_messages:
    #         connection.ack(msg['id'], msg['subscription'])


if __name__ == '__main__':
    main()
