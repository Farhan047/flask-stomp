import stomp




def send_message(message):



def main():
    message = "Hello"
    connection = stomp.Connection([('127.0.0.1', 62613)])
    connection.subscribe("/queue/{}", 123)
    connection.send('/queue/test', message)





if __name__ == '__main__':
    main()