#!/bin/env python

import sys
import time
import zmq
import os

CONTROLLER_PUB_PORT = 5559
MESSAGE_RECIVER_PORT = 5558
MESSAGE_PUBLISHER_PORT = 5557
FILE_SERVER_PORT = 5556

def usage():
    print "usage: %s DOWNSTREAM|UPSTREAM" % sys.argv[0]

def asUpStream():
    context = zmq.Context()
    # Socket to receive messages on
    receiver = context.socket(zmq.PULL)
    receiver.bind("tcp://*:%s" % MESSAGE_RECIVER_PORT)

    # Socket for worker control
    controller = context.socket(zmq.PUB)
    controller.bind("tcp://*:%s" % CONTROLLER_PUB_PORT)

    file_server = context.socket(zmq.REP)
    file_server.bind('tcp://*:%s' % FILE_SERVER_PORT)

    BUFF = 64

    poller = zmq.Poller()
    poller.register(receiver, zmq.POLLIN)
    poller.register(file_server, zmq.POLLIN)

    while True:
	socks = dict(poller.poll())
	if socks.get(receiver) == zmq.POLLIN:
	    received_msg = receiver.recv()
	    print "RECV: " + received_msg
	    if received_msg == 'STEP 9 DONE':
		controller.send("CMD_KILL")
	    elif received_msg == 'STEP 7 DONE':
		controller.send("PULL_FILE %s" % '/home/guoshiwei/start-proxy-gfw.sh')
	    elif received_msg.startswith('REGISTOR'):
		param = received_msg[8:]
		print 'REGISTOR recived:', param
		controller.send("CMD_START_TASK")
	elif socks.get(file_server) == zmq.POLLIN:
	    # Set up a return container
	    ret = {}
	    # Recieve the location of the file to serve
	    msg = file_server.recv_pyobj()
	    # Verify that the file is available
	    if not os.path.isfile(msg['path']):
		file_server.send('')
		continue
	    # Open the file for reading
	    fn = open(msg['path'], 'rb')
	    fn.seek(msg['loc'])
	    ret['body'] = fn.read(BUFF)
	    ret['loc'] = fn.tell()
	    file_server.send_pyobj(ret)

    time.sleep(1)

def asDownStream():
    context = zmq.Context()
    # Socket to send messages to
    sender = context.socket(zmq.PUSH)
    sender.connect("tcp://localhost:%s" % MESSAGE_RECIVER_PORT)

    # Socket for control input
    controller = context.socket(zmq.SUB)
    controller.connect("tcp://localhost:%s" % CONTROLLER_PUB_PORT)
    controller.setsockopt(zmq.SUBSCRIBE, "")

    sender.send("REGISTOR Node_1")
    # Process messages from receiver and controller
    poller = zmq.Poller()
    poller.register(controller, zmq.POLLIN)

    file_client = context.socket(zmq.REQ)
    file_client.connect('tcp://localhost:%s' % FILE_SERVER_PORT)
    # Process messages from both sockets
    while True:
	socks = dict(poller.poll())

	# Any waiting controller command acts as 'KILL'
	if socks.get(controller) == zmq.POLLIN:
	    recived_controll_cmd = controller.recv()
	    print "RECV_CONTROL_CMD: " + recived_controll_cmd
	    if recived_controll_cmd == 'CMD_KILL':
		break
	    elif recived_controll_cmd == "CMD_START_TASK":
		sender.send("STEP 1 DONE")
		sender.send("STEP 5 DONE")
		sender.send("STEP 6 DONE")
		sender.send("STEP 7 DONE")
	    elif recived_controll_cmd.startswith('PULL_FILE'):
		filename = recived_controll_cmd.split(None, 1)[1]

		# Open up the file we are going to write to
		dest = open(os.path.basename(filename), 'w+')
		msg = {'loc': 0,
		       'path': filename}

		while True:
		    # send the desired file and the location to the server
		    file_client.send_pyobj(msg)
		    # Start grabing data
		    data = file_client.recv_pyobj()
		    # Write the chunk to the file
		    if data['body']:
			dest.write(data['body'])
			msg['loc'] = dest.tell()
		    else:
			break 

		sender.send("STEP 8 DONE")
		sender.send("STEP 9 DONE")
	    else:
		print "Uknow CMD: %s"

def main():
    if len(sys.argv) != 2:
	usage()
	return 1

    role = sys.argv[1]
    if role == 'UPSTREAM':
	return asUpStream()
    elif role == 'DOWNSTREAM':
	return asDownStream()
    else:
	print >> sys.stderr, "Unknow role: " + role
	usage()
	return 1

if __name__ == '__main__':
    sys.exit(main())
