#!/bin/env python

import sys
import time
import zmq

CONTROLLER_PUB_PORT = 5559
MESSAGE_RECIVER_PORT = 5558
MESSAGE_PUBLISHER_PORT = 5557

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

    while True:
	received_msg = receiver.recv()
	print "RECV: " + received_msg
	if received_msg == 'STEP 9 DONE':
	    controller.send("CMD_KILL")
	elif received_msg.startswith('REGISTOR'):
	    param = received_msg[8:]
	    print 'REGISTOR recived:', param
	    controller.send("CMD_START_TASK")

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
