import SocketServer
import re
import threading

class dummysmtpd(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
	"""Test smtpd class"""
	def __init__(self):
		print "hello"

MAIL_TO_REGEX = re.compile("MAIL\\s+FROM:(.*)", re.I)
RCPT_TO_REGEX = re.compile("RCPT\\s+TO:(.*)", re.I)
HELO_REGEX = re.compile("HELO\\s+(.*)", re.I)

class MessageList:
	def __init__(self):
		self.lock = threading.Lock()
		self.mailboxes = {}

	def AddMessage(self, username, message):
		lock.acquire(1)
		if username in self.mailboxes:	
			self.mailboxes[username].append(message)
		else:
			self.mailboxes[username] = []
			self.mailboxes[username].append(message)

		lock.release()

	def GetMessages(self, username):
		return self.mailboxes[username]


""" For each email address we keep the list of messages """
class SmtpdHandler(SocketServer.StreamRequestHandler):

	def __init__(self, messagelist1, userMap1):
		self.messageList = messagelist1	
		self.userMap = userMap1

	def message(self, code, message):
		self.wfile.write(code + " " + message  + "\r\n")

	def readData(self):
		message = ""
		while True:
			line = self.rfile.readline()
			if line.strip() == ".":
				return message
			if line.startswith("."):
				line = line[1:]
			

	def handle(self):
		connector = ""
		sender = ""
		rcpts = []

		message(220, "Test server welcome")

		while True:
			line = self.rfile.readline().strip()
			isMailTo = MAIL_TO_REGEX.match(line)
			isRcptTo = RCPT_TO_REGEX.match(line)
			isHelo = HELO_REGEX.match(line)
			if isHelo != None:
				message(250, "Hello " + isHelo.group(1))
			elif isMailTo != None:
				sender = isMailTo.group(1).strip()
				message(250, "OK Mail from: " + sender)
			elif isRcptTo != None:
				rcpt = isRcptTo.group(1).strip()
				if rcpt in self.userMap:
					rcpt = self.userMap[rcpt]
					rcpts.append(rcpt)
					message(250, "OK Rcpt to: " + rcpt)
				else:
					message(550, "User does not exist")
				pass
			elif line == "DATA":
				msg = readData();
				message(250, "Message received: " + msg)
			else:
				message(550, "ERROR command was expected")
		

HOST, PORT = "localhost", 9999

messageList = MessageList()
userMap = {"me@genady.org": "genady"}
handler = SmtpdHandler(messageList, userMap)

server = SocketServer.TCPServer((HOST, PORT), handler)
server.serve_forever()

print "Server started"

