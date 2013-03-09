import socket
import gearman

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('localhost', 8000))

buf = gearman.protocol.pack_binary_command(1, {'task':'reverse'}, False)

sock.send(buf)

print "end."