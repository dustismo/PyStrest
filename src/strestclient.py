import re
import asyncore, socket
from Queue import Queue
from strestutil import STRESTHeaders, STRESTResponse, STRESTRequest
import zlib
from threading import RLock
import strestutil



class StrestResponseReader(object):
    
    def __init__(self, asynch_buffer, response_callback):
        self.response = None
        self._decompressor = None
        self.asynch_buffer = asynch_buffer
        self.asynch_buffer.read_until('\r\n\r\n', self._header_callback)
        self.respose_callback = response_callback
        
    
    def _header_callback(self, bytes):
        print "****** HEADER"
        print str(bytes)
        self.response = STRESTResponse()
        self.response.parse_headers(bytes)
        self.read_content()
        
    def read_content(self):
        self._decompressor = None
        if (self.response.headers.get("Content-Encoding") == "gzip"):
            # Magic parameter makes zlib module understand gzip header
            # http://stackoverflow.com/questions/1838699/how-can-i-decompress-a-gzip-stream-with-zlib
            self._decompressor = zlib.decompressobj(16+zlib.MAX_WBITS)
        if self.response.headers.get("Transfer-Encoding") == "chunked":
#            self.chunks = []
#            self.stream.read_until("\r\n", self._on_chunk_length)
#            TODO: Handle chunked encoding
            pass
        elif "Content-Length" in self.response.headers:
            num_bytes = int(self.response.headers["Content-Length"])
            self.asynch_buffer.read_bytes(num_bytes, self._content_callback)
        else:
            raise Exception("No Content-length or chunked encoding, "
                            "don't know how to read")
        
        
    def _content_callback(self, bytes):
        if self._decompressor :
            bytes = self._decompressor.decompress(bytes)
        
        print "******* CONTENT"
        print str(bytes)
        
        self.response.content = bytes
        self.respose_callback(self.response)
        
        self.asynch_buffer.read_until('\r\n\r\n', self._header_callback)
            
        
'''
    An asynchronous buffer dealing with byte streams
    currently only one read request is allowed at a time.  
    
    We could queue requests, but that seems fairly dangerous.  Will consider 
    if there is a need.
    
    Should be pretty fast, though some optimizations wouldn't hurt.
'''
class AsynchBuffer(object):
    
    def __init__(self):
        self.buf = bytearray()
        self.pointer = 0
        self.reader = None
        self.lock = RLock()
        
    
    def _set_reader(self, method, val, callback):
        with self.lock :
            if not method :
                self.reader = None
                return
            
            if self.reader :
                print str(self.reader)
                print method, val, callback
                raise Exception("There is already a reader waiting")
            else :
                self.reader = (method, val, callback)
        
        # now try to execute it
        self._attempt_read()
    
    def _get_reader(self):
        with self.lock :
            return self.reader
        
    
    '''
        Reads the specified number of bytes from the buffer
        calls the callback once the bytes are available.
    '''
    def read_bytes(self, num_bytes, callback):
        self._set_reader("read_bytes", num_bytes, callback)

    '''
        reads until the specified string is encountered. 
    '''
    def read_until(self, string, callback):
        self._set_reader("read_until", string, callback)
        
    '''
        attempts to read the requested data from the buffer.
        if read_until or read_bytes has not been called, then
        this does nothing.
    '''
    def _attempt_read(self):
        reader = self._get_reader()
        if not reader :
            return    
        method = reader[0]
        if method == 'read_until' :
            self._read_until(reader[1], reader[2])
        elif method == 'read_bytes' :
            self._read_bytes(reader[1], reader[2])
    '''
        Reads the specified number of bytes
        executes the callback once that number of bytes is read
    '''
    def _read_bytes(self, num_bytes, callback):    
        if num_bytes < 1 :
            self._do_callback('', callback)
            return
        
        with self.lock :
            if len(self.buf) > num_bytes :
                bytes = self.buf[0:num_bytes]
                self.buf = self.buf[num_bytes:]
                self._do_callback(bytes, callback)
        
        
    def _read_until(self, val, callback):
        with self.lock :
            index = self.buf.find(val, self.pointer)
            if index == -1 :
                # we set the new pointer to be 4 less then the length.
                # this is in case the last bytes ends with \r\n\r
                self.pointer = max(len(self.buf)-len(val), 0) 
            else :
                bytes = self.buf[0:index]
                self.buf = self.buf[index+len(val):]
                self.pointer = 0
                self._do_callback(bytes, callback)
        
    def _do_callback(self, bytes, callback):
        self._set_reader(None, None, None)
        #clear any 
        callback(bytes)
        
    
        
    def add_bytes(self, bytes):
        with self.lock :
            # This might be an atomic op, check on that..
            self.buf.extend(bytes)
        
        self._attempt_read()
    
'''
    Example asynch httpclient
    http://docs.python.org/library/asyncore.html
'''
class StrestClient(asyncore.dispatcher):

    def __init__(self, host, port, disconnect_callback=None):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect( (host, port) )
        self.buffer = bytearray()
        self.callbacks = dict()
        self.in_buf = AsynchBuffer()
        self.disconnect_callback = disconnect_callback
        self.responses = StrestResponseReader(self.in_buf, self.message_received)
    
        
    def message_received(self, response):
        print "recieved"
        callback = self.callbacks.get(response.headers.get_txn_id())
        if response.headers.get(strestutil.HEADERS.TXN_STATUS, "complete").lower() == "complete" :
            del self.callbacks[response.headers.get_txn_id()]
        if callback :
            callback[0](response)

    def send_request(self, request, response_callback, error_callback=None):
        request.headers.set_if_absent(strestutil.HEADERS.TXN_ID, strestutil.generate_txn_id())
        request.headers.set_if_absent(strestutil.HEADERS.TXN_ACCEPT, "multi")
        self.callbacks[request.headers.get_txn_id()] = (response_callback, error_callback)
        
        # Set the content length
        if "Content-Length" not in request.headers:
            length = 0
            if request.content :
                length = len(request.content)
            request.headers['Content-Length'] = length
        

        lines = [request.method + " " + request.uri + " " + strestutil.STREST_VERSION]
        lines.extend(["%s: %s" % (n, v) for n, v in request.headers.iteritems()])
        packet = "\r\n".join(lines) + "\r\n\r\n"
        print "******"
        print packet
        print "*******"
        self.buffer.extend(packet)
        
    
        
    def handle_connect(self):
        pass

    def handle_close(self):
        print "CLOSED!"
        if self.disconnect_callback :
            self.disconnect_callback(self)
        for txn in self.callbacks :
            if self.callbacks[txn][1] :
                self.callbacks[txn][1](self, Exception("Disconnected!"))    
        self.close()

    def handle_read(self):
        self.in_buf.add_bytes(self.recv(8192))
        

    def writable(self):
        return (len(self.buffer) > 0)

    def handle_write(self):
        sent = self.send(self.buffer)
        self.buffer = self.buffer[sent:]
        
        
def example_callback(response):
    print str(response)
            
#main app entry point
if __name__ == "__main__":
    client = StrestClient('localhost', 8000)
    request = STRESTRequest('/firehose')
    
    client.send_request(request, example_callback)
    print "entering loop"
    asyncore.loop()
    print "loop done"
    
    

    