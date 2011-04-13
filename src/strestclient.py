import asyncore, socket
from strestutil import STRESTHeaders, STRESTResponse, STRESTRequest
import zlib
from threading import RLock, BoundedSemaphore
import threading
import strestutil
import time


STREST_VERSION = "STREST/0.1"
USER_AGENT = "PyStrest/0.1"

class StrestResponseReader(object):
    
    def __init__(self, asynch_buffer, response_callback):
        self.response = None
        self._decompressor = None
        self.asynch_buffer = asynch_buffer
        self.asynch_buffer.read_until('\r\n\r\n', self._header_callback)
        self.respose_callback = response_callback
        
    
    def _header_callback(self, bytes):
#        print "****** RECEIVED HEADER"
#        print str(bytes)
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
            raise Exception("CHUNKED ENCODING not supported!")
        elif "Content-Length" in self.response.headers:
            num_bytes = int(self.response.headers["Content-Length"])
            self.asynch_buffer.read_bytes(num_bytes, self._content_callback)
        else:
            raise Exception("No Content-length or chunked encoding, "
                            "don't know how to read")
        
        
    def _content_callback(self, bytes):
        if self._decompressor :
            bytes = self._decompressor.decompress(bytes)
        
#        print "******* CONTENT"
#        print str(bytes)
        
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
            if len(self.buf) >= num_bytes :
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
    
''' Global thread for use in the ioloop '''


def _start_io_loop():
    thread = threading.Thread(target=asyncore.loop)
    thread.daemon = True
    thread.start()

'''
    This (internal) class allows us to do blocking requests via the asynch send_request method
'''
class BlockingRequest():
    
    def __init__(self):
        self.semaphore = BoundedSemaphore(1)
        self.exception = None
        self.response = None
        self.semaphore.acquire(True)
    
    def response_callback(self, response):
        self.response = response
        self.semaphore.release()
    
    
    def error_callback(self, exception):
        self.exception = exception
        self.semaphore.release()
    
    ''' returns the response or throws an exception '''
    def await_response(self):
        self.semaphore.acquire(True)        
        if self.exception :
            raise self.exception
        return self.response
        
        
'''
    The Strest Client
    
    
'''
class StrestClient(asyncore.dispatcher):

    '''
        host -> the address
        port -> the port
        disconnect_callback -> called on disconnect
        threaded_io_loop -> if true (default) will start a separate thread that the ioloop runs in.  if False you MUST call asyncore.loop() from elsewhere.
    '''
    def __init__(self, host, port, disconnect_callback=None, threaded_io_loop=True):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect( (host, port) )
        self.buffer = bytearray()
        self.callbacks = dict()
        self.in_buf = AsynchBuffer()
        self.disconnect_callback = disconnect_callback
        self.responses = StrestResponseReader(self.in_buf, self._message_received)
        self.lock = RLock()
        if threaded_io_loop:
            _start_io_loop()

    '''
        Sends a request to the server.
        
        request -> expected to be an instance of STRESTRequest
        response_callback -> function(STRESTResponse)
        txn_complete_callback -> function(STRESTResponse)
        error_callback -> function(exception) - this will only be called in cases of transport problems (ie disconnection).
        
        Note - Callbacks are all executed in the main IO loop, so it is the responsibilty of the caller
        to delegate any heavy processing or blocking functionality elsewhere.
        
    '''
    def send_request(self, request, response_callback=None, txn_complete_callback=None, error_callback=None):        
        with self.lock :
            request.headers.set_if_absent(strestutil.HEADERS.TXN_ID, strestutil.generate_txn_id())
            request.headers.set_if_absent(strestutil.HEADERS.TXN_ACCEPT, "multi")
            request.headers.set_if_absent("User-Agent", USER_AGENT)
            self.callbacks[request.headers.get_txn_id()] = (response_callback, txn_complete_callback, error_callback)
            
            # Set the content length
            if "Content-Length" not in request.headers:
                length = 0
                if request.content :
                    length = len(request.content)
                request.headers['Content-Length'] = length
            
    
            lines = [request.method + " " + request.uri + " " + STREST_VERSION]
            lines.extend(["%s: %s" % (n, v) for n, v in request.headers.iteritems()])
            packet = "\r\n".join(lines) + "\r\n\r\n"
#            print "******"
#            print packet
#            print "*******"
            self.buffer.extend(packet)
    
    '''
        Does a blocking request.
        does not block other threads doing concurrent requests
        
        returns the response or raises an exception    
    '''
    def send_blocking_request(self, request):
        cb = BlockingRequest()
        request.headers.set(strestutil.HEADERS.TXN_ACCEPT, "single")
        self.send_request(request, cb.response_callback, None, cb.error_callback)
        return cb.await_response()
        
    def _message_received(self, response):
        with self.lock :
            callback = self.callbacks.get(response.headers.get_txn_id())
        
        try :
            if callback :
                callback[0](response)
        except :
            pass #do something smarter here.
        
        if response.headers.get(strestutil.HEADERS.TXN_STATUS, "complete").lower() == "complete" :
            if callback and callback[1] :
                callback[1](response)
            del self.callbacks[response.headers.get_txn_id()]
            
    
        
    def handle_connect(self):
        pass

    def handle_close(self):
        with self.lock :
            print "CLOSED!"
            if self.disconnect_callback :
                self.disconnect_callback(self)
            for txn in self.callbacks :
                if self.callbacks[txn][2] :
                    self.callbacks[txn][2](Exception("Disconnected!"))    
            self.close()

    def handle_read(self):
        self.in_buf.add_bytes(self.recv(8192))
        

    def writable(self):
        return (len(self.buffer) > 0)

    def handle_write(self):
        with self.lock:
            sent = self.send(self.buffer)
            self.buffer = self.buffer[sent:]



def print_response(response):
    print "***** RESPONSE CONTENT ******"
    print str(response.content)
    print "***** **************** ******"
    
def example_callback(response):
    print_response(response)
    print "\n"
            
#main app entry point
if __name__ == "__main__":
    client = StrestClient('localhost', 8000)
    
    # required param example
    # Note the use of the blocking request..
    request = STRESTRequest('/require?what=something')
    response = client.send_blocking_request(request)
    print_response(response)
    
    # Firehose example.
    request = STRESTRequest('/firehose')
    client.send_request(request, example_callback)
    
    print "\nFIREHOSE EXAMPLE FOR 30 seconds\n"
    time.sleep(30)
    print "30 seconds is over, goodbye"
    