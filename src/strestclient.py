import socket
from threading import RLock, BoundedSemaphore
import threading
import socket
import time
import json
import sys, traceback



STREST_VERSION = 2.0
USER_AGENT = "PyStrest/0.2"


class STRESTMessage(dict):
    def __init__(self, mapping={}):
        self.update(mapping)
        return
    
    def get_txn_id(self):
        return self.get('strest.txn.id')

    '''
        puts only if the value does not exist in the map
    '''
    def put_default(self, key, value):
        if self.get(key):
            return
        self.put(key,value)


    '''
        updates the map, honors the dot operator
    '''
    def put(self, key, value):
        keys = key.split(".")
        tmp = self
        for k in keys[0:-1]:
            tmp2 = tmp.get(k)
            
            if not tmp2:
                tmp2 = {}
                tmp[k] = tmp2
            tmp = tmp2
        tmp[keys[-1]] = value

    '''
        Getter, works like normal dict but honors the dot operator.
    '''
    def get(self, key, default=None):
        #Surround this with try in case key is None or not a string or somethingtry:
        try:
            keys = key.split(".")
            tmp = super(STRESTMessage, self).get(keys.pop(0), default)
            
            for k in keys :
                try:
                    tmp = tmp[k]
                except:
                    #Exception other than TypeError probably missing key, so default
                    return default
            return tmp
        except:
            pass
        return default        

class STRESTRequest(STRESTMessage):
    def __init__(self, uri, method="GET", params={}):
        super(STRESTMessage, self).__init__()
        self.put('strest.uri', uri)
        self.put('strest.method', method)
        self.put('strest.params', params)

'''
    Implementaion of a streaming json JsonReader
'''
class JsonReader():

    def __init__(self):
        self.parsed = []
        self._reset()
        return

    def _reset(self):
        self.buffer = ''
        self.open_brackets = 0;
        self.is_quote = False;
        self.is_escape = False;
        

    def _object_complete(self):
        #TODO: handle badly formed json
        try :
            parsed = STRESTMessage(json.loads(self.buffer))
        except Exception,e:
            parsed = e
        self._reset()
        self.parsed.append(parsed)

        #reset everythign

    '''
    return the next parsed object.  returns None if no objects are availabel.
    should throw an exception if there was an invalid object
    '''
    def next(self):
        if len(self.parsed) == 0:
            return None
        obj = self.parsed.pop(0)
        if isinstance(obj, Exception):
            raise obj
        return obj


    '''
     some amount of incoming characters
    '''
    def incoming(self, str):

        for c in str:
            self.buffer += c
            if c == '{' :
                self.open_brackets += 1
            elif c == '"' :
                if not self.is_escape:
                    self.is_quote = not self.is_quote
            elif c == '}' :
                self.open_brackets -= 1
                if self.open_brackets == 0 :
                    #object is complete.
                    self._object_complete()
                    continue

            if c == '\\' :
                self.is_escape = not self.is_escape
            else :
                self.is_escape = False


class _ReadThread(threading.Thread):
    
    def __init__(self, strest_client):
        threading.Thread.__init__(self)
        self.daemon = True
        self.strest = strest_client

        
    def run(self):
        jsonReader = JsonReader()
        while 1 :
            try :
                data = self.strest.socket.recv(8192)
                if not data :
                    self.strest._close()
                    return

                data = data.decode('utf-8')
                
                jsonReader.incoming(data)

                msg = jsonReader.next()
                while msg:
                    self.strest._message_received(msg)
                    msg = jsonReader.next()

            except socket.timeout :
#                print "timeout, trying read again"
                pass
            except socket.error :
                print "There was a socket error, closing"
                self.strest._close()
                return
            except Exception, e:
                traceback.print_exc(file=sys.stdout)
                print "some errror happened"
                return
        

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
class StrestClient():

    '''
        Initializes and connects the client to the host.
        
        host -> the address
        port -> the port
        disconnect_callback -> called on disconnect
    '''
    def __init__(self, host, port, disconnect_callback=None):
        
        self.host = host
        self.port = port
        
        self.callbacks = dict()
        self.disconnect_callback = disconnect_callback
        self.lock = RLock()
        

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect( (host, port) )
        self.socket.settimeout(10)
        
        self.read_thread = _ReadThread(self)
        self.read_thread.start()
        self.connected = True
        
        self.txn = 0
        
        
    '''
        Sends a request to the server.
        
        request -> expected to be an instance of STRESTRequest
        response_callback -> function(STRESTMessage)
        txn_complete_callback -> function(STRESTMessage)
        error_callback -> function(exception) - this will only be called in cases of transport problems (ie disconnection).
        
        Note - Callbacks are all executed in the main IO loop, so it is the responsibilty of the caller
        to delegate any heavy processing or blocking functionality elsewhere.
        
    '''
    def send_request(self, request, response_callback=None, txn_complete_callback=None, error_callback=None):        
        with self.lock :
            self.txn += 1
            txn_id = 'txn'+str(self.txn)

            request.put_default('strest.txn.id', txn_id)
            request.put_default('strest.txn.accept', "multi")
            request.put_default('strest.user-agent', USER_AGENT)
            request.put_default('strest.v', STREST_VERSION)
            
            self.callbacks[txn_id] = (response_callback, txn_complete_callback, error_callback)
            
            
            try :
                packet = json.dumps(request)
                self.socket.sendall(packet)
                # print "******"
                # print packet
                # print "*******"
            except Exception as inst:
                # socket error.
#                print 'Socket exception!'
#                print 'Type: ' + str(type(inst))
#                print inst
                self._close()
                
                
        
    
    '''
        Does a blocking request.
        does not block other threads doing concurrent requests
        
        returns the response or raises an exception    
    '''
    def send_blocking_request(self, request):
        cb = BlockingRequest()
        request.put('strest.txn.accept', "single")
        self.send_request(request, cb.response_callback, None, cb.error_callback)
        return cb.await_response()
        
    def _message_received(self, response):
        with self.lock :
            callback = self.callbacks.get(response.get_txn_id())
        try :
            if callback :
                callback[0](response)
        except:
            traceback.print_exc(file=sys.stdout)
            print "Error executing callback, check your callback code"
            pass #do something smarter here.
        
        if response.get('strest.txn.status', "completed").lower() == "completed" :
            if callback and callback[1] :
                callback[1](response)
            try :
                del self.callbacks[response.get_txn_id()]
            except :
                pass
            
    '''
        Closes the connection and releases any resources.
        
        All waiting waiting async calls will be sent a disconnected exception
    ''' 
    def close(self):
        self._close()

    def _close(self):
        with self.lock :
            if self.connected :
                if self.disconnect_callback :
                    self.disconnect_callback(self)
                    
                self.socket.close()
                self.connected = False
            else :
                print "StrestClient already closed, skipping"
            # alert all the callbacks that the client is disconnected.
            for txn in self.callbacks :
                if self.callbacks[txn][2] :
                    self.callbacks[txn][2](Exception("Disconnected!"))
            self.callbacks.clear()
        

    
def example_callback(response):
    print_response(response)
    print "\n"
            
#main app entry point
if __name__ == "__main__":
    msg = STRESTMessage()
    client = StrestClient('localhost', 8081)
    
    # required param example
    # Note the use of the blocking request..
    for c in 'five':
        request = STRESTRequest('/ping', 'GET', {'key':'value'})
        response = client.send_blocking_request(request)
        print str(response)
    
    # print "***********************"
    # request = STRESTRequest('/ping', 'GET', {'key':'value'})
    # response = client.send_blocking_request(request)
    # print response

    sys.exit()

    