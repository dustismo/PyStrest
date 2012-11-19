#
# Copyright 2009 Facebook
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# originally used in the tornado webserver.  modified for use in STREST 
# Dustin Norlander

import itertools
import re
import urllib
import datetime

"""STREST utility code shared by clients and servers."""



''' Atomic increment http://29a.ch/2009/2/20/atomic-get-and-increment-in-python 
	TODO: I believe itertools is gone in python 3, need a replacement..
'''

_counter = itertools.count()
'''
	atomically generate a new txn-id.  
'''
def generate_txn_id():
	return 'pystrest_' + str(_counter.next())



def _clean_param(value):
	if value == None :
		return ''
	
	if isinstance(value, basestring) : 
		return value.encode("utf-8")        
	if isinstance(value, datetime.date) :
		return value.isoformat()

	return value

'''
	Safely url encodes a param dict
	automatically encodes dates and times as iso format.
'''
def urlencode(params):
	#fix unicode characters for older versions of urllib
	for k in params :
		params[_clean_param(k)] = _clean_param(params[k])

	return urllib.urlencode(params,True)

class HEADERS:
	TXN_ID = "Strest-Txn-Id"
	TXN_ACCEPT = "Strest-Txn-Accept"
	TXN_STATUS = "Strest-Txn-Status"
	
class STRESTHeaders(dict):
	
	"""A dictionary that maintains Http-Header-Case for all keys.

	Supports multiple values per key via a pair of new methods,
	add() and get_list().  The regular dictionary interface returns a single
	value per key, with multiple values joined by a comma.

	>>> h = STRESTHeaders({"content-type": "text/html"})
	>>> h.keys()
	['Content-Type']
	>>> h["Content-Type"]
	'text/html'

	>>> h.add("Set-Cookie", "A=B")
	>>> h.add("Set-Cookie", "C=D")
	>>> h["set-cookie"]
	'A=B,C=D'
	>>> h.get_list("set-cookie")
	['A=B', 'C=D']

	>>> for (k,v) in sorted(h.get_all()):
	...    print '%s: %s' % (k,v)
	...
	Content-Type: text/html
	Set-Cookie: A=B
	Set-Cookie: C=D
	"""
	def __init__(self, *args, **kwargs):
		# Don't pass args or kwargs to dict.__init__, as it will bypass
		# our __setitem__
		dict.__init__(self)
		self._as_list = {}
		self.update(*args, **kwargs)
		
		
	def get_txn_id(self):
		return self[HEADERS.TXN_ID]
	
	def set_txn_id(self, id):
		self[HEADERS.TXN_ID] = id
	
	
	def set_if_absent(self, key, val):
		if not self.get(key, False) :
			self[key] = val
	
	def set(self, key, val):
		self[key] = val
		
	
	# new public methods

	def add(self, name, value):
		"""Adds a new value for the given key."""
		norm_name = STRESTHeaders._normalize_name(name)
		if norm_name in self:
			# bypass our override of __setitem__ since it modifies _as_list
			dict.__setitem__(self, norm_name, self[norm_name] + ',' + value)
			self._as_list[norm_name].append(value)
		else:
			self[norm_name] = value

	def get_list(self, name):
		"""Returns all values for the given header as a list."""
		norm_name = STRESTHeaders._normalize_name(name)
		return self._as_list.get(norm_name, [])

	def get_all(self):
		"""Returns an iterable of all (name, value) pairs.

		If a header has multiple values, multiple pairs will be
		returned with the same name.
		"""
		for name, list in self._as_list.iteritems():
			for value in list:
				yield (name, value)

	def parse_line(self, line):
		"""Updates the dictionary with a single header line.

		>>> h = STRESTHeaders()
		>>> h.parse_line("Content-Type: text/html")
		>>> h.get('content-type')
		'text/html'
		"""
		name, value = line.split(":", 1)
		self.add(name, value.strip())

	@classmethod
	def parse(cls, headers):
		"""Returns a dictionary from HTTP header text.

		>>> h = STRESTHeaders.parse("Content-Type: text/html\\r\\nContent-Length: 42\\r\\n")
		>>> sorted(h.iteritems())
		[('Content-Length', '42'), ('Content-Type', 'text/html')]
		"""
		h = cls()
		for line in headers.splitlines():
			if line:
				h.parse_line(line)
		return h

	# dict implementation overrides

	def __setitem__(self, name, value):
		norm_name = STRESTHeaders._normalize_name(name)
		dict.__setitem__(self, norm_name, value)
		self._as_list[norm_name] = [value]

	def __getitem__(self, name):
		return dict.__getitem__(self, STRESTHeaders._normalize_name(name))

	def __delitem__(self, name):
		norm_name = STRESTHeaders._normalize_name(name)
		dict.__delitem__(self, norm_name)
		del self._as_list[norm_name]

	def get(self, name, default=None):
		return dict.get(self, STRESTHeaders._normalize_name(name), default)

	def update(self, *args, **kwargs):
		# dict.update bypasses our __setitem__
		for k, v in dict(*args, **kwargs).iteritems():
			self[k] = v


	
	_NORMALIZED_HEADER_RE = re.compile(r'^[A-Z0-9][a-z0-9]*(-[A-Z0-9][a-z0-9]*)*$')
	@staticmethod
	def _normalize_name(name):
		"""Converts a name to Http-Header-Case.

		>>> STRESTHeaders._normalize_name("coNtent-TYPE")
		'Content-Type'
		"""
		if STRESTHeaders._NORMALIZED_HEADER_RE.match(name):
			return name
		return "-".join([w.capitalize() for w in name.split("-")])

def _utf8(value):
	if value is None:
		return value
	if isinstance(value, unicode):
		return value.encode("utf-8")
	assert isinstance(value, str)
	return value

class STRESTRequest(object):
	def __init__(self, uri, method="GET", headers=None, content=None):
		if headers is None:
			headers = STRESTHeaders()      
		self.uri = _utf8(uri)
		self.method = method
		self.headers = headers
		self.content = content
	
	'''
		url encodes the dict and sets the content type to 
	'''
	def set_post_params(self, params):
		self.headers['Content-Type'] = 'application/x-www-form-urlencoded'
		self.content = urlencode(params)
		if 'Content-Length' in self.headers:
			del self.headers['Content-Length']
	
	'''
		url encodes the dict and sets it in the URI 
	'''
	def set_get_params(self, params):
		self.uri += '?' + urlencode(params)
	
class STRESTResponse(object):
	def __init__(self):
		self.headers = STRESTHeaders()
		self.content = None
			 
	def parse_headers(self, headers):
		first_line, _, header_data = str(headers).partition("\r\n")
		match = re.match("STREST/[01].[0-9]+ ([0-9]+) (.*)", first_line)
		assert match
		self.code = int(match.group(1))
		self.message = str(match.group(2))
		self.headers = STRESTHeaders.parse(header_data)   
