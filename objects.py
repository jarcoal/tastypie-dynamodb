class DynamoObject(object):
	"""
	Thin wrapper for Dynamo Items
	"""

	def __init__(self, initial=None):
		self.__dict__['_data'] = initial or {}
	
	def __getattr__(self, name):
		return self._data.get(name, None)
	
	def __setattr__(self, name, value):
		self.__dict__['_data'][name] = value
	
	def to_dict(self):
		return self._data