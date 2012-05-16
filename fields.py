from tastypie.fields import ApiField, DateTimeField
import datetime

class DynamoKeyField(ApiField):
	"""
	Root Dynamo Key Field
	"""
	def __init__(self, updateable=False, value=None, **k):
		super(DynamoKeyField, self).__init__(**k)
		
		self.updateable = updateable
		self.value = value
	

	def hydrate(self, bundle):
		if bundle.request.method == 'POST' and self.value:
			return self.value(bundle) if hasattr(self.value, '__call__') else self.value
		elif bundle.request.method == 'PUT' and not self.updateable:
			return None
		
		return super(DynamoKeyField, self).hydrate(bundle)


class HashKeyField(DynamoKeyField):
	"""
	Hash Primary Key Field
	"""


class RangeKeyField(DynamoKeyField):
	"""
	Range Primary Key Field
	"""


class DynamoNumberMixin(object):
	"""
	Converts values to numbers
	"""

	def convert(self, value):
		return None if value is None else int(value)


class DynamoStringMixin(object):
	"""
	Converts values to strings
	"""

	def convert(self, value):
		return None if value is None else str(value)



class NumericHashKeyField(DynamoNumberMixin, HashKeyField):
	pass

class StringHashKeyField(DynamoStringMixin, HashKeyField):
	pass

class NumericRangeKeyField(DynamoNumberMixin, RangeKeyField):
	pass

class StringRangeKeyField(DynamoStringMixin, RangeKeyField):
	pass
	

class TimestampField(DateTimeField):
	help_text = 'A date & time as a timestamp.'
	
	def convert(self, value):
		if value is None:
			return None
		
		return datetime.datetime.utcfromtimestamp(value)
