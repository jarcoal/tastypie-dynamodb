from tastypie.fields import ApiField, DateTimeField
import datetime

class DynamoKeyField(ApiField):
	"""
	Root Dynamo Key Field
	"""
	def __init__(self, value=None, **k):
		super(DynamoKeyField, self).__init__(**k)
		self.value = value

	def hydrate(self, bundle):
		#if this is a create, and there is a specified value for this key, use it
		if bundle.request.method == 'POST' and self.value:
			return self.value(bundle) if hasattr(self.value, '__call__') else self.value
		
		#dynamo keys can't be updated
		elif bundle.request.method == 'PUT':
			return None
		
		#fall into line
		return super(DynamoKeyField, self).hydrate(bundle)


class HashKeyField(DynamoKeyField):
	pass

class RangeKeyField(DynamoKeyField):
	pass

class DynamoNumberMixin(object):
	convert = lambda self, value: None if value is None else int(value)

class DynamoStringMixin(object):
	convert = lambda self, value: None if value is None else str(value)

class NumericHashKeyField(DynamoNumberMixin, HashKeyField):
	pass

class StringHashKeyField(DynamoStringMixin, HashKeyField):
	pass

class NumericRangeKeyField(DynamoNumberMixin, RangeKeyField):
	pass

class StringRangeKeyField(DynamoStringMixin, RangeKeyField):
	pass