from tastypie.fields import ApiField

class PrimaryKeyField(ApiField):
	def hydrate(self, bundle):
		if bundle.request.method == 'PUT':
			return None
		
		return super(DynamoKeyField, self).hydrate(bundle)


class HashKeyField(PrimaryKeyField):
	pass

class RangeKeyField(PrimaryKeyField):
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