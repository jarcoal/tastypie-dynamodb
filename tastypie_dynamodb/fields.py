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

class NumberMixin(object):
	convert = lambda self, value: None if value is None else int(value)

class StringMixin(object):
	convert = lambda self, value: None if value is None else str(value)

class NumericHashKeyField(NumberMixin, HashKeyField):
	pass

class StringHashKeyField(StringMixin, HashKeyField):
	pass

class NumericRangeKeyField(NumberMixin, RangeKeyField):
	pass

class StringRangeKeyField(StringMixin, RangeKeyField):
	pass