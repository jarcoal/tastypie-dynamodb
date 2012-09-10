from django.conf.urls import url
from django.http import Http404

from boto.dynamodb.condition import EQ, ConditionTwoArgs
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError

from tastypie.resources import DeclarativeMetaclass, Resource
from tastypie_dynamodb.objects import DynamoObject

from tastypie_dynamodb.fields import NumericHashKeyField, StringHashKeyField, NumericRangeKeyField, StringRangeKeyField


class DynamoDeclarativeMetaclass(DeclarativeMetaclass):
	def __new__(cls, name, bases, attrs):
		meta = attrs.get('Meta')

		#ensure that consistent_read has a value
		if not hasattr(meta, 'consistent_read'):
			setattr(meta, 'consistent_read', False)

		#ensure that object_class has a value
		if not hasattr(meta, 'object_class'):
			setattr(meta, 'object_class', DynamoObject)

		#if the user is asking us to auto-build their primary keys
		if getattr(meta, 'build_primary_keys', False) == True:
			schema = meta.table.schema
			attrs[schema.hash_key_name] = NumericHashKeyField(attribute=schema.hash_key_name) if schema.hash_key_type == 'N' else StringHashKeyField(attribute=schema.hash_key_name)

		return super(DynamoDeclarativeMetaclass, self).__new__(cls, name, bases, attrs)


class DynamoHashResource(Resource):
	"""Resource to use for Dynamo tables that only have a hash primary key."""

	__metaclass__ = DynamoDeclarativeMetaclass

	def __init__(self, *a, **k):
		super(DynamoHashResource, self).__init__(*a, **k)
		self._hash_key_type = int if self._meta.table.schema.hash_key_type == 'N' else str


	def dispatch_detail(self, request, **k):
		"""Ensure that the hash_key is received in the correct type"""
		k['hash_key'] = self._hash_key_type(k['hash_key'])
		return super(DynamoHashResource, self).dispatch_detail(request, **k)


	#
	prepend_urls = lambda self: [url(r'^(?P<resource_name>%s)/(?P<hash_key>.+)/$' % self._meta.resource_name, self.wrap_view('dispatch_detail'), name='api_dispatch_detail'),]
	get_resource_uri = lambda self, bundle: self._build_reverse_url('api_dispatch_detail', kwargs=self.get_resource_uri_kwargs(bundle))
	get_resource_uri_kwargs = lambda self, bundle: { 'api_name': self._meta.api_name, 'resource_name': self._meta.resource_name, 'hash_key': str(getattr(bundle.obj, self._meta.table.schema.hash_key_name)), }

	def _dynamo_update_or_insert(self, bundle, primary_keys=None):
		primary_keys = primary_keys or {}

		bundle = self.full_hydrate(bundle)
		item = self._meta.table.new_item(**primary_keys)
		
		#extract our attributes from the bundle
		attrs = bundle.obj.to_dict()
		
		#loop and add the valid values
		for key, val in attrs.items():
			if val is None:
				continue
			
			item[key] = val
		

		#if there are pks, this is an update, else it's new
		item.put() if primary_keys else item.save()


		#wrap the item and store it for return
		bundle.obj = DynamoObject(item)
		
		return bundle


	def obj_update(self, bundle, request=None, **k):
		"""Issues update command to dynamo, which will create if doesn't exist."""
		return self._dynamo_update_or_insert(bundle, primary_keys=k)


	def obj_create(self, bundle, request=None, **k):
		"""Creates an object in Dynamo"""
		return self._dynamo_update_or_insert(bundle)


	def obj_get(self, request=None, **k):
		"""Gets an object in Dynamo"""
		try:
			item = self._meta.table.get_item(consistent_read=self._meta.consistent_read, **k)
		except DynamoDBKeyNotFoundError:
			raise Http404
			
		return DynamoObject(item)


	def obj_delete(self, request=None, **k):
		"""Deletes an object in Dynamo"""
	
		item = self._meta.table.new_item(**k)
		item.delete()


	def rollback(self):
		pass

	def get_object_list(self, request=None):
		pass
	
	def obj_delete_list(self, request=None, **k):
		pass



class DynamoRangeDeclarativeMetaclass(DynamoDeclarativeMetaclass):
	def __new__(cls, name, bases, attrs):
		meta = attrs.get('Meta')

		#ensure scan index forward
		if not hasattr(meta, 'scan_index_forward'):
			setattr(meta, 'scan_index_forward', True)

		#ensure range key condition
		if not hasattr(meta, 'range_key_condition'):
			setattr(meta, 'range_key_condition', EQ)

		#ensure a proper delimeter
		if not hasattr(meta, 'primary_key_delimeter'):
			setattr(meta, 'primary_key_delimeter', ':')

		#invalid delimeter
		elif getattr(meta, 'primary_key_delimeter') in (';', '&', '?'):
			raise Exception('"%" is not a valid delimeter.' % getattr(meta, 'primary_key_delimeter'))

		#if the user is asking us to auto-build their primary keys
		if getattr(meta, 'build_primary_keys', False) == True:
			schema = meta.table.schema
			attrs[schema.range_key_name] = NumericRangeKeyField(attribute=schema.range_key_name) if schema.range_key_type == 'N' else StringRangeKeyField(attribute=schema.range_key_name)

		return super(DynamoRangeDeclarativeMetaclass, self).__new__(cls, name, bases, attrs)



class DynamoHashRangeResource(DynamoHashResource):
	"""Resource to use for Dynamo tables that have hash and range keys."""

	__metaclass__ = DynamoRangeDeclarativeMetaclass

	def __init__(self, *a, **k):
		super(DynamoHashRangeResource, self).__init__(*a, **k)
		self._range_key_type = int if self._meta.table.schema.range_key_type == 'N' else str


	def dispatch_detail(self, request, **k):
		"""Ensure that the range_key is received in the correct type"""

		k['range_key'] = self._range_key_type(k['range_key'])
		return super(DynamoHashRangeResource, self).dispatch_detail(request, **k)


	prepend_urls = lambda self: (url(r'^(?P<resource_name>%s)/(?P<hash_key>.+)%s(?P<range_key>.+)/$' % (self._meta.resource_name, self._meta.primary_key_delimeter), self.wrap_view('dispatch_detail'), name='api_dispatch_detail'),)

	def get_resource_uri_kwargs(self, bundle):
		resource_kwargs = super(DynamoHashRangeResource, self).get_resource_uri_kwargs(bundle)
		resource_kwargs['range_key'] = str(getattr(bundle.obj, self._meta.table.schema.range_key_name))
		return resource_kwargs


	def obj_get_list(self, request=None, **k):
		schema = self._meta.table.schema
	
		#work out the hash key
		hash_key = request.GET.get(schema.hash_key_name, None)
		
		if not hash_key:
			raise Http404
	
		#get initial params
		params = {
			'hash_key': self._hash_key_type(hash_key),
			'request_limit': self._meta.limit,
			'consistent_read': self._meta.consistent_read,
			'scan_index_forward': self._meta.scan_index_forward,
		}
		
		
		#see if there is a range key in the get request (which will override the default, if there was any)
		range_key = request.GET.get(schema.range_key_name, None)
		
		#if a range key value was specified, prepare
		if range_key:
			#get the range key condition
			range_key_condition = self._meta.range_key_condition

			#this is an instance, with default values we need to override.  convert back to class for re-instantiation.
			if not inspect.isclass(range_key_condition):
				range_key_condition = range_key_condition.__class__
		
			
			range_values = {}
			
			#this class should be instantiated with two values..
			if issubclass(range_key_condition, ConditionTwoArgs):
				range_values['v1'], range_values['v2'] = [self._range_key_type(i) for i in range_key.split(self._meta.primary_key_delimeter)]
			else:
				#setup the value that the class will be instantiated with
				range_values['v1'] = self._range_key_type(range_key)
			
			#instantiate the range condition class
			range_key_condition = range_key_condition(**range_values)
			
			#drop in the condition
			params['range_key_condition'] = range_key_condition


		#perform the query
		results = self._meta.table.query(**params)

		#return the results
		return [DynamoObject(obj) for obj in results]