from django.conf.urls import url
from django.http import Http404

from boto.dynamodb.condition import EQ, ConditionTwoArgs
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError

from tastypie.resources import Resource
from tastypie_dynamodb.objects import DynamoObject


class DynamoResource(Resource):
	"""
	Root Dynamo Resource.  This should be treated as abstract.
	Inherit from DynamoHashResource or DynamoHashRangeResource.
	"""

	def __init__(self, *a, **k):
		super(DynamoResource, self).__init__(*a, **k)
		self._meta.consistent_read = getattr(self._meta, 'consistent_read', False)
		self._meta.object_class = DynamoObject if self._meta.object_class is None else self._meta.object_class

		self._hash_key_type = int if self._meta.table.schema.hash_key_type == 'N' else str


	def full_hydrate(self, *a, **k):
		bundle = super(DynamoResource, self).full_hydrate(*a, **k)

		#make sure we get the hash key from the request
		hash_key_name = self._meta.table.schema.hash_key_name
		setattr(bundle.obj, hash_key_name, bundle.data.get(hash_key_name, None))

		return bundle

	#
	prepend_urls = lambda self: (url(r'^(?P<resource_name>%s)/(?P<hash_key>.+)/$' % self._meta.resource_name, self.wrap_view('dispatch_detail'), name='api_dispatch_detail'),)

	def get_resource_uri(self, bundle):
		return self._build_reverse_url('api_dispatch_detail', kwargs={
			'resource_name': self._meta.resource_name,
			'pk': self._dehydrate_pk_slug(bundle.obj),
			'api_name': self._meta.api_name,
		})


	def _dynamo_update_or_insert(self, bundle, params=None, update=False):
		params = params or {}

		bundle = self.full_hydrate(bundle)
		item = self._meta.table.new_item(**params)
		
		#extract our attributes from the bundle
		attrs = bundle.obj.to_dict()
		
		#loop and add the valid values
		for key, val in attrs.items():
			if val is None:
				continue
			
			item[key] = val
		
		#commit to db
		if update:
			item.save()
		else:
			item.put()

		#wrap the item and store it for return
		bundle.obj = DynamoObject(item)
		
		return bundle


	def obj_update(self, bundle, request=None, **k):
		"""
		Issues update command to dynamo, which will create if doesn't exist.
		"""
		return self._dynamo_update_or_insert(bundle, params=self._hydrate_pk_slug(k['pk']), update=True)


	def obj_create(self, bundle, request=None, **k):
		"""
		Creates an object in Dynamo
		"""
		return self._dynamo_update_or_insert(bundle)


	def obj_get(self, request=None, **k):
		"""
		Gets an object in Dynamo
		"""
	
		try:
			item = self._meta.table.get_item(consistent_read=self._meta.consistent_read, **self._hydrate_pk_slug(k['pk']))
		except DynamoDBKeyNotFoundError:
			raise Http404
			
		return DynamoObject(item)


	def obj_delete(self, request=None, **k):
		"""
		Deletes an object in Dynamo
		"""
	
		item = self._meta.table.new_item(**self._hydrate_pk_slug(k['pk']))
		item.delete()


	def rollback(self):
		pass

	def get_object_list(self, request=None):
		pass
	
	def obj_delete_list(self, request=None, **k):
		pass


class DynamoHashResource(DynamoResource):
	"""
	Resource to use for Dynamo tables that only have a hash primary key.
	"""
	
	_hydrate_pk_slug = lambda self, pk: { 'hash_key': self._hash_key_type(pk) }
	_dehydrate_pk_slug = lambda self, obj: str(getattr(obj, self._meta.table.schema.hash_key_name))


class DynamoHashRangeResource(DynamoResource):
	"""
	Resource to use for Dynamo tables that have hash and range keys.
	"""

	def __init__(self, *a, **k):
		super(DynamoHashRangeResource, self).__init__(*a, **k)

		self._range_key_type = int if self._meta.table.schema.range_key_type == 'N' else str
		self._meta.primary_key_delimeter = self._meta.primary_key_delimeter or ':'
		
		if self._meta.primary_key_delimeter in (';', '&', '?'):
			raise Exception('"%" is not a valid delimeter.' % self._meta.primary_key_delimeter)

	prepend_urls = lambda self: (url(r'^(?P<resource_name>%s)/(?P<hash_key>.+)%s(?P<range_key>.+)/$' % (self._meta.resource_name, self._meta.primary_key_delimeter), self.wrap_view('dispatch_detail'), name='api_dispatch_detail'),)

	def full_hydrate(self, *a, **k):
		bundle = super(DynamoHashRangeResource, self).full_hydrate(*a, **k)

		#make sure we get the range key from the request
		range_key_name = self._meta.table.schema.range_key_name
		setattr(bundle.obj, range_key_name, bundle.data.get(range_key_name, None))

		return bundle


	def _hydrate_pk_slug(self, pk):
		keys = {}
		
		#extract the hash/range from the pk
		keys['hash_key'], keys['range_key'] = pk.split(self._meta.primary_key_delimeter)
		
		#make sure they're in the right format
		keys['hash_key'] = self._hash_key_type(keys['hash_key'])
		keys['range_key'] = self._range_key_type(keys['range_key'])
		
		return keys

	def _dehydrate_pk_slug(self, obj):
		keys = [
			str(getattr(obj, self._meta.table.schema.hash_key_name)),
			str(getattr(obj, self._meta.table.schema.range_key_name)),
		]
	
		return self._meta.primary_key_delimeter.join(keys)

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
			'scan_index_forward': self._meta.scan_index_forward if hasattr(self._meta, 'scan_index_forward') else True,
		}
		
		
		#see if there is a range key in the get request (which will override the default, if there was any)
		range_key = request.GET.get(schema.range_key_name, None)
		
		#if a range key value was specified, prepare
		if range_key:
			#get the range key condition
			range_key_condition = self._meta.range_key_condition if hasattr(self._meta, 'range_key_condition') else EQ

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