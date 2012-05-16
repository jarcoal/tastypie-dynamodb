from django.conf.urls import url
from django.http import Http404

from boto.dynamodb.condition import EQ, ConditionTwoArgs
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError

from tastypie.resources import Resource
from dynamopie.objects import DynamoObject


class DynamoResource(Resource):
	"""
	Root Dynamo Resource.  This should be treated as abstract.
	Inherit from DynamoHashResource or DynamoHashRangeResource.
	"""

	@property
	def table(self):
		if not hasattr(self._meta, 'table'):
			raise Exception('No table provided')

		return self._meta.table
	
	@property
	def hash_key_type(self):
		return int if self.table.schema.hash_key_type == 'N' else str
	
	@property
	def consistent_read(self):
		return self._meta.consistent_read if hasattr(self._meta, 'consistent_read') else False

	def get_resource_uri(self, bundle):
		return self._build_reverse_url('api_dispatch_detail', kwargs={
			'resource_name': self._meta.resource_name,
			'pk': self.dehydrate_pk_slug(bundle.obj),
			'api_name': self._meta.api_name,
		})

	def base_urls(self):
		#up
		urls = super(DynamoResource, self).base_urls()
		
		#insert our url that allows for the <pk> to have additional symbols
		urls[3] = url(r'^(?P<resource_name>%s)/(?P<pk>[^\&\;\?]+)/$' % self._meta.resource_name, self.wrap_view('dispatch_detail'), name='api_dispatch_detail')
		
		#ship
		return urls


	def _dynamo_update_or_insert(self, bundle, params=None, update=False):
		#check for params
		params = params or {}
	
		#hydrate the bundle
		bundle = self.full_hydrate(bundle)
		
		#create our item
		item = self.table.new_item(**params)
		
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
		Updates an item in Dynamo
		"""
		return self._dynamo_update_or_insert(bundle, params=self.hydrate_pk_slug(k['pk']), update=True)


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
			item = self.table.get_item(consistent_read=self.consistent_read, **self.hydrate_pk_slug(k['pk']))
		except DynamoDBKeyNotFoundError:
			raise Http404
			
		return DynamoObject(item)


	def obj_delete(self, request=None, **k):
		"""
		Deletes an object in Dynamo
		"""
	
		item = self.table.new_item(**self.hydrate_pk_slug(k['pk']))
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
	
	def hydrate_pk_slug(self, pk):
		return { 'hash_key': self.hash_key_type(pk) }

	def dehydrate_pk_slug(self, obj):
		return str(getattr(obj, self.table.schema.hash_key_name))


class DynamoHashRangeResource(DynamoResource):
	"""
	Resource to use for Dynamo tables that have hash and range keys.
	"""

	@property
	def range_key_type(self):
		return int if self.table.schema.range_key_type == 'N' else str

	@property
	def primary_key_delimeter(self):
		delimeter = self._meta.primary_key_delimeter if hasattr(self._meta, 'primary_key_delimeter') else ':'
		
		if delimeter in (';', '&', '?'):
			raise Exception('"%" is not a valid delimeter.' % delimeter)		
		
		return delimeter

	def hydrate_pk_slug(self, pk):
		keys = {}
		
		#extract the hash/range from the pk
		keys['hash_key'], keys['range_key'] = pk.split(self.primary_key_delimeter)
		
		#make sure they're in the right format
		keys['hash_key'] = self.hash_key_type(keys['hash_key'])
		keys['range_key'] = self.range_key_type(keys['range_key'])
		
		return keys

	def dehydrate_pk_slug(self, obj):
		keys = [
			str(getattr(obj, self.table.schema.hash_key_name)),
			str(getattr(obj, self.table.schema.range_key_name)),
		]
	
		return self.primary_key_delimeter.join(keys)

	def obj_get_list(self, request=None, **k):
		schema = self.table.schema
	
		#work out the hash key
		hash_key = request.GET.get(schema.hash_key_name, None)
		
		if not hash_key:
			raise Http404
	
		#get initial params
		params = {
			'hash_key': self.hash_key_type(hash_key),
			'request_limit': self._meta.limit,
			'consistent_read': self.consistent_read,
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
				range_values['v1'], range_values['v2'] = [self.range_key_type(i) for i in range_key.split(self.primary_key_delimeter)]
			else:
				#setup the value that the class will be instantiated with
				range_values['v1'] = self.range_key_type(range_key)
			
			#instantiate the range condition class
			range_key_condition = range_key_condition(**range_values)
			
			#drop in the condition
			params['range_key_condition'] = range_key_condition


		#perform the query
		results = self.table.query(**params)

		#return the results
		return [DynamoObject(obj) for obj in results]