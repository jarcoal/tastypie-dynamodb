from tastypie.authentication import Authentication

class DjangoAuthentication(Authentication):
	"""
	Uses Django's default auth app.
	"""
	def is_authenticated(self, request, **k):
		return request.user.is_authenticated()
	
	def get_identifier(self, request):
		return request.user


class StandardAuth(DjangoAuthentication):
	"""
	Allows anonymous reads, but requires authentication for writes.
	"""
	def is_authenticated(self, request, **k):
		if request.method == 'GET':
			return True
		
		return super(StandardAuth, self).is_authenticated(request, **k)