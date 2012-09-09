from setuptools import setup

setup(
    name="tastypie-dynamodb",
    version="0.1.0",
    description="Amazon DynamoDB Adapter for Django-Tastypie",
    long_description="Amazon DynamoDB Adapter for Django-Tastypie",
    keywords="django, tastypie, dynamodb",
    author="Jared Morse",
    author_email="jarcoal@gmail.com",
    url="https://github.com/jarcoal/tastypie-dynamodb",
    license="BSD",
    packages=["tastypie_dynamodb"],
    zip_safe=False,
    install_requires=[
        'django >= 1.2',
        'django-tastypie >= 0.9.11',
    ],
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Framework :: Django",
        "Environment :: Web Environment",
    ],
)