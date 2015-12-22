import setuptools

CLASSIFIERS = ['Development Status :: 3 - Alpha',
               'Intended Audience :: Developers',
               'License :: OSI Approved :: BSD License',
               'Operating System :: OS Independent',
               'Programming Language :: Python :: 2',
               'Programming Language :: Python :: 2.7',
               'Programming Language :: Python :: 3',
               'Programming Language :: Python :: 3.3',
               'Programming Language :: Python :: 3.4',
               'Programming Language :: Python :: 3.5',
               'Programming Language :: Python :: Implementation :: CPython',
               'Programming Language :: Python :: Implementation :: PyPy',
               'Topic :: Communications',
               'Topic :: Internet',
               'Topic :: Software Development :: Libraries']

DESC = 'An Asynchronous DynamoDB client Tornado'

TESTS_REQUIRE = ['nose', 'mock', 'coverage']

setuptools.setup(name='tornado-dynamodb',
                 version='0.1.0',
                 description=DESC,
                 long_description=open('README.rst').read(),
                 author='Gavin M. Roy',
                 author_email='gavinmroy@gmail.com',
                 url='http://tornado-dynamodb.readthedocs.org',
                 packages=['tornado_dynamodb'],
                 package_data={'': ['LICENSE', 'README.rst', 'requirements.txt']},
                 include_package_data=True,
                 install_requires=['arrow', 'tornado-aws'],
                 tests_require=TESTS_REQUIRE,
                 license='BSD',
                 classifiers=CLASSIFIERS,
                 zip_safe=True)
