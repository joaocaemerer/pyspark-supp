from setuptools import setup, find_packages
from os import environ
 
long_description = 'This library was made to help Data Engineer that uses Pyspark data frames. Using functions to compare, test and check data.'

classifiers = [
  'Development Status :: 1 - Development',
  'Intended Audience :: Data Engineers',
  'Operating System :: All',
  'License :: OSI Approved :: MIT License',
  'Programming Language :: Python :: 3'
]

pkg_name = ((environ.get('PKG_PREFIX').strip() + '.') if (environ.get('PKG_PREFIX') is not None) else '') + 'pyspark_supp'

print('Package name: ' + pkg_name)

setup(
  name='pyspark_supp',
  version='0.1.0',
  description='Data Engineer Support PySpark Library',
  long_description=long_description,
  url='https://github.com/joaocaemerer/pyspark-supp',
  author='Fernando Caemerer',
  author_email='fernando.caemerer@gmail.com',
  license='MIT',
  keywords=['dataengineer', 'pyspark', 'dataframe'], 
  packages=find_packages(),
  install_requires=['pyspark']
)