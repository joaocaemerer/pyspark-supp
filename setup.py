from setuptools import setup, find_packages
 
long_description = 'This library was made to help Data Engineer that uses Pyspark data frames. Using functions to compare, test and check data.'

classifiers = [
  'Development Status :: 1 - Development',
  'Intended Audience :: Data Engineers',
  'Operating System :: All',
  'License :: OSI Approved :: MIT License',
  'Programming Language :: Python :: 3'
]
 
setup(
  name='pyspark_supp',
  version='0.0.1',
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