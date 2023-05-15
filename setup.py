from setuptools import setup, find_packages
 
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
  long_description=open('README.txt').read() + '\n\n' + open('CHANGELOG.txt').read(),
  url='',  
  author='Fernando Caemerer',
  author_email='fernando.caemerer@gmail.com',
  license='MIT', 
  classifiers=classifiers,
  keywords=['dataengineer', 'pyspark', 'dataframe'], 
  packages=find_packages(),
  install_requires=['pyspark'] 
)