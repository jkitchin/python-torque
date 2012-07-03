from distutils.core import setup

setup(name = 'torque',
      version='0.1',
      description='python interface to torque',
      url='',
      maintainer='John Kitchin',
      maintainer_email='jkitchin@andrew.cmu.edu',
      license='LGPL',
      platforms=['linux'],
      packages=['torque'],
      scripts=['torque/printlocalsetup.py'],
      long_description='''python interface to torque''')
