torque module
=============

this module makes it easier to use python scripts within the Torque
queue system. 

>>> import torque

When you add that line to the top of your script, it will
automatically change into the working directory of the script when run
by torque, and it will select a non-X backend for matplotlib.

if you set $PBS_DEBUG before

>>> os.environ['PBS_DEBUG'] = ''
>>> import torque

then the output of a script called printlocalsetup will be shown which
contains a lot of information about your setup.

to interact with the torque server, you can do this:

>>> from torque.torque import *

>>> pbs = PBS()
