
import os
import subprocess
import signal
import rospy
import rospkg
import time

class Racer:
  def __init__(self, grammar=None):

    self.process = None

    rospack = rospkg.RosPack()
    pkg_path = rospack.get_path('lingua_kb')

    self.executable = os.path.join(pkg_path, 'ccl', 'ccl')
    self.config = os.path.join(pkg_path, 'ccl', 'setup.lisp')
  
  def __del__(self):
    self.stop()

  def start(self):
    FNULL = open(os.devnull, 'w')
    
    self.process = subprocess.Popen(
      args=' '.join([
          self.executable, 
          '-e', '"(load \\"{}\\")"'.format(self.config), 
          '-e', '"(ql:quickload \'racer)"', 
          '-e', '"(racer:racer-toplevel)"'
        ]), 
      shell=True,
      executable='/bin/bash',
      stdin=subprocess.PIPE,
      stdout=subprocess.PIPE,
      stderr=FNULL
    )

    # Give the service time to start up
    rospy.loginfo('Waiting for knowledge engine to come up...')
    time.sleep(4)

  def stop(self):
    if self.process:
      self.process.terminate()
      self.process.wait()
      self.process = None

  def __enter__(self):
    self.start()
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.stop()