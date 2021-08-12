import os
import yaml as yml

def get_env():
  env = os.environ['PYTHON_ENV']
  return env

def get_env_vars(path):
  with open(path) as f:
    return yml.full_load(stream=f)

def load_yml_env(path):
  env_vars = get_env_vars(path)
  for k in env_vars:
    os.environ[k] = str(env_vars[k])

def make_dir(path):
  os.system(f'mkdir -p {path}')

def get_elapsed_time(begin_time):
  elapsed_time = time.time() - begin_time
  elapsed_time = strftime('%H:%M:%S',gmtime(elapsed_time))
  return elapsed_time  