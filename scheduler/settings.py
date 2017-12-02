AUTH = True 
#DB = 'postgresql://test:test@localhost/schedulerapi'
# KMH: need to change this for my slurm test
with open('psql', 'rt') as fh:
    DB = fh.read().rstrip()
 
PROXIES = True

ALLOWED_DOCKER_REGISTRIES = [
    "quay.io/cdis/",
    "quay.io/ncigdc/",
]
