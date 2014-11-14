CLIENT_DIR = '.'
XMLRCP_SSL = False
XMLRCP_SSL_CA_CERTS = CLIENT_DIR + "/pki/ca-chain.pem"

# URL of the IM to connect to find the spot nodes
IM_URL = 'http://localhost:8899'

# File with the authentication data to use to connect to the IM
AUTH_FILE = "/usr/local/clues/bin/cloud/auth.dat"

# Log file route
LOG_FILE = "/usr/local/ckptman/log/ckptman.log"

# Chechpointing algorithm to apply ('HOUR', 'THRESHOLD')
CKPT_ALGORITHM = 'THRESHOLD'

# Time to revalue 
REVALUE_TIME = 60

# Checkpointing time margin 
CKPT_TIME_MARGIN = 100

# Threshold (in percentage) for the THRESHOLD algorithm
THRESHOLD = 20