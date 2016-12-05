from getpass import getpass
from LAPDStopData import app, config

app.config.from_object(config)
app.config['UNAME'] = raw_input('Username: ')
app.config['PASSWD'] = getpass('Password: ')
