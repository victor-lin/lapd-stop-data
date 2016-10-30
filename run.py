import logging

from LAPDStopData import app
from getpass import getpass


app.config['UNAME'] = raw_input('Username: ')
app.config['PASSWD'] = getpass('Password: ')

app.run(debug=True)

if not app.debug:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s",
                                  "%b %d %H:%M:%S")
    handler.setFormatter(formatter)
    app.logger.addHandler(handler)
