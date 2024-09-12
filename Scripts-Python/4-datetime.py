# Trabalhando com biblioteca pytz
from datetime import datetime
import pytz

data = datetime.now(pytz.timezone('Europe/Paris'))
data2 = datetime.now(pytz.timezone('America/Sao_Paulo'))

print(data, data2)
