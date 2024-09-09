#Manipulando Dtas com strftime e strptime
from datetime import datetime

data_hora_atual = datetime.now()
data_hora_str = '2023-10-20 10:20'
mascara_ptbr = '%d/%m/%Y'

print(data_hora_atual.strftime(mascara_ptbr))