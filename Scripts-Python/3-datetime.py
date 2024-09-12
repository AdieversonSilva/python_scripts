#Manipulando Dtas com strftime e strptime
from datetime import datetime

data_hora_atual = datetime.now()
data_hora_str = '2023-10-20 10:20'
mascara_ptbr = '%d/%m/%Y %a'
mascara_nova = "%Y-%m-%d %H:%M"

print(data_hora_atual.strftime(mascara_ptbr)) #strftime é para transformar a data com base em uma mascara
print(datetime.strptime(data_hora_str, mascara_nova).strftime(mascara_ptbr))#strptime é converter o formato da data, porém tem que ser usada forma que está o string 