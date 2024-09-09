# Manipulando Datas - Com timedelta
from datetime import timedelta, datetime

tipo_carro = 'm'
tempo_pequeno = 30
tempo_medio = 45
tempo_grande = 60

data_atual = datetime.now()

if tipo_carro == 'p':
    data_estimada = data_atual + timedelta(minutes=tempo_pequeno)
    print(f'O carro chegou: {data_atual} e ficará pronto às {data_estimada}')
elif tipo_carro == 'm':
    data_estimada = data_atual + timedelta(minutes=tempo_medio)
    print(f'O carro chegou: {data_atual} e ficará pronto às {data_estimada}')
else:
    data_estimada = data_atual + timedelta(minutes=tempo_grande)
    print(f'O carro chegou: {data_atual} e ficará pronto às {data_estimada}')
