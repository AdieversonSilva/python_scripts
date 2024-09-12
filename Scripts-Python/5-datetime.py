# Trabalhando com fusos em datetime
from datetime import datetime, timedelta, timezone

data_paris = datetime.now(timezone(timedelta(hours=5)))
data_brasil = datetime.now(timezone(timedelta(hours=-3)))

print(data_paris)
print(data_brasil)
