# afficher_kpis.py — Affichage KPIs dans le terminal
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'logi_alertes',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print('=== KPIs Logi-Agri SN ===')
print(f'{"Produit":<12} {"Statut":<10} {"Perte":>8} {"Coût/tonne":>15}')
print('-' * 50)

for message in consumer:
    d = message.value
    print(f'{d["produit"]:<12} {d["statut_alerte"]:<10} {d["risque_perte_score"]*100:>7.1f}% {d["cout_par_tonne_fcfa"]:>14.0f} FCFA')
    