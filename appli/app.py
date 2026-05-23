from flask import Flask, render_template, jsonify
from kafka import KafkaConsumer
import json, threading, collections

app = Flask(__name__)

alertes = collections.deque(maxlen=100)

def lire_kafka():
    try:
        consumer = KafkaConsumer(
            'logi_alertes',
            bootstrap_servers=['localhost:29092'],
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        for message in consumer:
            alertes.append(message.value)
    except Exception as e:
        print(f'Erreur Kafka: {e}')

thread = threading.Thread(target=lire_kafka, daemon=True)
thread.start()

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/kpis')
def kpis():
    return render_template('kpis.html')

@app.route('/ml')
def ml():
    return render_template('ml.html')

@app.route('/optimisation')
def optimisation():
    return render_template('optimisation.html')

@app.route('/infrastructure')
def infrastructure():
    return render_template('infrastructure.html')

@app.route('/parametres')
def parametres():
    return render_template('parametres.html')

@app.route('/api/alertes')
def api_alertes():
    data = list(alertes)
    total = len(data)
    critiques = sum(1 for d in data if d.get('statut_alerte') == 'CRITIQUE')
    attention = sum(1 for d in data if d.get('statut_alerte') == 'ATTENTION')
    perte_moy = sum(d.get('risque_perte_score', 0) for d in data) / max(total, 1) * 100
    cout_moy = sum(d.get('cout_par_tonne_fcfa', 0) for d in data) / max(total, 1)
    return jsonify({
        'alertes': data[-20:],
        'kpis': {
            'total': total,
            'critiques': critiques,
            'attention': attention,
            'perte_moy': round(perte_moy, 1),
            'cout_moy': round(cout_moy, 0)
        }
    })

@app.route('/api/kpis')
def api_kpis():
    data = list(alertes)
    total = len(data)

    tonnage = sum(d.get('poids_kg', 0) for d in data) / 1000
    pertes = sum(d.get('poids_kg', 0) * d.get('risque_perte_score', 0) for d in data) / 1000
    perte_pct = sum(d.get('risque_perte_score', 0) for d in data) / max(total, 1) * 100
    cout_total = sum(d.get('cout_total_fcfa', 0) for d in data)
    termines = sum(1 for d in data if d.get('statut_alerte') == 'NORMAL')
    termines_pct = termines / max(total, 1) * 100

    produits = ['MANGUE', 'TOMATE', 'ARACHIDE', 'RIZ']
    perte_par_produit = {}
    for p in produits:
        items = [d for d in data if d.get('produit') == p]
        perte_par_produit[p] = round(
            sum(d.get('risque_perte_score', 0) for d in items) / max(len(items), 1) * 100, 1
        )

    zones = ['CASAMANCE', 'SINE_SALOUM', 'BASSIN_ARACHIDIER', 'NIAYES']
    cout_par_zone = {}
    for z in zones:
        items = [d for d in data if d.get('zone_origine') == z]
        cout_par_zone[z] = round(
            sum(d.get('cout_par_tonne_fcfa', 0) for d in items) / max(len(items), 1), 0
        )

    destinations = ['DAKAR', 'EXPORT', 'THIES', 'KAOLACK']
    goulots = {}
    for p in produits:
        goulots[p] = {}
        for dest in destinations:
            items = [d for d in data if d.get('produit') == p and d.get('zone_dest') == dest]
            if items:
                score = sum(d.get('risque_perte_score', 0) for d in items) / len(items)
                if score > 0.4:
                    goulots[p][dest] = 'CRITIQUE'
                elif score > 0.3:
                    goulots[p][dest] = 'MODERE'
                elif score > 0.2:
                    goulots[p][dest] = 'ATTENTION'
                else:
                    goulots[p][dest] = 'NORMAL'
            else:
                goulots[p][dest] = 'NORMAL'

    return jsonify({
        'cards': {
            'tonnage': round(tonnage, 1),
            'pertes': round(pertes, 1),
            'perte_pct': round(perte_pct, 1),
            'cout_total': round(cout_total / 1_000_000, 1),
            'termines': termines,
            'termines_pct': round(termines_pct, 1)
        },
        'perte_par_produit': perte_par_produit,
        'cout_par_zone': cout_par_zone,
        'goulots': goulots
    })

if __name__ == '__main__':
    app.run(debug=True, port=5001, host='0.0.0.0')