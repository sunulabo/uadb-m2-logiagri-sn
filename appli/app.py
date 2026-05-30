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

@app.route('/api/optimisation')
def api_optimisation():
    from scipy.optimize import linprog
    from collections import Counter
    import numpy as np

    data = list(alertes)

    routes = [
        ('CASAMANCE', 'DAKAR'),
        ('CASAMANCE', 'EXPORT'),
        ('CASAMANCE', 'THIES'),
        ('BASSIN_ARACHIDIER', 'DAKAR'),
        ('BASSIN_ARACHIDIER', 'KAOLACK'),
        ('SINE_SALOUM', 'DAKAR'),
        ('SINE_SALOUM', 'THIES'),
        ('NIAYES', 'DAKAR'),
        ('NIAYES', 'KAOLACK'),
    ]

    couts_actuels = []
    tonnages = []
    pertes = []
    produits_dominants = []

    for orig, dest in routes:
        items = [d for d in data if d.get('zone_origine') == orig and d.get('zone_dest') == dest]
        if items:
            cout = sum(d.get('cout_par_tonne_fcfa', 0) for d in items) / len(items)
            tonnage = sum(d.get('poids_kg', 0) for d in items) / 1000
            perte = sum(d.get('risque_perte_score', 0) for d in items) / len(items)
            produit_counts = Counter(d.get('produit', '') for d in items)
            produit_dominant = produit_counts.most_common(1)[0][0] if produit_counts else 'N/A'
        else:
            cout = 35000
            tonnage = 0
            perte = 0
            produit_dominant = 'N/A'

        couts_actuels.append(round(cout, 0))
        tonnages.append(round(tonnage, 1))
        pertes.append(round(perte * 100, 1))
        produits_dominants.append(produit_dominant)

    n = len(routes)
    c = np.array(couts_actuels, dtype=float)
    A_ub = -np.eye(n)
    b_ub = np.zeros(n)
    bounds = [(0, max(t * 1.2, 1)) for t in tonnages]
    linprog(c, A_ub=A_ub, b_ub=b_ub, bounds=bounds, method='highs')

    couts_optimises = []
    statuts = []
    for i in range(n):
        if pertes[i] > 30:
            couts_optimises.append(round(couts_actuels[i] * 1.1, 0))
            statuts.append('ÉVITER')
        elif pertes[i] > 15:
            couts_optimises.append(round(couts_actuels[i] * 0.95, 0))
            statuts.append('ALTERNATIF')
        else:
            couts_optimises.append(round(couts_actuels[i] * 0.85, 0))
            statuts.append('OPTIMAL')

    total_actuel = sum(couts_actuels[i] * tonnages[i] for i in range(n))
    total_optimise = sum(couts_optimises[i] * tonnages[i] for i in range(n))
    economie = total_actuel - total_optimise
    reduction_pct = round(economie / max(total_actuel, 1) * 100, 1)

    zones = ['CASAMANCE', 'SINE_SALOUM', 'BASSIN_ARACHIDIER', 'NIAYES']
    pertes_par_zone = {}
    for z in zones:
        items = [d for d in data if d.get('zone_origine') == z]
        pertes_par_zone[z] = round(
            sum(d.get('poids_kg', 0) * d.get('risque_perte_score', 0) for d in items) / 1000, 1
        )

    cout_transport_economie = round(sum(
        (couts_actuels[i] - couts_optimises[i]) * tonnages[i]
        for i in range(n) if couts_optimises[i] < couts_actuels[i]
    ) / 1_000_000, 1)

    pertes_evitees = round(sum(
        tonnages[i] * pertes[i] / 100
        for i in range(n) if statuts[i] == 'ÉVITER'
    ), 1)

    return jsonify({
        'cards': {
            'economie': round(economie / 1000, 1),
            'routes_optimisees': len([i for i in range(n) if statuts[i] == 'OPTIMAL']),
            'reduction_pct': reduction_pct,
            'tonnes_total': round(sum(tonnages), 1)
        },
        'banniere': {
            'reduction_pct': reduction_pct,
            'tonnes_evitees': pertes_evitees,
            'economie_m': round(economie / 1_000_000, 1)
        },
        'routes': [
            {
                'produit': produits_dominants[i],
                'origine': routes[i][0],
                'destination': routes[i][1],
                'tonnage': tonnages[i],
                'cout_optimise': int(couts_optimises[i]),
                'reduction_pct': round((couts_actuels[i] - couts_optimises[i]) / max(couts_actuels[i], 1) * 100, 1),
                'statut': statuts[i]
            }
            for i in range(n)
        ],
        'economies': {
            'cout_transport': cout_transport_economie,
            'pertes_evitees': round(pertes_evitees * 35000 / 1_000_000, 1),
            'total': round(economie / 1_000_000, 1)
        },
        'pertes_par_zone': pertes_par_zone
    })

@app.route('/api/infrastructure')
def api_infrastructure():
    import psutil

    ram = psutil.virtual_memory()
    cpu = psutil.cpu_percent(interval=1)
    disque = psutil.disk_usage('/')

    return jsonify({
        'systeme': {
            'ram_total': round(ram.total / 1024**3, 1),
            'ram_used': round(ram.used / 1024**3, 1),
            'ram_pct': ram.percent,
            'cpu_pct': cpu,
            'disque_total': round(disque.total / 1024**3, 1),
            'disque_used': round(disque.used / 1024**3, 1),
            'disque_pct': round(disque.percent, 1)
        }
    })

if __name__ == '__main__':
    app.run(debug=True, port=5001, host='0.0.0.0')