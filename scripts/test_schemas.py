"""
tests/test_schemas.py — Tests unitaires Pytest Logi-Agri SN
UADB | Master 2 Big Data & IA | 2025-2026
Couvre :
  - Validation schémas Pandera (valide/invalide)
  - Calcul pct_perte_reel
  - Anonymisation SHA-256
  - Edge cases (DataFrame vide, nulls)
"""

import pytest
import pandas as pd
import numpy as np
import hashlib
import sys
import os

# Ajouter le répertoire parent au path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from schema import (
    TransportLogisticsSchema,
    StockEntrepotSchema,
    CapteurIoTSchema,
    validate_and_filter,
    anonymize_pii,
    get_schema_stats,
    SALT,
)
from src.kafka_producer_logi import compute_pct_perte, gen_transport, gen_capteur


# ══════════════════════════════════════════════════════════════════════════
# FIXTURES
# ══════════════════════════════════════════════════════════════════════════

@pytest.fixture
def transport_valide():
    """DataFrame de transport valide minimal."""
    return pd.DataFrame([{
        'voyage_id':       'VY12345',
        'produit':         'MANGUE',
        'poids_kg':        5000.0,
        'temp_moyenne_c':  10.0,
        'temp_max_tolere': 12.0,
        'delai_prevu_h':   48,
        'date_depart':     '2025-06-15',
        'zone_origine':    'CASAMANCE',
        'zone_dest':       'DAKAR',
        'distance_km':     490.0,
        'cout_fcfa_km':    480.0,
        'pct_perte_reel':  None,
        'statut':          'EN_COURS',
    }])


@pytest.fixture
def transport_invalide():
    """DataFrame avec plusieurs violations de schéma."""
    return pd.DataFrame([{
        'voyage_id':       'INVALID_ID',       # Ne correspond pas ^VY\d{5}$
        'produit':         'MANIOC',            # Produit non valide
        'poids_kg':        -100.0,              # Négatif
        'temp_moyenne_c':  100.0,               # Hors plage [2, 45]
        'temp_max_tolere': 12.0,
        'delai_prevu_h':   500,                 # > 168h
        'date_depart':     '15-06-2025',        # Format invalide
        'zone_origine':    'KAOLACK',           # Pas une zone de production
        'zone_dest':       'ZIGUINCHOR',        # Zone dest non valide
        'distance_km':     2000.0,              # > 800km
        'cout_fcfa_km':    50.0,                # < 200 (min)
        'pct_perte_reel':  1.5,                 # > 1.0
        'statut':          'EN_COURS',
    }])


@pytest.fixture
def stock_valide():
    return pd.DataFrame([{
        'entrepot_id':     'ENT_DAK01',
        'produit':         'TOMATE',
        'quantite_kg':     15000.0,
        'temp_entrepot_c': 8.0,
        'humidite_pct':    70.0,
        'capacite_max_kg': 50000.0,
        'date_entree':     '2025-06-15',
        'date_sortie':     None,
    }])


@pytest.fixture
def capteur_valide():
    return pd.DataFrame([{
        'capteur_id':     'CPT1234',
        'voyage_id':      'VY12345',
        'timestamp_utc':  '2025-06-15T08:30:00',
        'temperature_c':  15.0,
        'latitude':       14.692,
        'longitude':      -17.446,
        'batterie_pct':   85.0,
        'signal_qualite': 4,
    }])


# ══════════════════════════════════════════════════════════════════════════
# TESTS — SCHÉMA TRANSPORT
# ══════════════════════════════════════════════════════════════════════════

class TestTransportSchema:

    def test_transport_valide_passe(self, transport_valide):
        """Un transport valide doit passer sans erreur."""
        result = validate_and_filter(transport_valide, TransportLogisticsSchema)
        assert len(result) == 1, "Le transport valide doit être conservé"

    def test_transport_invalide_rejete(self, transport_invalide):
        """Un transport invalide doit être rejeté ou filtré."""
        result = validate_and_filter(transport_invalide, TransportLogisticsSchema)
        assert len(result) == 0, "Le transport invalide doit être rejeté"

    def test_voyage_id_unique(self):
        """Deux voyages avec le même ID doivent déclencher une erreur d'unicité."""
        df = pd.DataFrame([
            {
                'voyage_id': 'VY99999', 'produit': 'RIZ',
                'poids_kg': 1000.0, 'temp_moyenne_c': 25.0, 'temp_max_tolere': 35.0,
                'delai_prevu_h': 24, 'date_depart': '2025-06-15',
                'zone_origine': 'NIAYES', 'zone_dest': 'DAKAR',
                'distance_km': 45.0, 'cout_fcfa_km': 290.0,
                'pct_perte_reel': None, 'statut': 'EN_COURS',
            },
            {
                'voyage_id': 'VY99999',  # DUPLICATE
                'produit': 'RIZ', 'poids_kg': 2000.0, 'temp_moyenne_c': 20.0,
                'temp_max_tolere': 35.0, 'delai_prevu_h': 12, 'date_depart': '2025-06-15',
                'zone_origine': 'NIAYES', 'zone_dest': 'DAKAR',
                'distance_km': 45.0, 'cout_fcfa_km': 290.0,
                'pct_perte_reel': None, 'statut': 'EN_COURS',
            },
        ])
        result = validate_and_filter(df, TransportLogisticsSchema)
        assert len(result) < 2, "Les doublons de voyage_id doivent être détectés"

    def test_pct_perte_nullable(self, transport_valide):
        """pct_perte_reel peut être None (non connu à l'envoi)."""
        transport_valide['pct_perte_reel'] = None
        result = validate_and_filter(transport_valide, TransportLogisticsSchema)
        assert len(result) == 1

    def test_tous_produits_valides(self):
        """Tester que les 4 produits valides passent le schéma."""
        for produit in ['MANGUE', 'ARACHIDE', 'RIZ', 'TOMATE']:
            df = pd.DataFrame([{
                'voyage_id': f'VY1000{produit[:1]}', 'produit': produit,
                'poids_kg': 5000.0, 'temp_moyenne_c': 20.0, 'temp_max_tolere': 25.0,
                'delai_prevu_h': 24, 'date_depart': '2025-06-15',
                'zone_origine': 'SINE_SALOUM', 'zone_dest': 'DAKAR',
                'distance_km': 280.0, 'cout_fcfa_km': 355.0,
                'pct_perte_reel': None, 'statut': 'EN_COURS',
            }])
            result = validate_and_filter(df, TransportLogisticsSchema)
            assert len(result) == 1, f'Produit {produit} devrait être valide'

    def test_dataframe_vide(self):
        """Un DataFrame vide doit être géré sans exception."""
        df = pd.DataFrame(columns=[
            'voyage_id', 'produit', 'poids_kg', 'temp_moyenne_c', 'temp_max_tolere',
            'delai_prevu_h', 'date_depart', 'zone_origine', 'zone_dest',
            'distance_km', 'cout_fcfa_km', 'pct_perte_reel', 'statut'
        ])
        result = validate_and_filter(df, TransportLogisticsSchema)
        assert len(result) == 0


# ══════════════════════════════════════════════════════════════════════════
# TESTS — SCHÉMA STOCK
# ══════════════════════════════════════════════════════════════════════════

class TestStockSchema:

    def test_stock_valide(self, stock_valide):
        result = validate_and_filter(stock_valide, StockEntrepotSchema)
        assert len(result) == 1

    def test_stock_depasse_capacite(self):
        """Un stock dépassant la capacité max doit être invalide."""
        df = pd.DataFrame([{
            'entrepot_id': 'ENT_DAK01', 'produit': 'RIZ',
            'quantite_kg': 150000.0,     # > capacite_max_kg
            'temp_entrepot_c': 25.0, 'humidite_pct': 60.0,
            'capacite_max_kg': 100000.0,
            'date_entree': '2025-06-15', 'date_sortie': None,
        }])
        result = validate_and_filter(df, StockEntrepotSchema)
        assert len(result) == 0, "Stock > capacité max doit être rejeté"

    def test_conditions_mangue_optimales(self):
        """Mangue à 10°C et 88% humidité doit passer."""
        df = pd.DataFrame([{
            'entrepot_id': 'ENT_DAK02', 'produit': 'MANGUE',
            'quantite_kg': 5000.0, 'temp_entrepot_c': 10.0,
            'humidite_pct': 88.0, 'capacite_max_kg': 50000.0,
            'date_entree': '2025-06-15', 'date_sortie': None,
        }])
        result = validate_and_filter(df, StockEntrepotSchema)
        assert len(result) == 1

    def test_conditions_mangue_trop_chaud(self):
        """Mangue à 20°C (> 12°C) doit générer une alerte."""
        df = pd.DataFrame([{
            'entrepot_id': 'ENT_DAK02', 'produit': 'MANGUE',
            'quantite_kg': 5000.0, 'temp_entrepot_c': 20.0,  # Trop chaud
            'humidite_pct': 75.0, 'capacite_max_kg': 50000.0,
            'date_entree': '2025-06-15', 'date_sortie': None,
        }])
        result = validate_and_filter(df, StockEntrepotSchema)
        # Check Mangue temp > 12°C → ligne rejetée par dataframe_check
        assert len(result) == 0


# ══════════════════════════════════════════════════════════════════════════
# TESTS — SCHÉMA CAPTEUR IoT
# ══════════════════════════════════════════════════════════════════════════

class TestCapteurSchema:

    def test_capteur_valide(self, capteur_valide):
        result = validate_and_filter(capteur_valide, CapteurIoTSchema)
        assert len(result) == 1

    def test_coordonnees_hors_senegal(self):
        """GPS hors Sénégal (Paris) doit être rejeté."""
        df = pd.DataFrame([{
            'capteur_id': 'CPT5678', 'voyage_id': 'VY12345',
            'timestamp_utc': '2025-06-15T08:30:00',
            'temperature_c': 15.0,
            'latitude': 48.8566,   # Paris — hors plage [12, 15.5]
            'longitude': 2.3522,   # Paris — hors plage [-17.5, -11.5]
            'batterie_pct': 90.0, 'signal_qualite': 5,
        }])
        result = validate_and_filter(df, CapteurIoTSchema)
        assert len(result) == 0


# ══════════════════════════════════════════════════════════════════════════
# TESTS — CALCUL PCT_PERTE
# ══════════════════════════════════════════════════════════════════════════

class TestCalculPerte:

    def test_mangue_temperature_ok(self):
        """Mangue à 10°C (< 12°C max) doit avoir une perte proche de la base."""
        perte = compute_pct_perte('MANGUE', temp_c=10.0, delai_h=24)
        assert 0.0 <= perte <= 1.0
        assert perte < 0.2, f"Perte trop élevée pour Mangue à temp optimale : {perte}"

    def test_mangue_temperature_critique(self):
        """Mangue à 40°C (>> 12°C) doit déclencher une perte élevée (> 0.4)."""
        perte = compute_pct_perte('MANGUE', temp_c=40.0, delai_h=80)
        assert perte > 0.4, f"Mangue à 40°C devrait avoir perte critique : {perte}"

    def test_riz_robuste(self):
        """Riz à 30°C (< 35°C max) sur durée courte doit avoir faible perte."""
        perte = compute_pct_perte('RIZ', temp_c=30.0, delai_h=100)
        assert perte < 0.15

    def test_perte_bornee(self):
        """pct_perte doit toujours être dans [0, 1]."""
        for _ in range(100):
            p = compute_pct_perte('TOMATE', temp_c=50.0, delai_h=200, humidite=100.0)
            assert 0.0 <= p <= 1.0, f"Perte hors bornes : {p}"

    def test_perte_monotone_avec_temperature(self):
        """La perte doit augmenter avec la température pour Tomate."""
        pertes = [compute_pct_perte('TOMATE', temp_c=t, delai_h=24)
                  for t in [5.0, 10.0, 20.0, 30.0, 40.0]]
        # Tendance générale croissante (tolérance bruit gaussien)
        assert pertes[-1] > pertes[0], "Perte doit augmenter avec température"


# ══════════════════════════════════════════════════════════════════════════
# TESTS — ANONYMISATION PII
# ══════════════════════════════════════════════════════════════════════════

class TestAnonymisation:

    def test_pii_remplace_par_hash(self):
        """La colonne PII doit être remplacée par une version hachée."""
        df = pd.DataFrame([{'raw_entity_id': 'T_ABC12345', 'produit': 'RIZ'}])
        result = anonymize_pii(df, ['raw_entity_id'])

        assert 'raw_entity_id' not in result.columns, "PII original ne doit plus exister"
        assert 'raw_entity_id_secure' in result.columns, "Hash doit être présent"

    def test_hash_deterministe(self):
        """Le même ID doit produire le même hash."""
        df1 = pd.DataFrame([{'raw_entity_id': 'T_XYZ999', 'v': 1}])
        df2 = pd.DataFrame([{'raw_entity_id': 'T_XYZ999', 'v': 2}])
        r1 = anonymize_pii(df1, ['raw_entity_id'])
        r2 = anonymize_pii(df2, ['raw_entity_id'])
        assert r1['raw_entity_id_secure'].iloc[0] == r2['raw_entity_id_secure'].iloc[0]

    def test_hash_sha256_avec_sel(self):
        """Vérification manuelle du calcul SHA-256 + sel."""
        entity_id = 'T_TEST001'
        df = pd.DataFrame([{'raw_entity_id': entity_id, 'data': 'x'}])
        result = anonymize_pii(df, ['raw_entity_id'])

        expected_hash = hashlib.sha256(f'{entity_id}{SALT}'.encode()).hexdigest()
        assert result['raw_entity_id_secure'].iloc[0] == expected_hash

    def test_pii_null_gere(self):
        """Une valeur NULL dans le champ PII ne doit pas lever d'exception."""
        df = pd.DataFrame([{'raw_entity_id': None, 'data': 'x'}])
        result = anonymize_pii(df, ['raw_entity_id'])
        assert result['raw_entity_id_secure'].iloc[0] is None


# ══════════════════════════════════════════════════════════════════════════
# TESTS — STATISTIQUES QUALITÉ
# ══════════════════════════════════════════════════════════════════════════

class TestQualiteStats:

    def test_stats_100_pct_valides(self, capteur_valide):
        stats = get_schema_stats(capteur_valide, CapteurIoTSchema)
        assert stats['taux_qualite'] == 100.0
        assert stats['valides'] == 1

    def test_stats_avec_invalides(self, transport_invalide):
        stats = get_schema_stats(transport_invalide, TransportLogisticsSchema)
        assert stats['taux_qualite'] < 100.0

    def test_stats_df_vide(self):
        df = pd.DataFrame()
        stats = get_schema_stats(df, TransportLogisticsSchema)
        assert stats['total'] == 0
        assert stats['taux_qualite'] == 0.0


# ══════════════════════════════════════════════════════════════════════════
# TESTS — SIMULATEUR
# ══════════════════════════════════════════════════════════════════════════

class TestSimulateur:

    def test_gen_transport_champs_requis(self):
        """L'événement généré doit contenir tous les champs nécessaires."""
        evt = gen_transport()
        champs_requis = [
            'voyage_id', 'produit', 'poids_kg', 'temp_moyenne_c',
            'temp_max_tolere', 'delai_prevu_h', 'date_depart',
            'zone_origine', 'zone_dest', 'distance_km',
            'cout_fcfa_km', 'pct_perte_reel', 'statut'
        ]
        for champ in champs_requis:
            assert champ in evt, f"Champ manquant : {champ}"

    def test_gen_transport_critique_force(self):
        """Le scénario critique doit produire Mangue à 40°C avec perte > 0.4."""
        evt = gen_transport(scenario='critique')
        assert evt['produit'] == 'MANGUE'
        assert evt['temp_moyenne_c'] == 40.0
        assert evt['pct_perte_reel'] > 0.4, \
            f"Scénario critique : perte {evt['pct_perte_reel']} devrait être > 0.4"

    def test_gen_capteur_structure(self):
        """Le capteur généré doit avoir les coordonnées dans les limites du Sénégal."""
        evt = gen_capteur('VY99999', panne=False)
        if evt.get('latitude') is not None:
            assert 12.0 <= evt['latitude'] <= 15.5
        if evt.get('longitude') is not None:
            assert -17.5 <= evt['longitude'] <= -11.5

    def test_gen_transport_statut_en_cours(self):
        """Le statut par défaut doit être EN_COURS."""
        for _ in range(5):
            evt = gen_transport()
            assert evt['statut'] == 'EN_COURS'


# ══════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short', '--color=yes'])
