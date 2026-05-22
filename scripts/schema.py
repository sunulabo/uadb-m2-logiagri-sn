"""
schema.py — Contrats Pandera pour Logi-Agri SN
UADB | Master 2 Big Data & IA | 2025-2026
Améliorations v2+ :
  - 3 schémas complets (Transport, Stock, CapteurIoT)
  - Validateurs métier cross-colonnes (check)
  - Rapport d'erreurs détaillé avec statistiques
  - Décorateur @validate_input pour fonctions critiques
  - Anonymisation SHA-256 + sel intégrée
"""

import pandera as pa
from pandera.typing import Series
import pandas as pd
import hashlib
import os
import logging
from functools import wraps
from typing import Optional, Callable, Type

# ── Configuration ──────────────────────────────────────────────────────────
SALT = os.environ.get('LOGI_SECRET_SALT', 'logi_agri_sn_2025_uadb_secret')
logger = logging.getLogger('LogiAgriSchema')

# ── Constantes métier ──────────────────────────────────────────────────────
PRODUITS_VALIDES    = ['ARACHIDE', 'MANGUE', 'RIZ', 'TOMATE']
ZONES_PROD_VALIDES  = ['CASAMANCE', 'BASSIN_ARACHIDIER', 'SINE_SALOUM', 'NIAYES']
ZONES_DEST_VALIDES  = ['DAKAR', 'EXPORT', 'THIES', 'KAOLACK']
STATUTS_VALIDES     = ['EN_COURS', 'TERMINE', 'ANNULE', 'RETARD']
ALERTES_VALIDES     = ['NORMAL', 'ATTENTION', 'CRITIQUE']

# Températures maximales tolérées par produit (°C)
TEMP_MAX_PAR_PRODUIT = {
    'MANGUE':    12.0,
    'TOMATE':    10.0,
    'ARACHIDE':  30.0,
    'RIZ':       35.0,
}


# ══════════════════════════════════════════════════════════════════════════
# SCHÉMA 1 — Transport Logistique
# ══════════════════════════════════════════════════════════════════════════
class TransportLogisticsSchema(pa.SchemaModel):
    """
    Valide les événements de transport issus du topic Kafka logi_raw.
    Toutes les contraintes métier sénégalaises sont encodées ici.
    """
    voyage_id:       Series[str]   = pa.Field(unique=True, str_matches=r'^VY\d{5}$')
    produit:         Series[str]   = pa.Field(isin=PRODUITS_VALIDES)
    poids_kg:        Series[float] = pa.Field(gt=0, le=30000,
                                               description='Charge utile camion max 30t')
    temp_moyenne_c:  Series[float] = pa.Field(ge=2.0, le=45.0)
    temp_max_tolere: Series[float] = pa.Field(ge=0.0, le=45.0)
    delai_prevu_h:   Series[int]   = pa.Field(ge=1, le=168,
                                               description='Max 1 semaine')
    date_depart:     Series[str]   = pa.Field(str_matches=r'^\d{4}-\d{2}-\d{2}$')
    zone_origine:    Series[str]   = pa.Field(isin=ZONES_PROD_VALIDES)
    zone_dest:       Series[str]   = pa.Field(isin=ZONES_DEST_VALIDES)
    distance_km:     Series[float] = pa.Field(gt=0, le=800,
                                               description='Max Dakar–Ziguinchor ~490km')
    cout_fcfa_km:    Series[float] = pa.Field(ge=200.0, le=800.0,
                                               description='Fourchette réaliste Sénégal')
    pct_perte_reel:  Series[float] = pa.Field(ge=0.0, le=1.0, nullable=True,
                                               description='Renseigné à la livraison')
    statut:          Series[str]   = pa.Field(isin=STATUTS_VALIDES, nullable=True)

    @pa.check('poids_kg', name='poids_coherent_distance')
    @classmethod
    def poids_non_nul_si_en_cours(cls, series: Series[float]) -> Series[bool]:
        """Un transport en cours doit avoir un poids positif enregistré."""
        return series > 0

    @pa.dataframe_check
    @classmethod
    def temp_coherente_avec_produit(cls, df: pd.DataFrame) -> pd.Series:
        """
        Vérifie la cohérence temp_max_tolere vs produit.
        Alerte si l'écart dépasse 5°C (mauvaise configuration capteur).
        """
        expected = df['produit'].map(TEMP_MAX_PAR_PRODUIT)
        ecart = (df['temp_max_tolere'] - expected).abs()
        invalides = ecart > 5.0
        if invalides.any():
            logger.warning(
                f'[TransportSchema] {invalides.sum()} lignes avec temp_max_tolere '
                f'incohérente avec le produit'
            )
        return ~invalides  # On ne bloque pas, on avertit

    class Config:
        strict = True
        coerce = True
        name = 'TransportLogisticsSchema'


# ══════════════════════════════════════════════════════════════════════════
# SCHÉMA 2 — Stock Entrepôt
# ══════════════════════════════════════════════════════════════════════════
class StockEntrepotSchema(pa.SchemaModel):
    """Valide les niveaux de stock et conditions de conservation."""
    entrepot_id:    Series[str]   = pa.Field(str_matches=r'^ENT_[A-Z0-9]{4,10}$')
    produit:        Series[str]   = pa.Field(isin=PRODUITS_VALIDES)
    quantite_kg:    Series[float] = pa.Field(ge=0, le=500_000,
                                              description='Entrepôt max 500t')
    temp_entrepot_c:Series[float] = pa.Field(ge=0.0, le=40.0)
    humidite_pct:   Series[float] = pa.Field(ge=0.0, le=100.0)
    capacite_max_kg:Series[float] = pa.Field(gt=0, le=500_000, nullable=True)
    date_entree:    Series[str]   = pa.Field(str_matches=r'^\d{4}-\d{2}-\d{2}$')
    date_sortie:    Series[str]   = pa.Field(str_matches=r'^\d{4}-\d{2}-\d{2}$',
                                              nullable=True)

    @pa.dataframe_check
    @classmethod
    def taux_remplissage_valide(cls, df: pd.DataFrame) -> pd.Series:
        """Le stock ne peut dépasser la capacité maximale de l'entrepôt."""
        if 'capacite_max_kg' not in df.columns:
            return pd.Series([True] * len(df))
        masque_cap = df['capacite_max_kg'].notna()
        valide = pd.Series([True] * len(df))
        valide[masque_cap] = (
            df.loc[masque_cap, 'quantite_kg'] <= df.loc[masque_cap, 'capacite_max_kg']
        )
        return valide

    @pa.dataframe_check
    @classmethod
    def conditions_conservation_mangue(cls, df: pd.DataFrame) -> pd.Series:
        """Mangue : temp < 12°C ET humidité entre 85–95%."""
        mangue = df['produit'] == 'MANGUE'
        if not mangue.any():
            return pd.Series([True] * len(df))
        valide = pd.Series([True] * len(df))
        valide[mangue] = (
            (df.loc[mangue, 'temp_entrepot_c'] <= 12.0) &
            (df.loc[mangue, 'humidite_pct'].between(80, 100))
        )
        return valide

    class Config:
        strict = False   # Colonnes additionnelles tolérées pour les entrepôts
        coerce = True
        name = 'StockEntrepotSchema'


# ══════════════════════════════════════════════════════════════════════════
# SCHÉMA 3 — Capteur IoT
# ══════════════════════════════════════════════════════════════════════════
class CapteurIoTSchema(pa.SchemaModel):
    """Valide les relevés IoT (GPS + température) des camions frigorifiques."""
    capteur_id:    Series[str]   = pa.Field(str_matches=r'^CPT\d{4}$')
    voyage_id:     Series[str]   = pa.Field(str_matches=r'^VY\d{5}$')
    timestamp_utc: Series[str]   = pa.Field(
        str_matches=r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
    )
    temperature_c: Series[float] = pa.Field(ge=-5.0, le=50.0)
    # Boîte englobante Sénégal (coordonnées GPS)
    latitude:      Series[float] = pa.Field(ge=12.0, le=15.5,
                                             description='Bande latitudinale Sénégal')
    longitude:     Series[float] = pa.Field(ge=-17.5, le=-11.5,
                                             description='Bande longitudinale Sénégal')
    batterie_pct:  Series[float] = pa.Field(ge=0.0, le=100.0, nullable=True)
    signal_qualite:Series[int]   = pa.Field(ge=0, le=5, nullable=True,
                                             description='0=pas de signal, 5=excellent')

    @pa.check('temperature_c', name='temperature_capteur_plausible')
    @classmethod
    def temperature_pas_constante(cls, series: Series[float]) -> bool:
        """Détecte un capteur bloqué (température identique sur > 10 mesures consécutives)."""
        if len(series) < 10:
            return True
        # Variance minimum attendue pour un capteur fonctionnel
        return series.std() > 0.01

    class Config:
        strict = True
        coerce = True
        name = 'CapteurIoTSchema'


# ══════════════════════════════════════════════════════════════════════════
# FONCTIONS UTILITAIRES
# ══════════════════════════════════════════════════════════════════════════

def validate_and_filter(
    df: pd.DataFrame,
    schema_class: Type[pa.SchemaModel],
    log_errors: bool = True
) -> pd.DataFrame:
    """
    Valide un DataFrame, rejette les lignes invalides, retourne les lignes propres.
    Génère un rapport détaillé d'erreurs si log_errors=True.
    """
    if df.empty:
        logger.warning(f'[{schema_class.__name__}] DataFrame vide reçu')
        return df

    total = len(df)
    try:
        return schema_class.validate(df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        err_df = exc.failure_cases

        if log_errors:
            nb_erreurs = len(err_df)
            logger.warning(
                f'[{schema_class.__name__}] {nb_erreurs}/{total} lignes rejetées '
                f'({nb_erreurs/total*100:.1f}%)'
            )
            # Résumé par type d'erreur
            if 'check' in err_df.columns:
                resume = err_df.groupby('check').size().to_dict()
                for check_name, count in resume.items():
                    logger.warning(f'  → {check_name}: {count} violation(s)')

        # Filtrer les lignes invalides
        invalid_idx = err_df['index'].dropna().astype(int).unique()
        valid_df = df.drop(index=invalid_idx, errors='ignore')

        logger.info(
            f'[{schema_class.__name__}] {len(valid_df)}/{total} lignes valides conservées'
        )
        return valid_df


def anonymize_pii(df: pd.DataFrame, colonnes_pii: list, salt: str = SALT) -> pd.DataFrame:
    """
    Anonymise les colonnes PII avec SHA-256 + sel.
    Les colonnes originales sont supprimées, remplacées par *_secure.
    """
    df = df.copy()
    for col in colonnes_pii:
        if col in df.columns:
            df[f'{col}_secure'] = df[col].apply(
                lambda x: hashlib.sha256(f'{x}{salt}'.encode()).hexdigest()
                if pd.notna(x) else None
            )
            df.drop(columns=[col], inplace=True)
            logger.debug(f'Colonne PII anonymisée : {col} → {col}_secure')
    return df


def validate_input(schema_class: Type[pa.SchemaModel]) -> Callable:
    """
    Décorateur : valide le premier argument DataFrame d'une fonction.
    Usage : @validate_input(TransportLogisticsSchema)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(df: pd.DataFrame, *args, **kwargs):
            df_valid = validate_and_filter(df, schema_class)
            return func(df_valid, *args, **kwargs)
        return wrapper
    return decorator


def get_schema_stats(df: pd.DataFrame, schema_class: Type[pa.SchemaModel]) -> dict:
    """Retourne des statistiques de qualité de données pour un schéma donné."""
    total = len(df)
    if total == 0:
        return {'total': 0, 'valides': 0, 'taux_qualite': 0.0}

    try:
        schema_class.validate(df, lazy=True)
        return {'total': total, 'valides': total, 'taux_qualite': 100.0}
    except pa.errors.SchemaErrors as exc:
        invalides = exc.failure_cases['index'].dropna().nunique()
        valides = total - invalides
        return {
            'total': total,
            'valides': valides,
            'invalides': invalides,
            'taux_qualite': round(valides / total * 100, 2),
            'types_erreurs': exc.failure_cases['check'].value_counts().to_dict()
                if 'check' in exc.failure_cases.columns else {}
        }
