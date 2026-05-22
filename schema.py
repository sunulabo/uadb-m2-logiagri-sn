# schema.py — Contrats Pandera pour Logi-Agri SN
import pandera as pa
from pandera.typing import Series
import pandas as pd, logging

logger = logging.getLogger('LogiAgriSchema')

class TransportLogisticsSchema(pa.SchemaModel):
    voyage_id: Series[str] = pa.Field(unique=True)
    produit: Series[str] = pa.Field(isin=['ARACHIDE','MANGUE','RIZ','TOMATE'])
    poids_kg: Series[float] = pa.Field(gt=0, le=30000)
    temp_moyenne_c: Series[float] = pa.Field(ge=2.0, le=45.0)
    delai_prevu_h: Series[int] = pa.Field(ge=1, le=168)
    date_depart: Series[str] = pa.Field(str_matches=r'^\d{4}-\d{2}-\d{2}$')
    zone_origine: Series[str] = pa.Field(isin=['CASAMANCE','BASSIN_ARACHIDIER',
        'SINE_SALOUM','NIAYES'])
    zone_dest: Series[str] = pa.Field(isin=['DAKAR','EXPORT','THIES','KAOLACK'])
    distance_km: Series[float] = pa.Field(gt=0, le=800)
    class Config:
        strict = True
        coerce = True

class StockEntrepotSchema(pa.SchemaModel):
    entrepot_id: Series[str] = pa.Field()
    produit: Series[str] = pa.Field(isin=['ARACHIDE','MANGUE','RIZ','TOMATE'])
    quantite_kg: Series[float] = pa.Field(ge=0)
    temp_entrepot_c: Series[float] = pa.Field(ge=0.0, le=40.0)
    humidite_pct: Series[float] = pa.Field(ge=0.0, le=100.0)
    date_entree: Series[str] = pa.Field(str_matches=r'^\d{4}-\d{2}-\d{2}$')
    class Config:
        strict = True
        coerce = True

class CapteurIoTSchema(pa.SchemaModel):
    capteur_id: Series[str] = pa.Field()
    voyage_id: Series[str] = pa.Field()
    timestamp_utc: Series[str] = pa.Field(str_matches=r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$')
    temperature_c: Series[float] = pa.Field(ge=-5.0, le=50.0)
    latitude: Series[float] = pa.Field(ge=12.0, le=15.5)
    longitude: Series[float] = pa.Field(ge=-17.5, le=-11.5)
    class Config:
        strict = True
        coerce = True

def validate_and_filter(df, schema_class):
    try:
        return schema_class.validate(df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        err = exc.failure_cases
        logger.warning(f'[{schema_class.__name__}] {len(err)} erreur(s) rejetées')
        valid_idx = df.index.difference(err['index'].dropna().astype(int))
        return df.loc[valid_idx]