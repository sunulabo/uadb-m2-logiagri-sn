-- hive_setup.sql — Tables et vues Hive Logi-Agri SN
CREATE DATABASE IF NOT EXISTS logi_agri
  COMMENT 'Logistique agricole Sénégal — UADB 2025';

USE logi_agri;

CREATE TABLE IF NOT EXISTS flux_temps_reel (
  entity_id_secure STRING, voyage_id STRING, produit STRING,
  poids_kg FLOAT, temp_moyenne_c FLOAT, delai_prevu_h INT,
  zone_origine STRING, zone_dest STRING, distance_km FLOAT,
  cout_total_fcfa FLOAT, cout_par_tonne_fcfa FLOAT,
  risque_perte_score FLOAT, pct_perte_reel FLOAT,
  statut_alerte STRING, statut STRING, ingestion_ts TIMESTAMP
)
PARTITIONED BY (date_depart STRING, produit_part STRING)
STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY');

CREATE TABLE IF NOT EXISTS historique_transport (
  voyage_id STRING, produit STRING, poids_kg FLOAT,
  temp_moyenne_c FLOAT, temp_max_tolere FLOAT,
  delai_prevu_h INT, distance_km FLOAT,
  cout_total_fcfa FLOAT, cout_par_tonne_fcfa FLOAT,
  pct_perte_reel FLOAT,
  date_depart DATE
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS kpi_logistique (
  produit STRING, zone_origine STRING, zone_dest STRING,
  nb_voyages INT, cout_moy_tonne_fcfa FLOAT, taux_perte_moyen_pct FLOAT,
  tonnage_total FLOAT, delai_moyen_h FLOAT, date_calcul DATE
) STORED AS ORC;

CREATE OR REPLACE VIEW vue_goulots_entrepots AS
SELECT zone_dest AS entrepot, produit,
  COUNT(*) AS nb_voyages,
  AVG(delai_prevu_h) AS delai_moyen_h,
  SUM(COALESCE(poids_kg,0)) / 1000.0 AS tonnage_t,
  AVG(COALESCE(risque_perte_score,0)) AS risque_moyen,
  CASE WHEN AVG(delai_prevu_h) > 48 THEN 'GOULOT_CRITIQUE'
       WHEN AVG(delai_prevu_h) > 24 THEN 'GOULOT_MODERE'
       ELSE 'NORMAL' END AS statut_goulot
FROM flux_temps_reel
WHERE statut = 'EN_COURS' AND date_depart = CURRENT_DATE()
GROUP BY zone_dest, produit;

CREATE OR REPLACE VIEW vue_pertes_par_region AS
SELECT zone_origine, produit,
  SUM(COALESCE(poids_kg,0) * COALESCE(risque_perte_score,0))/1000.0 AS tonnes_perdues_est,
  SUM(COALESCE(poids_kg,0)) / 1000.0 AS tonnage_total_t,
  AVG(COALESCE(risque_perte_score,0)) * 100 AS taux_perte_pct,
  SUM(COALESCE(cout_total_fcfa,0) * COALESCE(risque_perte_score,0)) AS cout_perte_fcfa
FROM flux_temps_reel
WHERE date_depart >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY zone_origine, produit
ORDER BY tonnes_perdues_est DESC;