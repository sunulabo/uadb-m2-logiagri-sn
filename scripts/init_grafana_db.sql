CREATE TABLE IF NOT EXISTS logi_alertes (
    id SERIAL PRIMARY KEY,
    voyage_id VARCHAR(100),
    produit VARCHAR(50),
    zone_origine VARCHAR(100),
    zone_dest VARCHAR(100),
    temp_moyenne_c DOUBLE PRECISION,
    cout_total_fcfa DOUBLE PRECISION,
    risque_perte_score DOUBLE PRECISION,
    statut_alerte VARCHAR(50),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_logi_alertes_timestamp
    ON logi_alertes (timestamp);

CREATE INDEX IF NOT EXISTS idx_logi_alertes_statut
    ON logi_alertes (statut_alerte);
