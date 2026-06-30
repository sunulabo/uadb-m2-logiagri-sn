import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
import plotly.express as px
import time

# --- Configuration de la page ---
st.set_page_config(page_title="Logi-Agri SN Dashboard", layout="wide")
st.title("🚜 Dashboard Temps Réel Logi-Agri SN")
st.markdown("Ce dashboard consomme directement le topic Kafka **`logi_alertes`** généré par Spark Streaming.")

# --- Configuration Kafka ---
KAFKA_BROKER = 'localhost:29092'
TOPIC_ALERTES = 'logi_alertes'

# Initialisation du consommateur Kafka (mis en cache pour éviter la recréation à chaque rafraîchissement)
@st.cache_resource
def init_kafka_consumer():
    try:
        consumer = KafkaConsumer(
            TOPIC_ALERTES,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            consumer_timeout_ms=500  # Ne bloque pas trop longtemps
        )
        return consumer
    except Exception as e:
        st.error(f"Erreur de connexion Kafka: {e}")
        return None

# Stockage des données dans la session (persiste entre les rafraîchissements)
if 'data' not in st.session_state:
    st.session_state.data = []

consumer = init_kafka_consumer()

# --- Barre latérale ---
st.sidebar.header("Paramètres")
refresh_rate = st.sidebar.slider("Taux de rafraîchissement (secondes)", 1, 10, 2)
if st.sidebar.button("Vider les données historiques"):
    st.session_state.data = []

# --- Fonction de lecture ---
def fetch_new_messages():
    if consumer is None:
        return
    
    # Utilisation de poll() qui est "thread-safe" par rapport au cycle de rechargement Streamlit
    records = consumer.poll(timeout_ms=500)
    for tp, messages in records.items():
        for msg in messages:
            st.session_state.data.append(msg.value)
            
            # On garde uniquement les 200 derniers événements
            if len(st.session_state.data) > 200:
                st.session_state.data.pop(0)

fetch_new_messages()

# --- Affichage du Dashboard ---
placeholder = st.empty()

with placeholder.container():
    if not st.session_state.data:
        st.info("⏳ En attente de données depuis Kafka... Assure-toi que ton simulateur Python ET ton script Spark tournent !")
    else:
        df = pd.DataFrame(st.session_state.data)
        
        # --- 1. KPIs ---
        st.markdown("### 📊 Indicateurs Clés")
        col1, col2, col3, col4 = st.columns(4)
        
        col1.metric("Transports traqués (récents)", len(df))
        
        nb_critiques = len(df[df['statut_alerte'] == 'CRITIQUE']) if 'statut_alerte' in df.columns else 0
        col2.metric("Alertes Critiques", nb_critiques, delta_color="inverse")
        
        cout_total = df['cout_total_fcfa'].sum() if 'cout_total_fcfa' in df.columns else 0
        col3.metric("Coût Logistique Cumulé", f"{cout_total:,.0f} FCFA")
        
        tonnes = df['poids_kg'].sum() / 1000 if 'poids_kg' in df.columns else 0
        col4.metric("Volume Transporté", f"{tonnes:.1f} T")

        st.markdown("---")

        # --- 2. Graphiques ---
        g1, g2 = st.columns(2)
        
        with g1:
            if 'statut_alerte' in df.columns:
                # Répartition des statuts
                fig_status = px.pie(
                    df, 
                    names='statut_alerte', 
                    title="Répartition des Statuts d'Alerte", 
                    color='statut_alerte',
                    color_discrete_map={'NORMAL': '#2ecc71', 'ATTENTION': '#f39c12', 'CRITIQUE': '#e74c3c'}
                )
                st.plotly_chart(fig_status, use_container_width=True)

        with g2:
            if 'produit' in df.columns and 'cout_total_fcfa' in df.columns:
                # Coût par produit
                cout_par_produit = df.groupby('produit')['cout_total_fcfa'].sum().reset_index()
                fig_cout = px.bar(
                    cout_par_produit, 
                    x='produit', 
                    y='cout_total_fcfa', 
                    title="Coût Logistique par Produit (FCFA)",
                    color='produit'
                )
                st.plotly_chart(fig_cout, use_container_width=True)

        # --- 3. Tableau détaillé ---
        st.markdown("### 📋 Derniers Mouvements")
        # Colonnes intéressantes à afficher en priorité
        display_cols = ['voyage_id', 'produit', 'zone_origine', 'zone_dest', 'temp_moyenne_c', 'risque_perte_score', 'cout_total_fcfa', 'statut_alerte']
        existing_cols = [c for c in display_cols if c in df.columns]
        
        # Affichage du dataframe inversé (les plus récents en haut)
        st.dataframe(df[existing_cols].iloc[::-1].head(15), use_container_width=True)

# Boucle de rafraîchissement
time.sleep(refresh_rate)
st.rerun()
