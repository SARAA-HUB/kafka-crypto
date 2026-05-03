import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import time

st.set_page_config(
    page_title="Crypto Streaming",
    page_icon="⚡",
    layout="wide"
)
st.title("⚡ Crypto Streaming — Temps Réel")

def load_data():
    conn = psycopg2.connect(
        host="localhost", port=5433,
        database="crypto_db",
        user="crypto", password="crypto"
    )
    df = pd.read_sql("""
        SELECT * FROM crypto_streaming
        ORDER BY date DESC
        LIMIT 500
    """, conn)
    conn.close()
    return df

placeholder = st.empty()

while True:
    df = load_data()

    with placeholder.container():
        st.caption(f"⏱️ Mis à jour : {pd.Timestamp.now().strftime('%H:%M:%S')} — {len(df)} messages reçus")

        # ── Métriques ──
        latest = df.groupby('coin').first().reset_index()
        cols = st.columns(4)
        for i, (coin, label) in enumerate([
            ('bitcoin', 'Bitcoin'),
            ('ethereum', 'Ethereum'),
            ('solana', 'Solana'),
            ('binancecoin', 'BNB')
        ]):
            row = latest[latest['coin'] == coin]
            if not row.empty:
                cols[i].metric(
                    label,
                    f"${row['prix_usd'].values[0]:,.2f}",
                    f"{row['variation_24h'].values[0]}%"
                )

        # ── Graphique évolution ──
        st.subheader("📈 Prix en temps réel")
        fig = px.line(
            df.sort_values('date'),
            x='date', y='prix_usd',
            color='coin',
            title="Prix USD — streaming Kafka",
            log_y=True
        )
        st.plotly_chart(fig, use_container_width=True)

        # ── Variation 24h ──
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("📉 Variation 24h")
            fig2 = px.bar(
                latest, x='coin', y='variation_24h',
                color='variation_24h',
                color_continuous_scale='RdYlGn'
            )
            st.plotly_chart(fig2, use_container_width=True)

        with col2:
            st.subheader("📋 Derniers messages Kafka")
            st.dataframe(
                df[['coin','prix_usd','variation_24h','date']].head(20),
                use_container_width=True
            )

    time.sleep(10)