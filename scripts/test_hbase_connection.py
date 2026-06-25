# test_hbase_connection.py — Diagnostic connexion HBase Thrift
import happybase

for transport in ['buffered', 'framed']:
    for protocol in ['binary', 'compact']:
        try:
            conn = happybase.Connection(
                'localhost', port=9090,
                timeout=5000,
                transport=transport,
                protocol=protocol,
            )
            conn.open()
            tables = conn.tables()
            print(f"✅ SUCCÈS : transport={transport}, protocol={protocol} → tables={tables}")
            conn.close()
        except Exception as e:
            print(f"❌ ÉCHEC : transport={transport}, protocol={protocol} → {e}")
