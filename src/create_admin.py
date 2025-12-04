import sys
import os
import getpass
import psycopg2

# 1. Pfad korrigieren
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

# 2. Windows Encoding Fix
if sys.platform == "win32":
    try:
        if hasattr(sys.stdin, 'reconfigure'):
            sys.stdin.reconfigure(encoding='utf-8')
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# 3. Import bcrypt Check
try:
    import bcrypt
except ImportError:
    print("❌ KRITISCHER FEHLER: Modul 'bcrypt' fehlt!")
    print("👉 Bitte installiere es mit: pip install bcrypt")
    sys.exit(1)

from src.database import get_db_connection
from config.settings import DB_CONFIG

def check_and_init_db():
    print(f"🔍 Prüfe Datenbank-Verbindung zu: {DB_CONFIG.get('host')}...")
    conn = get_db_connection()
    if not conn:
        print("❌ VERBINDUNGSFEHLER: Konnte keine Verbindung zur DB herstellen.")
        return False

    print("✅ Verbindung erfolgreich.")

    try:
        with conn.cursor() as cur:
            # Wir prüfen, ob die Tabelle existiert und ob sie die richtigen Spalten hat
            print("🔨 Prüfe Tabelle 'users'...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(50) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    role VARCHAR(20) DEFAULT 'admin',
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    last_login TIMESTAMPTZ
                );
            """)
        conn.commit()
        print("✅ Tabelle 'users' ist bereit.")
        return True
    except Exception as e:
        print(f"❌ FEHLER beim Tabellen-Setup: {e}")
        return False
    finally:
        conn.close()

def create_user_debug(username, password, role):
    """
    Erstellt Benutzer mit ausführlichem Debug-Log direkt im Skript.
    """
    conn = get_db_connection()
    if not conn:
        print("❌ FEHLER: DB Verbindung konnte nicht geöffnet werden.")
        return False

    try:
        # Hashing
        print("🔐 Hashe Passwort mit bcrypt...")
        hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        with conn.cursor() as cur:
            print(f"📝 Sende INSERT an Datenbank für '{username}'...")
            cur.execute(
                "INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s)",
                (username, hashed, role)
            )
            print("✅ INSERT ausgeführt.")
            
        conn.commit()
        print("✅ COMMIT erfolgreich.")
        return True

    except psycopg2.IntegrityError as e:
        conn.rollback()
        print("\n❌ INTEGRITY ERROR (Datenbank-Konflikt):")
        print(f"   Meldung: {e}")
        print("   -> Der Benutzername existiert bereits (Unique Constraint).")
        return False
        
    except psycopg2.Error as e:
        conn.rollback()
        print("\n❌ SQL FEHLER:")
        print(f"   Code: {e.pgcode}")
        print(f"   Meldung: {e}")
        return False
        
    except Exception as e:
        print(f"\n❌ UNBEKANNTER FEHLER: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        conn.close()

def main():
    print("\n--- 🛠️ Admin User Setup & DEBUG Modus ---\n")
    
    if not check_and_init_db():
        return

    print("\n--- Benutzer Daten eingeben ---")
    username = input("Benutzername: ").strip()
    if not username: return
        
    password = getpass.getpass("Passwort: ")
    if not password: return

    role = input("Rolle (default: admin): ").strip() or "admin"
    
    print(f"\n--- Start Vorgang für '{username}' ---")
    if create_user_debug(username, password, role):
        print(f"\n✅ ERFOLG: Benutzer '{username}' wurde angelegt!")
        print("🚀 Login möglich via: 'streamlit run dashboard.py'")
    else:
        print("\n❌ FEHLGESCHLAGEN (Siehe Fehlermeldung oben).")

if __name__ == "__main__":
    main()