import bcrypt
import psycopg2
import sys
import os

# Pfad-Setup: Fügt das Root-Verzeichnis hinzu, damit Imports funktionieren
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import get_db_connection
from config.logging_config import setup_logging

logger = setup_logging("auth")

def verify_user(username, plain_password):
    """
    Prüft Benutzername und Passwort gegen die Datenbank.
    Gibt (True, role) zurück bei Erfolg, sonst (False, None).
    """
    conn = get_db_connection()
    if not conn:
        logger.error("Auth: Keine DB-Verbindung")
        return False, None

    try:
        with conn.cursor() as cur:
            # Wir holen Hash und Rolle aus der DB
            cur.execute("SELECT id, password_hash, role FROM users WHERE username = %s", (username,))
            user = cur.fetchone()
            
            if user:
                user_id, stored_hash, role = user
                
                # WICHTIG: bcrypt vergleicht Bytes, keine Strings!
                # plain_password.encode('utf-8') -> macht Bytes aus der Eingabe
                # stored_hash.encode('utf-8') -> macht Bytes aus dem DB-String
                if bcrypt.checkpw(plain_password.encode('utf-8'), stored_hash.encode('utf-8')):
                    
                    # Login erfolgreich -> Zeitstempel updaten (Optional, fail-safe)
                    try:
                        cur.execute("UPDATE users SET last_login = NOW() WHERE id = %s", (user_id,))
                        conn.commit()
                    except Exception as e:
                        logger.warning(f"Konnte last_login nicht updaten (ignoriert): {e}")
                    
                    return True, role
                else:
                    logger.warning(f"Login fehlgeschlagen für '{username}': Falsches Passwort")
            else:
                logger.warning(f"Login fehlgeschlagen: Benutzer '{username}' nicht gefunden")
                    
    except Exception as e:
        logger.error(f"Auth System Error: {e}")
    finally:
        conn.close()
        
    return False, None

def create_user(username, plain_password, role='admin'):
    """
    Erstellt einen neuen Benutzer (Hasht das Passwort).
    Rückgabe: True bei Erfolg, False bei Fehler.
    """
    conn = get_db_connection()
    if not conn:
        return False

    # 1. Hashing
    try:
        hashed = bcrypt.hashpw(plain_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    except Exception as e:
        logger.error(f"Hashing Error: {e}")
        return False
    
    # 2. Speichern
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s)",
                (username, hashed, role)
            )
        conn.commit()
        logger.info(f"Benutzer '{username}' erstellt (Rolle: {role})")
        return True
        
    except psycopg2.IntegrityError:
        conn.rollback()
        logger.warning(f"Create User fehlgeschlagen: '{username}' existiert bereits.")
        print(f"❌ Fehler: Benutzer '{username}' existiert bereits.")
        return False
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Create User DB Error: {e}")
        print(f"❌ Datenbank-Fehler: {e}")
        return False
    finally:
        conn.close()