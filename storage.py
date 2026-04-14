import os
import json
import psycopg2
import psycopg2.extras
from decimal import Decimal
from typing import Optional, List, Dict, Any
from threading import Lock
from datetime import datetime


class StorageError(Exception):
    pass


def _convert_decimals(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: _convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_decimals(v) for v in obj]
    return obj


class BanqueStorage:
    def __init__(self, db_url: str = None):
        if db_url:
            self._use_postgres = True
            self._conn = psycopg2.connect(db_url)
            self._init_db()
        else:
            self._use_postgres = False
            json_path = "comptes.json"
            self.json_path = json_path
            self._lock = Lock()
            self._init_file()

    def _init_db(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS comptes (
                    numero VARCHAR(20) PRIMARY KEY,
                    titulaire VARCHAR(80) NOT NULL,
                    solde DECIMAL(15,2) NOT NULL DEFAULT 0,
                    type VARCHAR(20) NOT NULL,
                    decouvert DECIMAL(15,2),
                    taux DECIMAL(5,4),
                    date_creation TIMESTAMP,
                    date_deblocage TIMESTAMP,
                    est_bloque BOOLEAN
                )
            """)
            self._conn.commit()

    def _init_file(self) -> None:
        import os
        if not os.path.exists(self.json_path):
            try:
                with open(self.json_path, "w", encoding="utf-8") as f:
                    json.dump({"comptes": []}, f, ensure_ascii=False, indent=2)
            except OSError as e:
                raise StorageError(f"Impossible de créer le fichier JSON: {e}")

    def _lire_donnees(self) -> dict:
        try:
            with open(self.json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if "comptes" not in data or not isinstance(data["comptes"], list):
                    return {"comptes": []}
                return data
        except (OSError, json.JSONDecodeError) as e:
            raise StorageError(f"Erreur lecture JSON: {e}")

    def _serialiser_compte(self, compte: dict) -> dict:
        c = compte.copy()
        for champ in ["date_creation", "date_deblocage"]:
            if champ in c and isinstance(c[champ], datetime):
                c[champ] = c[champ].isoformat()
        return c

    def _ecrire_donnees(self, data: dict) -> None:
        try:
            serialized = {"comptes": [self._serialiser_compte(c) for c in data["comptes"]]}
            with open(self.json_path, "w", encoding="utf-8") as f:
                json.dump(serialized, f, ensure_ascii=False, indent=2)
        except OSError as e:
            raise StorageError(f"Erreur écriture JSON: {e}")

    def ajouter(self, compte: dict) -> None:
        if self._use_postgres:
            with self._conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO comptes (numero, titulaire, solde, type, decouvert, taux, date_creation, date_deblocage, est_bloque)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    compte.get("numero"),
                    compte.get("titulaire"),
                    compte.get("solde", 0),
                    compte.get("type"),
                    compte.get("decouvert"),
                    compte.get("taux"),
                    compte.get("date_creation"),
                    compte.get("date_deblocage"),
                    compte.get("est_bloque")
                ))
                self._conn.commit()
        else:
            data = self._lire_donnees()
            for c in data["comptes"]:
                if c["numero"] == compte["numero"]:
                    raise StorageError("Ce numéro de compte existe déjà.")
            data["comptes"].append(compte)
            self._ecrire_donnees(data)

    def get(self, numero: str) -> Optional[dict]:
        if self._use_postgres:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM comptes WHERE numero = %s", (numero,))
                row = cur.fetchone()
                if row:
                    return _convert_decimals(dict(row))
                return None
        else:
            data = self._lire_donnees()
            for c in data["comptes"]:
                if c["numero"] == numero:
                    return c
            return None

    def maj(self, numero: str, champs: dict) -> Optional[dict]:
        if self._use_postgres:
            set_parts = []
            values = []
            for key, value in champs.items():
                set_parts.append(f"{key} = %s")
                values.append(value)
            values.append(numero)
            
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(f"UPDATE comptes SET {', '.join(set_parts)} WHERE numero = %s RETURNING *", values)
                row = cur.fetchone()
                self._conn.commit()
                return dict(row) if row else None
        else:
            data = self._lire_donnees()
            for i, c in enumerate(data["comptes"]):
                if c["numero"] == numero:
                    data["comptes"][i].update(champs)
                    self._ecrire_donnees(data)
                    return data["comptes"][i]
            return None

    def supprimer(self, numero: str) -> bool:
        if self._use_postgres:
            with self._conn.cursor() as cur:
                cur.execute("DELETE FROM comptes WHERE numero = %s", (numero,))
                deleted = cur.rowcount > 0
                self._conn.commit()
                return deleted
        else:
            data = self._lire_donnees()
            avant = len(data["comptes"])
            data["comptes"] = [c for c in data["comptes"] if c["numero"] != numero]
            if len(data["comptes"]) == avant:
                return False
            self._ecrire_donnees(data)
            return True

    def tous(self) -> List[dict]:
        if self._use_postgres:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM comptes ORDER BY titulaire")
                rows = cur.fetchall()
                return [_convert_decimals(dict(row)) for row in rows]
        else:
            data = self._lire_donnees()
            return sorted(data["comptes"], key=lambda x: x["titulaire"].lower())

    def tous_par_type(self, type_compte: str) -> List[dict]:
        if self._use_postgres:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM comptes WHERE type = %s ORDER BY titulaire", (type_compte,))
                rows = cur.fetchall()
                return [_convert_decimals(dict(row)) for row in rows]
        else:
            return [c for c in self.tous() if c["type"] == type_compte]


class JsonBanqueStorage:
    def __init__(self, json_path: str = "comptes.json"):
        self.json_path = json_path
        self._lock = Lock()
        self._init_file()

    def _init_file(self) -> None:
        import os
        if not os.path.exists(self.json_path):
            try:
                with open(self.json_path, "w", encoding="utf-8") as f:
                    json.dump({"comptes": []}, f, ensure_ascii=False, indent=2)
            except OSError as e:
                raise StorageError(f"Impossible de créer le fichier JSON: {e}")

    def _lire_donnees(self) -> dict:
        try:
            with open(self.json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if "comptes" not in data or not isinstance(data["comptes"], list):
                    return {"comptes": []}
                return data
        except (OSError, json.JSONDecodeError) as e:
            raise StorageError(f"Erreur lecture JSON: {e}")

    def _serialiser_compte(self, compte: dict) -> dict:
        c = compte.copy()
        for champ in ["date_creation", "date_deblocage"]:
            if champ in c and isinstance(c[champ], datetime):
                c[champ] = c[champ].isoformat()
        return c

    def _ecrire_donnees(self, data: dict) -> None:
        try:
            serialized = {"comptes": [self._serialiser_compte(c) for c in data["comptes"]]}
            with open(self.json_path, "w", encoding="utf-8") as f:
                json.dump(serialized, f, ensure_ascii=False, indent=2)
        except OSError as e:
            raise StorageError(f"Erreur écriture JSON: {e}")

    def ajouter(self, compte: dict) -> None:
        with self._lock:
            data = self._lire_donnees()
            for c in data["comptes"]:
                if c["numero"] == compte["numero"]:
                    raise StorageError("Ce numéro de compte existe déjà.")
            data["comptes"].append(compte)
            self._ecrire_donnees(data)

    def get(self, numero: str) -> Optional[dict]:
        with self._lock:
            data = self._lire_donnees()
            for c in data["comptes"]:
                if c["numero"] == numero:
                    return c
            return None

    def maj(self, numero: str, champs: dict) -> Optional[dict]:
        with self._lock:
            data = self._lire_donnees()
            for i, c in enumerate(data["comptes"]):
                if c["numero"] == numero:
                    data["comptes"][i].update(champs)
                    self._ecrire_donnees(data)
                    return data["comptes"][i]
            return None

    def supprimer(self, numero: str) -> bool:
        with self._lock:
            data = self._lire_donnees()
            avant = len(data["comptes"])
            data["comptes"] = [c for c in data["comptes"] if c["numero"] != numero]
            if len(data["comptes"]) == avant:
                return False
            self._ecrire_donnees(data)
            return True

    def tous(self) -> List[dict]:
        with self._lock:
            data = self._lire_donnees()
            return sorted(data["comptes"], key=lambda x: x["titulaire"].lower())

    def tous_par_type(self, type_compte: str) -> List[dict]:
        return [c for c in self.tous() if c["type"] == type_compte]