from typing import List, Optional
from datetime import datetime, timedelta
from storage import BanqueStorage, StorageError
from validation import (
    valider_numero, valider_titulaire, valider_solde, valider_montant,
    valider_decouvert, valider_taux, valider_date, DUREE_BLOCAGE_JOURS
)


class ServiceError(Exception):
    pass


class BanqueService:
    def __init__(self, storage: BanqueStorage):
        self.storage = storage

    def _est_bloque(self, compte: dict) -> bool:
        if compte["type"] != "bloque":
            return False
        return datetime.now() < compte["date_deblocage"]

    def _peut_retirer(self, compte: dict, montant: float) -> tuple[bool, str]:
        if self._est_bloque(compte):
            return False, "Compte bloqué : retrait impossible avant la date de déblocage."
        if compte["type"] == "courant":
            if compte["solde"] - montant < -compte["decouvert"]:
                return False, "Dépassement du découvert autorisé."
        else:
            if compte["solde"] - montant < 0:
                return False, "Solde insuffisant."
        return True, ""

    def creer_compte_courant(self, numero: str, titulaire: str, solde: float, decouvert: float) -> dict:
        num, err_n = valider_numero(numero)
        if num is None:
            raise ServiceError(err_n)
        tit, err_t = valider_titulaire(titulaire)
        if tit is None:
            raise ServiceError(err_t)
        s, err_s = valider_solde(solde)
        if s is None:
            raise ServiceError(err_s)
        d, err_d = valider_decouvert(decouvert)
        if d is None:
            raise ServiceError(err_d)
        if self.storage.get(num) is not None:
            raise ServiceError("Ce numéro de compte existe déjà.")
        compte = {
            "numero": num, "titulaire": tit, "solde": s,
            "type": "courant", "decouvert": d
        }
        try:
            self.storage.ajouter(compte)
        except StorageError as e:
            raise ServiceError(str(e))
        return compte

    def creer_compte_epargne(self, numero: str, titulaire: str, solde: float, taux: float) -> dict:
        num, err_n = valider_numero(numero)
        if num is None:
            raise ServiceError(err_n)
        tit, err_t = valider_titulaire(titulaire)
        if tit is None:
            raise ServiceError(err_t)
        s, err_s = valider_solde(solde)
        if s is None:
            raise ServiceError(err_s)
        tx, err_tx = valider_taux(taux)
        if tx is None:
            raise ServiceError(err_tx)
        if self.storage.get(num) is not None:
            raise ServiceError("Ce numéro de compte existe déjà.")
        compte = {
            "numero": num, "titulaire": tit, "solde": s,
            "type": "epargne", "taux": tx
        }
        try:
            self.storage.ajouter(compte)
        except StorageError as e:
            raise ServiceError(str(e))
        return compte

    def creer_compte_bloque(self, numero: str, titulaire: str, solde: float, date_creation_str: str) -> dict:
        num, err_n = valider_numero(numero)
        if num is None:
            raise ServiceError(err_n)
        tit, err_t = valider_titulaire(titulaire)
        if tit is None:
            raise ServiceError(err_t)
        s, err_s = valider_solde(solde)
        if s is None:
            raise ServiceError(err_s)
        date_creation, err_d = valider_date(date_creation_str)
        if date_creation is None:
            raise ServiceError(err_d)
        if self.storage.get(num) is not None:
            raise ServiceError("Ce numéro de compte existe déjà.")
        date_deblocage = date_creation + timedelta(days=DUREE_BLOCAGE_JOURS)
        compte = {
            "numero": num, "titulaire": tit, "solde": s,
            "type": "bloque", "date_creation": date_creation, "date_deblocage": date_deblocage
        }
        try:
            self.storage.ajouter(compte)
        except StorageError as e:
            raise ServiceError(str(e))
        return compte

    def deposer(self, numero: str, montant: float) -> dict:
        m, err_m = valider_montant(montant)
        if m is None:
            raise ServiceError(err_m)
        compte = self.storage.get(numero)
        if compte is None:
            raise ServiceError("Compte introuvable.")
        compte["solde"] += m
        try:
            self.storage.maj(numero, {"solde": compte["solde"]})
        except StorageError as e:
            raise ServiceError(str(e))
        return compte

    def retirer(self, numero: str, montant: float) -> dict:
        m, err_m = valider_montant(montant)
        if m is None:
            raise ServiceError(err_m)
        compte = self.storage.get(numero)
        if compte is None:
            raise ServiceError("Compte introuvable.")
        peut, raison = self._peut_retirer(compte, m)
        if not peut:
            raise ServiceError(raison)
        compte["solde"] -= m
        try:
            self.storage.maj(numero, {"solde": compte["solde"]})
        except StorageError as e:
            raise ServiceError(str(e))
        return compte

    def appliquer_interets(self) -> dict:
        try:
            comptes = self.storage.tous()
        except StorageError as e:
            raise ServiceError(str(e))
        total_gains = 0.0
        comptes_maj = 0
        for c in comptes:
            if c["type"] == "epargne":
                gain = c["solde"] * (c["taux"] / 100)
                c["solde"] += gain
                total_gains += gain
                comptes_maj += 1
                self.storage.maj(c["numero"], {"solde": c["solde"]})
        return {"total_gains": total_gains, "comptes_mis_a_jour": comptes_maj}

    def supprimer(self, numero: str) -> None:
        if self.storage.get(numero) is None:
            raise ServiceError("Compte introuvable.")
        try:
            ok = self.storage.supprimer(numero)
            if not ok:
                raise ServiceError("Compte introuvable.")
        except StorageError as e:
            raise ServiceError(str(e))

    def modifier(self, numero: str, titulaire: Optional[str] = None,
                 decouvert: Optional[float] = None, taux: Optional[float] = None) -> dict:
        compte = self.storage.get(numero)
        if compte is None:
            raise ServiceError("Compte introuvable.")
        champs = {}
        if titulaire is not None:
            t, err_t = valider_titulaire(titulaire)
            if t is None:
                raise ServiceError(err_t)
            champs["titulaire"] = t
        if decouvert is not None:
            if compte["type"] != "courant":
                raise ServiceError("Le découvert ne concerne que les comptes courants.")
            d, err_d = valider_decouvert(decouvert)
            if d is None:
                raise ServiceError(err_d)
            champs["decouvert"] = d
        if taux is not None:
            if compte["type"] != "epargne":
                raise ServiceError("Le taux ne concerne que les comptes épargne.")
            tx, err_tx = valider_taux(taux)
            if tx is None:
                raise ServiceError(err_tx)
            champs["taux"] = tx
        if not champs:
            return compte
        try:
            updated = self.storage.maj(numero, champs)
        except StorageError as e:
            raise ServiceError(str(e))
        return updated

    def get_compte(self, numero: str) -> Optional[dict]:
        compte = self.storage.get(numero)
        if compte:
            compte["est_bloque"] = self._est_bloque(compte)
        return compte

    def tous(self) -> List[dict]:
        try:
            comptes = self.storage.tous()
            for c in comptes:
                c["est_bloque"] = self._est_bloque(c)
            return comptes
        except StorageError as e:
            raise ServiceError(str(e))

    def tous_par_type(self, type_compte: str) -> List[dict]:
        comptes = self.storage.tous_par_type(type_compte)
        for c in comptes:
            c["est_bloque"] = self._est_bloque(c)
        return comptes