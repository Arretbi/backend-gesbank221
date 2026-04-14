import re
from datetime import datetime


MIN_MONTANT = 0.01
MAX_MONTANT = 1_000_000_000.00

MIN_SOLDE = -1_000_000_000.00
MAX_SOLDE = 1_000_000_000.00

MIN_DECOUVERT = 0.00
MAX_DECOUVERT = 1_000_000_000.00

MIN_TAUX = 0.00
MAX_TAUX = 100.00

DUREE_BLOCAGE_JOURS = 30

NAME_RE = re.compile(r"^[A-Za-zÀ-ÖØ-öø-ÿ' -]{2,80}$")
ACCOUNT_RE = re.compile(r"^\d{6,20}$")


def normaliser_texte(s: str) -> str:
    return " ".join((s or "").strip().split())


def valider_numero(raw: str):
    raw = (raw or "").strip()
    if not ACCOUNT_RE.match(raw):
        return None, "Numéro de compte invalide (6 à 20 chiffres requis)."
    return raw, None


def valider_titulaire(raw: str):
    raw = normaliser_texte(raw)
    if not raw:
        return None, "Nom du titulaire vide."
    if not NAME_RE.match(raw):
        return None, "Nom du titulaire invalide (2 à 80 caractères, lettres, espaces, apostrophes, tirets)."
    return raw, None


def valider_solde(valeur: float):
    if valeur < MIN_SOLDE:
        return None, f"Solde trop bas (min {MIN_SOLDE})."
    if valeur > MAX_SOLDE:
        return None, f"Solde trop élevé (max {MAX_SOLDE})."
    return valeur, None


def valider_montant(valeur: float):
    if valeur < MIN_MONTANT:
        return None, f"Montant trop petit (min {MIN_MONTANT})."
    if valeur > MAX_MONTANT:
        return None, f"Montant trop élevé (max {MAX_MONTANT})."
    return valeur, None


def valider_decouvert(valeur: float):
    if valeur < MIN_DECOUVERT:
        return None, f"Découvert négatif interdit (min {MIN_DECOUVERT})."
    if valeur > MAX_DECOUVERT:
        return None, f"Découvert trop élevé (max {MAX_DECOUVERT})."
    return valeur, None


def valider_taux(valeur: float):
    if valeur < MIN_TAUX:
        return None, f"Taux négatif interdit (min {MIN_TAUX})."
    if valeur > MAX_TAUX:
        return None, f"Taux trop élevé (max {MAX_TAUX})."
    return valeur, None


def valider_date(raw: str):
    try:
        dt = datetime.strptime(raw, "%d/%m/%Y %H:%M")
        return dt, None
    except ValueError:
        return None, "Format de date invalide. Utilisez JJ/MM/AAAA HH:MM."