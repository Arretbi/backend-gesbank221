from pydantic import BaseModel, Field
from typing import Optional, Literal
from datetime import datetime


class CompteBase(BaseModel):
    numero: str = Field(..., description="Numéro de compte (6 à 20 chiffres)", example="12345678")
    titulaire: str = Field(..., description="Nom du titulaire", example="Jean Dupont")
    solde: float = Field(..., description="Solde du compte", example=1000.50)


class CompteCourant(CompteBase):
    type: Literal["courant"] = "courant"
    decouvert: float = Field(..., description="Découvert autorisé", example=500.00)


class CompteEpargne(CompteBase):
    type: Literal["epargne"] = "epargne"
    taux: float = Field(..., description="Taux d'intérêt annuel (%)", example=2.5)


class CompteBloque(CompteBase):
    type: Literal["bloque"] = "bloque"
    date_creation: datetime = Field(..., description="Date de création")
    date_deblocage: datetime = Field(..., description="Date de déblocage")


class CompteCreateCourant(BaseModel):
    numero: str = Field(..., example="12345678")
    titulaire: str = Field(..., example="Jean Dupont")
    solde: float = Field(0.0, example=1000.50)
    type: Literal["courant"] = "courant"
    decouvert: float = Field(..., example=500.00)


class CompteCreateEpargne(BaseModel):
    numero: str = Field(..., example="87654321")
    titulaire: str = Field(..., example="Marie Martin")
    solde: float = Field(0.0, example=5000.00)
    type: Literal["epargne"] = "epargne"
    taux: float = Field(..., example=2.5)


class CompteCreateBloque(BaseModel):
    numero: str = Field(..., example="11223344")
    titulaire: str = Field(..., example="Pierre Durand")
    solde: float = Field(0.0, example=10000.00)
    type: Literal["bloque"] = "bloque"
    date_creation: datetime = Field(..., description="JJ/MM/AAAA HH:MM", example="2026-02-07T14:30:00")


class CompteUpdate(BaseModel):
    titulaire: Optional[str] = Field(None, description="Nouveau nom du titulaire")
    decouvert: Optional[float] = Field(None, description="Nouveau découvert (courant uniquement)")
    taux: Optional[float] = Field(None, description="Nouveau taux (épargne uniquement)")


class DepotRetrait(BaseModel):
    montant: float = Field(..., gt=0, description="Montant de l'opération", example=100.00)


class CompteResponse(BaseModel):
    numero: str
    titulaire: str
    solde: float
    type: str
    decouvert: Optional[float] = None
    taux: Optional[float] = None
    date_creation: Optional[datetime] = None
    date_deblocage: Optional[datetime] = None
    est_bloque: Optional[bool] = None


class MessageResponse(BaseModel):
    message: str = Field(..., example="Opération réussie")


class InteretsResponse(BaseModel):
    message: str
    total_gains: float
    comptes_mis_a_jour: int


class ErrorResponse(BaseModel):
    detail: str = Field(..., example="Compte introuvable")