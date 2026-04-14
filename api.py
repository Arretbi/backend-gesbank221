import os
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Union

from .models import (
    CompteCreateCourant, CompteCreateEpargne, CompteCreateBloque,
    CompteUpdate, DepotRetrait, CompteResponse, MessageResponse,
    InteretsResponse, ErrorResponse
)
from .storage import BanqueStorage
from .services import BanqueService, ServiceError


app = FastAPI(
    title="API Gestion de Comptes Bancaires",
    description="API REST pour la gestion de comptes bancaires (courant, épargne, bloqué) avec stockage JSON",
    version="1.0.0",
    contact={"name": "Équipe Banque", "email": "contact@banque-api.com"},
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db_url = os.environ.get("DATABASE_URL")
storage = BanqueStorage(db_url)
service = BanqueService(storage)


@app.get("/", tags=["Racine"], summary="Page d'accueil de l'API")
def racine():
    return {
        "message": "Bienvenue sur l'API Gestion de Comptes Bancaires",
        "documentation": "/docs",
        "redoc": "/redoc",
        "version": "1.0.0",
        "endpoints": {
            "comptes": "/comptes",
            "comptes_courants": "/comptes/type/courant",
            "comptes_epargne": "/comptes/type/epargne",
            "comptes_bloques": "/comptes/type/bloque"
        }
    }


@app.get(
    "/comptes",
    response_model=List[CompteResponse],
    tags=["Comptes"],
    summary="Lister tous les comptes",
    description="Retourne la liste complète des comptes triés par titulaire."
)
def lister_comptes():
    try:
        return service.tous()
    except ServiceError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/comptes/type/courant",
    response_model=List[CompteResponse],
    tags=["Comptes"],
    summary="Lister les comptes courants"
)
def lister_courants():
    try:
        return service.tous_par_type("courant")
    except ServiceError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/comptes/type/epargne",
    response_model=List[CompteResponse],
    tags=["Comptes"],
    summary="Lister les comptes épargne"
)
def lister_epargnes():
    try:
        return service.tous_par_type("epargne")
    except ServiceError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/comptes/type/bloque",
    response_model=List[CompteResponse],
    tags=["Comptes"],
    summary="Lister les comptes bloqués"
)
def lister_bloques():
    try:
        return service.tous_par_type("bloque")
    except ServiceError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/comptes/{numero}",
    response_model=CompteResponse,
    tags=["Comptes"],
    summary="Récupérer un compte par numéro",
    responses={404: {"model": ErrorResponse, "description": "Compte introuvable"}}
)
def obtenir_compte(numero: str):
    compte = service.get_compte(numero)
    if compte is None:
        raise HTTPException(status_code=404, detail="Compte introuvable.")
    return compte


@app.post(
    "/comptes/courant",
    response_model=CompteResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Création"],
    summary="Créer un compte courant",
    responses={400: {"model": ErrorResponse, "description": "Données invalides"}}
)
def creer_courant(compte: CompteCreateCourant):
    try:
        return service.creer_compte_courant(
            compte.numero, compte.titulaire, compte.solde, compte.decouvert
        )
    except ServiceError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post(
    "/comptes/epargne",
    response_model=CompteResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Création"],
    summary="Créer un compte épargne",
    responses={400: {"model": ErrorResponse, "description": "Données invalides"}}
)
def creer_epargne(compte: CompteCreateEpargne):
    try:
        return service.creer_compte_epargne(
            compte.numero, compte.titulaire, compte.solde, compte.taux
        )
    except ServiceError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post(
    "/comptes/bloque",
    response_model=CompteResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Création"],
    summary="Créer un compte bloqué",
    responses={400: {"model": ErrorResponse, "description": "Données invalides"}}
)
def creer_bloque(compte: CompteCreateBloque):
    try:
        date_str = compte.date_creation.strftime("%d/%m/%Y %H:%M")
        return service.creer_compte_bloque(
            compte.numero, compte.titulaire, compte.solde, date_str
        )
    except ServiceError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post(
    "/comptes/{numero}/deposer",
    response_model=CompteResponse,
    tags=["Opérations"],
    summary="Déposer de l'argent sur un compte",
    responses={
        400: {"model": ErrorResponse},
        404: {"model": ErrorResponse, "description": "Compte introuvable"}
    }
)
def deposer(numero: str, operation: DepotRetrait):
    try:
        return service.deposer(numero, operation.montant)
    except ServiceError as e:
        if "introuvable" in str(e).lower():
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=400, detail=str(e))


@app.post(
    "/comptes/{numero}/retirer",
    response_model=CompteResponse,
    tags=["Opérations"],
    summary="Retirer de l'argent d'un compte",
    responses={
        400: {"model": ErrorResponse, "description": "Solde insuffisant ou compte bloqué"},
        404: {"model": ErrorResponse, "description": "Compte introuvable"}
    }
)
def retirer(numero: str, operation: DepotRetrait):
    try:
        return service.retirer(numero, operation.montant)
    except ServiceError as e:
        if "introuvable" in str(e).lower():
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=400, detail=str(e))


@app.post(
    "/comptes/interets",
    response_model=InteretsResponse,
    tags=["Opérations"],
    summary="Appliquer les intérêts aux comptes épargne"
)
def appliquer_interets():
    try:
        result = service.appliquer_interets()
        return {
            "message": "Intérêts appliqués avec succès.",
            "total_gains": result["total_gains"],
            "comptes_mis_a_jour": result["comptes_mis_a_jour"]
        }
    except ServiceError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put(
    "/comptes/{numero}",
    response_model=CompteResponse,
    tags=["Comptes"],
    summary="Modifier un compte existant",
    responses={
        400: {"model": ErrorResponse},
        404: {"model": ErrorResponse, "description": "Compte introuvable"}
    }
)
def modifier_compte(numero: str, maj: CompteUpdate):
    try:
        return service.modifier(numero, maj.titulaire, maj.decouvert, maj.taux)
    except ServiceError as e:
        if "introuvable" in str(e).lower():
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=400, detail=str(e))


@app.delete(
    "/comptes/{numero}",
    response_model=MessageResponse,
    tags=["Comptes"],
    summary="Supprimer un compte",
    responses={404: {"model": ErrorResponse, "description": "Compte introuvable"}}
)
def supprimer_compte(numero: str):
    try:
        service.supprimer(numero)
        return {"message": "Compte supprimé avec succès."}
    except ServiceError as e:
        if "introuvable" in str(e).lower():
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=400, detail=str(e))