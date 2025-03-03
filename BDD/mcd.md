```mermaid
---
title: Schema Base de Données CIR
---
erDiagram
    %% Définition des entités principales
    SOCIETE {
        integer siren PK
        varchar denomination_societe
        varchar complement_denomination
        varchar forme_juridique
    }

    ANNEE_FISCALE {
        integer millesime_id PK
        date date_debut
        date date_fin
        integer duree
    }

    DEPOT {
        varchar id_depot PK
        integer millesime_id FK
        integer siren FK
        date dateDebPer
        date dateFinPer
        date dateEnregistrement
        boolean depose_neant
    }

    %% Définition des formulaires
    FORMULAIRE_2058CG {
        integer formulaire_2058cg_id PK
        varchar id_depot FK
        integer siren_societe FK
        integer nombre_filiales
        integer nombre_filiales_renseignees
    }

    FORMULAIRE_2069A {
        integer formulaire_2069a_id PK
        varchar id_depot FK
        integer siren_societe_deposant FK
        integer siren_societe_declarant FK
        varchar type_declarant
        text Adresse
        integer nb_societe_grp
    }

    ANNEXE_2069A12 {
        integer annexe_2069a12_id PK
        integer formulaire_2069a_id FK
    }

    SOCIETE_FILLE {
        integer id PK
        integer formulaire_2058cg_id FK
        integer siren_fille FK
        decimal creances_report_filiales
        decimal total_creances_report_filiales
        decimal creances_utilisees_fille
        decimal total_creances_utilisees_fille
    }

    CREDIT_IMPOT {
        integer id PK
        integer siren_fille FK
        integer formulaire_2069a_id FK
        varchar type_credit
        decimal montant_credit_impot
        text precision_utilisation_ci
        decimal reduction_credit_impot
    }

    EVOLUTION_CIR {
        integer id PK
        integer societe_siren FK
        integer millesime_id FK
        decimal montant_credit_impot
        decimal variation
        decimal pourcentage_evolution
    }

    %% Définition des relations avec cardinalités explicites
    SOCIETE ||--o{ DEPOT : "effectue (1,n)"
    ANNEE_FISCALE ||--o{ DEPOT : "appartient à (1,n)"
    DEPOT ||--o{ FORMULAIRE_2058CG : "contient (0,n)"
    DEPOT ||--o{ FORMULAIRE_2069A : "contient (0,n)"

    SOCIETE ||--o{ SOCIETE_FILLE : "est mère de (0,n)"
    SOCIETE ||--o{ EVOLUTION_CIR : "suit évolution (0,n)"

    FORMULAIRE_2058CG ||--o{ SOCIETE_FILLE : "déclare (0,n)"
    FORMULAIRE_2069A ||--o| ANNEXE_2069A12 : "contient (0,1)"
    FORMULAIRE_2069A ||--o{ CREDIT_IMPOT : "déclare (0,n)"

    SOCIETE_FILLE ||--o{ CREDIT_IMPOT : "possède (0,n)"

