-- Script d'insertion des données test basées sur le fichier XML

-- 1. Insertion dans la table societe
INSERT INTO societe (siren, denomination_societe) VALUES
(491544458, 'MERE-CERIE RASCOLL'),
(533382487, 'FILLE3-ROSSANO'),
(882376122, 'FILLE1-CAZEEL'),
(801755927, 'FILLE2-LA GOELETTE');

-- 2. Insertion dans la table annee_fiscale
INSERT INTO annee_fiscale (millesime_id, date_debut, date_fin, duree) VALUES
(51, '2049-12-31', '2050-12-30', 365);

-- 3. Insertion dans la table depot
INSERT INTO depot (id_depot, millesime_id, siren, datedebper, datefinper, dateenregistrement, depose_neant) VALUES
('qXbUsvgiRdKerTgTyl3OxQ==', 51, 491544458, '2049-12-31', '2050-12-30', '2051-05-12', false),
('b4osNg0BTP+62z2xBy//qw==', 51, 533382487, '2049-12-31', '2050-12-30', '2051-05-12', false),
('b/tkxsivRHW+Bz+a3J3h6Q==', 51, 491544458, '2049-12-31', '2050-12-30', '2051-05-12', false);

-- 4. Insertion dans la table formulaire_2058cg
INSERT INTO formulaire_2058cg (id_depot, siren_societe, nombre_filiales, nombre_filiales_renseignees) VALUES
('qXbUsvgiRdKerTgTyl3OxQ==', 491544458, 3, 3),
('b/tkxsivRHW+Bz+a3J3h6Q==', 491544458, 3, 3);

-- 5. Insertion dans la table formulaire_2069a
INSERT INTO formulaire_2069a (id_depot, siren_societe_deposant, siren_societe_declarant, type_declarant) VALUES
('qXbUsvgiRdKerTgTyl3OxQ==', 491544458, 491544458, 'ISGROUPE'),
('qXbUsvgiRdKerTgTyl3OxQ==', 491544458, 882376122, 'FILIALE'),
('qXbUsvgiRdKerTgTyl3OxQ==', 491544458, 801755927, 'FILIALE'),
('b4osNg0BTP+62z2xBy//qw==', 533382487, 533382487, 'FILIALE');

-- Récupérer les IDs générés pour les références dans les autres tables
DO $$
DECLARE
    form_id_mere integer;
    form_id_fille1 integer;
    form_id_fille2 integer;
    form_id_fille3 integer;
    form_2058cg_id1 integer;
    form_2058cg_id2 integer;
BEGIN
    -- Récupérer les IDs des formulaires 2069A
    SELECT formulaire_2069a_id INTO form_id_mere FROM formulaire_2069a 
    WHERE siren_societe_declarant = 491544458 AND siren_societe_deposant = 491544458 LIMIT 1;
    
    SELECT formulaire_2069a_id INTO form_id_fille1 FROM formulaire_2069a 
    WHERE siren_societe_declarant = 882376122 LIMIT 1;
    
    SELECT formulaire_2069a_id INTO form_id_fille2 FROM formulaire_2069a 
    WHERE siren_societe_declarant = 801755927 LIMIT 1;
    
    SELECT formulaire_2069a_id INTO form_id_fille3 FROM formulaire_2069a 
    WHERE siren_societe_declarant = 533382487 LIMIT 1;
    
    -- Récupérer les IDs des formulaires 2058CG
    SELECT formulaire_2058cg_id INTO form_2058cg_id1 FROM formulaire_2058cg 
    WHERE id_depot = 'qXbUsvgiRdKerTgTyl3OxQ==' LIMIT 1;
    
    SELECT formulaire_2058cg_id INTO form_2058cg_id2 FROM formulaire_2058cg 
    WHERE id_depot = 'b/tkxsivRHW+Bz+a3J3h6Q==' LIMIT 1;
    
    -- 6. Insertion dans la table annexe_2069a12
    INSERT INTO annexe_2069a12 (formulaire_2069a_id) VALUES
    (form_id_mere),
    (form_id_fille1),
    (form_id_fille2),
    (form_id_fille3);
    
    -- 7. Insertion dans la table societe_fille
    INSERT INTO societe_fille (formulaire_2058cg_id, siren_fille, creances_report_filiales, total_creances_report_filiales, creances_utilisees_fille, total_creances_utilisees_fille) VALUES
    (form_2058cg_id1, 882376122, 5319, 5319, 165003, 165003),
    (form_2058cg_id1, 801755927, 494, 494, 89, 89),
    (form_2058cg_id1, 533382487, 1486800, 1486800, 2400000, 2400000),
    (form_2058cg_id2, 882376122, 5319, 5319, 165003, 165003),
    (form_2058cg_id2, 801755927, 494, 494, 89, 89),
    (form_2058cg_id2, 533382487, 1486800, 1486800, 2400000, 2400000);
    
    -- 8. Insertion dans la table credit_impot
    INSERT INTO credit_impot (siren_fille, formulaire_2069a_id, type_credit, montant_credit_impot) VALUES
    (491544458, form_id_mere, 'CIR', 2625001),
    (882376122, form_id_fille1, 'CIR', 5319),
    (882376122, form_id_fille1, 'CRC', 165003),
    (801755927, form_id_fille2, 'CIR', 494),
    (801755927, form_id_fille2, 'CRC', 89),
    (533382487, form_id_fille3, 'CIR', 1486800),
    (533382487, form_id_fille3, 'CRC', 2400000);
    
    -- 9. Insertion dans la table evolution_cir (données d'exemple car nous n'avons pas d'historique)
    INSERT INTO evolution_cir (societe_siren, millesime_id, montant_credit_impot, variation, pourcentage_evolution) VALUES
    (491544458, 51, 2625001, 0, 0),
    (882376122, 51, 5319, 0, 0),
    (801755927, 51, 494, 0, 0),
    (533382487, 51, 1486800, 0, 0);
END $$;
