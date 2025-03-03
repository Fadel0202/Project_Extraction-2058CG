-- Désactiver temporairement les contraintes de clés étrangères
SET session_replication_role = 'replica';

-- Vider toutes les tables dans le bon ordre (pour respecter les dépendances)
TRUNCATE TABLE credit_impot CASCADE;
TRUNCATE TABLE evolution_cir CASCADE;
TRUNCATE TABLE annexe_2069a12 CASCADE;
TRUNCATE TABLE societe_fille CASCADE;
TRUNCATE TABLE formulaire_2069a CASCADE;
TRUNCATE TABLE formulaire_2058cg CASCADE;
TRUNCATE TABLE depot CASCADE;
TRUNCATE TABLE societe CASCADE;
TRUNCATE TABLE annee_fiscale CASCADE;

-- Réinitialiser les séquences
ALTER SEQUENCE annee_fiscale_millesime_id_seq RESTART WITH 1;
ALTER SEQUENCE annexe_2069a12_annexe_2069a12_id_seq RESTART WITH 1;
ALTER SEQUENCE credit_impot_id_seq RESTART WITH 1;
ALTER SEQUENCE evolution_cir_id_seq RESTART WITH 1;
ALTER SEQUENCE formulaire_2058cg_formulaire_2058cg_id_seq RESTART WITH 1;
ALTER SEQUENCE formulaire_2069a_formulaire_2069a_id_seq RESTART WITH 1;
ALTER SEQUENCE societe_fille_id_seq RESTART WITH 1;
ALTER SEQUENCE societe_siren_seq RESTART WITH 1;

-- Réactiver les contraintes de clés étrangères
SET session_replication_role = 'origin';

-- Vérifier que les tables sont vides
SELECT 'annee_fiscale' AS table_name, COUNT(*) AS row_count FROM annee_fiscale UNION ALL
SELECT 'annexe_2069a12', COUNT(*) FROM annexe_2069a12 UNION ALL
SELECT 'credit_impot', COUNT(*) FROM credit_impot UNION ALL
SELECT 'depot', COUNT(*) FROM depot UNION ALL
SELECT 'evolution_cir', COUNT(*) FROM evolution_cir UNION ALL
SELECT 'formulaire_2058cg', COUNT(*) FROM formulaire_2058cg UNION ALL
SELECT 'formulaire_2069a', COUNT(*) FROM formulaire_2069a UNION ALL
SELECT 'societe', COUNT(*) FROM societe UNION ALL
SELECT 'societe_fille', COUNT(*) FROM societe_fille
ORDER BY table_name;
