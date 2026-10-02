INSERT INTO concern_effect_map (concern_id, effect_id, priority_score)
SELECT c.concern_id, e.effect_id, 1.0
FROM concern_taxonomy c
JOIN effect_taxonomy e
ON (
    (c.concern_code='ACNE' AND e.effect_code IN ('ANTI_INFLAMMATORY','SEBUM_REGULATION','KERATOLYTIC','COMEDOLYTIC','ANTIMICROBIAL','BLEMISH_CARE'))
 OR (c.concern_code='SENSITIVE_SKIN' AND e.effect_code IN ('SOOTHING','ANTI_INFLAMMATORY','BARRIER_REPAIR','HYDRATING'))
 OR (c.concern_code='BARRIER_DAMAGE' AND e.effect_code IN ('BARRIER_REPAIR','HYDRATING','MOISTURE_RETENTION','SOOTHING'))
 OR (c.concern_code='HYPERPIGMENTATION' AND e.effect_code IN ('DEPIGMENTING','BRIGHTENING','ANTI_INFLAMMATORY'))
 OR (c.concern_code='DRY_SKIN' AND e.effect_code IN ('HYDRATING','MOISTURE_RETENTION','BARRIER_REPAIR'))
 OR (c.concern_code='OILY_SKIN' AND e.effect_code IN ('SEBUM_REGULATION','ANTI_INFLAMMATORY'))
 OR (c.concern_code='POST_ACNE_MARKS' AND e.effect_code IN ('DEPIGMENTING','BRIGHTENING','ANTI_INFLAMMATORY'))
);