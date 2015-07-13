SELECT
  opr.identificatie AS id,
  openbareruimtenaam AS name,
  gerelateerdewoonplaats::int AS woonplaatscode,
  wp.woonplaatsnaam::text
FROM
  openbareruimteactueelbestaand opr
JOIN
  woonplaatsactueelbestaand wp ON opr.gerelateerdewoonplaats = wp.identificatie
