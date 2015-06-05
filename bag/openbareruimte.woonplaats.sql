SELECT
  opr.identificatie AS id,
  openbareruimtenaam AS name,
  gerelateerdewoonplaats::int AS woonplaatscode,
  wp.woonplaatsnaam::text
FROM
  openbareruimteactueelbestaand opr
JOIN
  woonplaats wp ON opr.gerelateerdewoonplaats = wp.identificatie
WHERE
  wp.identificatie = {woonplaatscode}
