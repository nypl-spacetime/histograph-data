SELECT
  opr.identificatie AS id,
  openbareruimtenaam AS name,
  gerelateerdewoonplaats AS woonplaatscode
FROM
  openbareruimteactueelbestaand opr
JOIN
  woonplaats wp ON opr.gerelateerdewoonplaats = wp.identificatie
