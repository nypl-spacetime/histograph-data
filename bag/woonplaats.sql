SELECT
  identificatie::int AS id,
  woonplaatsnaam::text AS name,
  gemeentecode::int,
  ST_AsGeoJSON(ST_Transform(ST_Force_2d(geovlak), 4326)) AS geometry
FROM
  woonplaatsactueelbestaand wp
JOIN
  gemeente_woonplaatsactueelbestaand gwp
ON
  wp.identificatie = gwp.woonplaatscode
