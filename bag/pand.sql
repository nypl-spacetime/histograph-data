SELECT
  p.identificatie AS id,
  bouwjaar::int,
  ST_AsGeoJSON(ST_Transform(ST_Force_2d(p.geovlak), 4326)) AS geometry
FROM
  pandactueelbestaand p
