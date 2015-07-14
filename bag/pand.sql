SELECT * FROM (
  SELECT
    p.identificatie AS id,
    bouwjaar,
    ST_AsGeoJSON(ST_Transform(ST_Force_2d(p.geovlak), 4326)) AS geometry,
    array_to_string(ARRAY(
      SELECT DISTINCT opr.identificatie::bigint FROM
        verblijfsobjectpandactueel vbop
      JOIN
        verblijfsobjectactueelbestaand vbo
      ON
        vbo.identificatie = vbop.identificatie
      JOIN
        nummeraanduidingactueelbestaand na
      ON
        na.identificatie = vbo.hoofdadres
      JOIN
        openbareruimteactueelbestaand opr
      ON
        na.gerelateerdeopenbareruimte = opr.identificatie
      WHERE
       vbop.gerelateerdpand = p.identificatie
     ), ',') AS openbareruimtes
  FROM pandactueelbestaand p
) AS panden
WHERE openbareruimtes != ''
