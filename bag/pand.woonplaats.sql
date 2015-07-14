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
  JOIN
    verblijfsobjectpand vbop
  ON
    vbop.gerelateerdpand = p.identificatie
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
    opr.gerelateerdewoonplaats = {woonplaatscode}
) AS panden
WHERE openbareruimtes != ''
