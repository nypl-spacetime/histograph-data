SELECT DISTINCT ON (vbo.identificatie)
  vbo.identificatie::bigint AS id,
  (
  	SELECT
  	array_to_string(array_agg(p.identificatie::bigint), ',') AS pand_ids
  	FROM pandactueelbestaand p
  	JOIN verblijfsobjectpandactueel vbop
  	ON p.identificatie = vbop.gerelateerdpand
  	WHERE vbop.identificatie = vbo.identificatie
  ),
  openbareruimtenaam,
  huisnummer,
  huisletter,
  huisnummertoevoeging
  postcode,
  opr.identificatie AS openbareruimte,
  ST_AsGeoJSON(ST_Transform(ST_Force_2d(geopunt), 4326)) AS geometry
FROM
  verblijfsobjectactueelbestaand vbo
JOIN
  verblijfsobjectgebruiksdoelactueel gd
ON
  gd.identificatie = vbo.identificatie
JOIN
  nummeraanduidingactueelbestaand na
ON
  na.identificatie = vbo.hoofdadres
JOIN
  openbareruimteactueelbestaand opr
ON
  na.gerelateerdeopenbareruimte = opr.identificatie
