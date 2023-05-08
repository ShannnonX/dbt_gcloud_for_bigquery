{{
    config(
        materialized = 'table'
    )
}}


SELECT *, 
  CASE
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('St', 'st', 'Street', 'street', 's', 'S') THEN 'Street'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Rd', 'rd', 'Road', 'road') THEN 'Road'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Grove', 'grove') THEN 'Grove'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Gr', 'gr') THEN 'Green'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Dr', 'dr', 'Drive', 'drive') THEN 'Drive'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Pde', 'pde', 'Parade', 'parade') THEN 'Parade'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Pl', 'pl', 'Place', 'place') THEN 'Place'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Ct', 'ct', 'Court', 'court') THEN 'Court'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Av', 'av', 'Ave', 'ave', 'Avenue', 'avenue') THEN 'Avenue'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Corso', 'corso') THEN 'Corso'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Glade', 'glade') THEN 'Glade'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Gdns', 'gdns') THEN 'Gardens'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Highway', 'highway') THEN 'Highway'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Cir', 'cir', 'Circle', 'circle') THEN 'Circle'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Fairway', 'fairway') THEN 'Fairway'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Ambl', 'ambl', 'Ambleside', 'ambleside') THEN 'Ambleside'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Crossway', 'crossway') THEN 'Crossway'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Briars', 'briars') THEN 'Briars'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Mw', 'mw', 'Motorway', 'motorway') THEN 'Motorway'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Victoria', 'victoria') THEN 'Victoria'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Circuit', 'circuit') THEN 'Circuit'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('East', 'east') THEN 'East'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Cove', 'cove') THEN 'Cove'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Sq', 'square', 'SQR', 'sqr') THEN 'Square'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Esp', 'esp', 'esplanade', 'e', 'E') THEN 'Esplanade'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Tce', 'tce', 'terrace') THEN 'Terrace'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Bvd', 'bvd', 'terrace') THEN 'Boulevard'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Gra', 'gra', 'grange') THEN 'Grange'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('cr', 'Cr', 'cres') THEN 'Cres'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('n', 'N') THEN 'Street North'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('W', 'w') THEN 'Street West'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('la', 'LA', 'La') THEN 'Lane'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('HWY', 'hwy', 'Hwy') THEN 'Highway'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('cct', 'CCT', 'Cct') THEN 'Circuit, Coburg'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('wy', 'Wy', 'WY') THEN 'Way'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Cl', 'cl', 'Cl') THEN 'Close'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('qy', 'QY', 'Qy') THEN 'Quay'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('res', 'Res', 'RES') THEN 'Reserve'
    WHEN REGEXP_EXTRACT(Address, r'\b(\w+)\s*$') IN ('Hts', 'hts', 'HTS') THEN 'Heights'
    ELSE REGEXP_EXTRACT(Address, r'\b(\w+)\s*$')
  END AS StreetName
FROM {{ref('silver_full_column_version_updated')}}