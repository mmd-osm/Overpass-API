[maxsize:4294967296]
[timeout:86400];

(
  relation
    (changed:"{{area_version}}","{{area_version}}")
    ["type"="multipolygon"]
    ["name"];
  relation
    (changed:"{{area_version}}","{{area_version}}")
    ["type"="boundary"]
    ["name"];
  relation
    (changed:"{{area_version}}","{{area_version}}")
    ["admin_level"]
    ["name"];
  relation
    (changed:"{{area_version}}","{{area_version}}")
    ["postal_code"];
  relation
    (changed:"{{area_version}}","{{area_version}}")
    ["addr:postcode"];
);

foreach->.pivot{
  (way(r.pivot);node(w););
  make_area[.pivot, no];
}

