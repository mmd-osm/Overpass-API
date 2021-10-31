[maxsize:4294967296]
[timeout:86400];

(
  relation
    ["type"="multipolygon"]
    ["name"];
  relation
    ["type"="boundary"]
    ["name"];
  relation
    ["admin_level"]
    ["name"];
  relation["postal_code"];
  relation["addr:postcode"];
);

foreach->.pivot{
  (
    way(r.pivot);
    node(w);
  );
  make_area[.pivot, return_area=no];
}

