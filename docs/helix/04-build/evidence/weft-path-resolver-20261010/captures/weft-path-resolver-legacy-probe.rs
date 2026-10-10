
#[cfg(test)]
mod path_resolver_compatibility_probe {
    use super::*;
    fn run(req: &serde_json::Value, keep_profile: bool) -> serde_json::Value {
        let result: Result<ir::Plan> = (|| {
            let catalog = Catalog::prepare(serde_json::from_value(req["modules"].clone()).map_err(|e| fail("PROBE-INPUT", &e.to_string()))?)?;
            let query = ast::parse(req["sql"].as_str().unwrap())?;
            let parameters = serde_json::from_value(req.get("parameters").cloned().unwrap_or(json!({}))).map_err(|e| fail("PROBE-INPUT", &e.to_string()))?;
            let profile = if keep_profile {serde_json::from_value(req.get("readProfile").cloned().unwrap_or(json!(null))).map_err(|e| fail("PROBE-INPUT", &e.to_string()))?} else {None};
            resolve(&catalog, query, parameters, profile)
        })();
        serde_json::to_value(result).unwrap()
    }
    #[test]
    fn capture_legacy_resolution() {
        let cases: serde_json::Value = serde_json::from_str(include_str!("../../../tests/application/fixtures/cases.json")).unwrap();
        let mut captures = vec![];
        for case in cases.as_array().unwrap() {
            for keep_profile in [false,true] {
                captures.push(json!({"id":case["id"],"keepProfile":keep_profile,"result":run(&case["request"],keep_profile)}));
            }
        }
        let base = cases.as_array().unwrap().iter().find(|c|c["id"]=="join-count").unwrap()["request"].clone();
        let controls = [
            ("SELECT c.id+1 AS next,o.total*12.5000 AS scaled FROM Customer c JOIN Orders o ON o.customer_id=c.id WHERE o.total*2>1",json!({})),
            ("SELECT c.name,COUNT(DISTINCT c.name) AS n FROM Customer c GROUP BY c.name HAVING COUNT(DISTINCT c.name)>1 ORDER BY c.name LIMIT 10",json!({})),
            ("SELECT c.id FROM Customer c LEFT JOIN Orders o ON c.id=o.customer_id WHERE c.id>0 ORDER BY o.id",json!({})),
            ("SELECT c.id+1 AS next,COUNT(*) AS n FROM Customer c GROUP BY c.id",json!({})),
            ("SELECT c.id+1 AS next,COUNT(*) AS n FROM Customer c",json!({})),
            ("SELECT :n+1 AS next FROM Customer c WHERE c.id=:n",json!({"n":{"family":"integer","value":"2"}})),
            ("SELECT :n+1 AS next FROM Customer c WHERE c.name=:n",json!({"n":{"family":"integer","value":"2"}})),
            ("SELECT c.id+1 AS next FROM Customer c",json!({"unused":{"family":"integer","value":"2"}})),
            ("SELECT :n+1 AS next FROM Customer c",json!({"n":{"family":"integer","value":"2"},"N":{"family":"integer","value":"3"}})),
            ("SELECT c.id AS n,c.name AS n FROM Customer c",json!({})),
            ("SELECT c.id,c.id FROM Customer c",json!({})),
            ("SELECT c.name,COUNT(DISTINCT c.name) AS n FROM Customer c GROUP BY c.name HAVING COUNT(DISTINCT d.name)>1",json!({})),
            ("SELECT c.name,COUNT(*) AS n FROM Customer c GROUP BY c.name HAVING COUNT(DISTINCT c.name)>1",json!({})),
        ];
        for (index,(sql,parameters)) in controls.into_iter().enumerate() {
            let mut req=base.clone();req["sql"]=json!(sql);req["parameters"]=parameters;
            captures.push(json!({"id":format!("control-{index}"),"result":run(&req,false)}));
        }
        let mut optional=base.clone();
        for module in optional["modules"].as_array_mut().unwrap() {
            let mut doc:serde_json::Value=serde_json::from_str(module["documentJson"].as_str().unwrap()).unwrap();
            for m in doc["modules"].as_array_mut().unwrap() {for e in m["elements"].as_array_mut().unwrap() {if e["kind"]=="field" && e["name"]=="name" {e["nullability"]=json!("absent-allowed");}}}
            let raw=doc.to_string();module["documentJson"]=json!(raw);module["pin"]["sha256"]=json!(crate::json::sha256(raw.as_bytes()));
        }
        for (index,sql) in [
            "SELECT COUNT(DISTINCT c.name) AS n FROM Customer c HAVING COUNT(DISTINCT c.name)>1",
            "SELECT c.name FROM Customer c WHERE c.name IS NULL",
            "SELECT c.id FROM Customer c LEFT JOIN Customer d ON c.name=d.name ORDER BY d.name",
        ].into_iter().enumerate() {optional["sql"]=json!(sql);captures.push(json!({"id":format!("optional-{index}"),"result":run(&optional,false)}));}
        std::fs::write("/private/tmp/weft-path-resolver-legacy-probe-current.json",serde_json::to_vec_pretty(&captures).unwrap()).unwrap();
    }
}
