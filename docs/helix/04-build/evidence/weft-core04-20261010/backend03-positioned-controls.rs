
#[test]
fn reviewer_registry_positioned_exact_map_positive_and_refusals() {
    let c=catalog();
    let p=crate::path_application_resolve::resolve(&c,crate::path_query::parse("SELECT l.id,l.id FROM order_lines l").unwrap(),Default::default()).unwrap();
    let v=Plan04View::new(&p);let m=manifest(v);
    assert!(v.capabilities().iter().any(|c|c=="project.positionedOutputs"));
    let columns=v.outputs().enumerate().map(|(i,o)| {
        let ExpressionView::Legacy(crate::arithmetic_plan::Expression::Field{identity,..})=o.expression else {panic!()};
        let d=v.type_graph().iter().find(|d|d.identity==*identity).unwrap();
        let crate::application_model::Shape::Scalar{logical_type}=&d.shape else {panic!()};
        Column{position:i+1,output_name:o.name.into(),carrier_name:Some(format!("physical_{i}")),representation:Representation::Scalar{logical_type:logical_type.clone(),carrier:crate::backend::ScalarCarrier::Text,decoder:crate::backend::ScalarDecoder::Text,path_target:None},source_identities:vec![identity.clone()],nullable:false}
    }).collect();
    let mut e=Emission{sql:"SELECT 'a','b'".into(),parameters:vec![],columns,obligations:vec![]};
    let map=json!(e.columns.iter().map(|c|json!({"position":c.position,"outputName":c.output_name,"carrierName":c.carrier_name,"sourceIdentities":c.source_identities})).collect::<Vec<_>>());
    e.obligations.push(Obligation{id:"weft.output.positioned".into(),owner:crate::backend::ObligationOwner::Host,failure_code:"WFT-OBLIGATION".into(),parameters:json!({"profile":"weft-positioned-output/0.3.0","columns":map})});
    for case in 0..10 {
        let mut bad=e.clone();
        match case {
            0=>(),
            1=>bad.obligations.clear(),
            2=>bad.obligations[0].parameters["columns"].as_array_mut().unwrap().reverse(),
            3=>{bad.obligations[0].parameters["columns"].as_array_mut().unwrap().pop();},
            4=>{bad.obligations[0].parameters["columns"][1]=bad.obligations[0].parameters["columns"][0].clone();},
            5=>bad.obligations[0].parameters["extra"]=json!(true),
            6=>bad.obligations[0].parameters["columns"][0]["sourceIdentities"]=json!([]),
            7=>bad.columns[1].carrier_name=bad.columns[0].carrier_name.clone(),
            8=>bad.columns[0].carrier_name=None,
            _=>bad.obligations[0].parameters["profile"]=json!("weft-positioned-output/0.4.0"),
        }
        let mut r=Registry::default();r.register(FixtureBackend{manifest:m.clone(),emission:bad,edges:vec![],calls:Arc::new(AtomicUsize::new(0))}).unwrap();
        let result=r.compile(&c,v,&target(),&binding());
        if case==0 {result.unwrap();} else {assert!(result.is_err(),"registry must refuse isolated map mutation {case}");}
    }
}
