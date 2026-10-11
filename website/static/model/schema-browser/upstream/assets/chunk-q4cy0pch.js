import{$ as $h,$a as _f,Aa as Yf,Ab as Kf,Ba as En,Bb as Vf,Ca as ke,Cb as yf,Da as cl,Ea as A,Eb as El,Fa as hh,Ga as zt,Ha as Pl,Hb as Uf,Ia as ye,Ja as re,Ka as Oe,La as Te,Ma as Mn,Na as pe,Oa as ol,Pa as bt,Qa as Zn,Ra as $n,Sa as Ke,Ta as Od,Ua as Lt,Va as qt,Wa as Wt,Xa as Oh,Ya as Un,Z as Qf,Za as nl,_ as Nt,_a as el,aa as o,ab as Yn,ba as Mf,bb as At,ca as Bf,cb as Hd,da as K,db as Ff,ea as Yi,eb as Hf,fa as ge,fb as Nd,ga as U,gb as Nf,ha as L,hb as hl,ia as Xe,ib as dl,ja as Tn,jb as jf,ka as Ol,kb as D,la as It,lb as bs,ma as rs,mb as Vt,na as mn,nb as Df,oa as Rt,ob as jd,pa as Bn,pb as bn,qa as H,qb as ds,ra as q,rb as Wn,sa as un,sb as ll,ta as nt,tb as jt,ua as et,ub as F,va as Of,vb as Vh,wa as Pf,wb as gd,xa as Xf,xb as bl,ya as Zf,yb as yd,za as $f,zb as ce}from"./chunk-24vsj7ef.js";import"./chunk-mpg5a0b8.js";import{Gc as Jf}from"./chunk-9q1y432v.js";import{Rc as nu,Sc as eu}from"./chunk-q47cz7hd.js";import{Uc as gf}from"./chunk-6p6f7mkg.js";import"./chunk-b29kpdv8.js";import{md as ln,nd as Je,od as ze,pd as Dt}from"./chunk-keh55rwe.js";import{td as an,vd as co}from"./chunk-jbyes3dd.js";var ti={};an(ti,{$onValidate:()=>Kh,$lib:()=>Ct,$decorators:()=>Ou});var hu={type:"object",additionalProperties:!1,properties:{"output-file":{type:"string",nullable:!0,description:["Name of the output file."," Output file will interpolate the following values:"," - schema-name: Name of the schema if multiple",""," Default: `{schema-name}.graphql`",""," Example Single schema"," - `schema.graphql`",""," Example Multiple schemas"," - `Org1.Schema1.graphql`"," - `Org1.Schema2.graphql`"].join(`
`)},"new-line":{type:"string",enum:["crlf","lf"],default:"lf",nullable:!0,description:"Set the newLine character for emitting files."},"omit-unreachable-types":{type:"boolean",nullable:!0,description:["Omit unreachable types.","By default all types declared under the schema namespace will be included.","With this flag on only types references in an operation will be emitted."].join(`
`)}},required:[]},du={name:"@typespec/graphql",diagnostics:{"graphql-operation-kind-duplicate":{severity:"error",messages:{default:o`GraphQL Operation Kind already applied to \`${"entityName"}\`.`}},"operation-field-conflict":{severity:"error",messages:{default:o`Operation \`${"operation"}\` conflicts with an existing ${"conflictType"} on model \`${"model"}\`.`}},"operation-field-duplicate":{severity:"warning",messages:{default:o`Operation \`${"operation"}\` is defined multiple times on \`${"model"}\`.`}},"invalid-interface":{severity:"error",messages:{default:o`All models used with \`@compose\` must be marked with \`@graphqlInterface\`, but ${"interface"} is not.`}},"circular-interface":{severity:"error",messages:{default:"An interface cannot implement itself."}},"missing-interface-property":{severity:"error",messages:{default:o`Model must contain property \`${"property"}\` from \`${"interface"}\` in order to implement it in GraphQL.`}},"incompatible-interface-property":{severity:"error",messages:{default:o`Property \`${"property"}\` is incompatible with \`${"interface"}\`.`}},"unrecognized-union":{severity:"error",messages:{default:"Unrecognized union construction. Union must be named, a return type, a model property, or an alias."}},"duplicate-union-variant":{severity:"warning",messages:{default:o`Union variant type "${"type"}" appears multiple times after flattening nested unions. Duplicate removed.`}},"empty-union":{severity:"error",messages:{default:"Union has no non-null variants. A GraphQL union must contain at least one member type."}},"graphql-builtin-scalar-collision":{severity:"warning",messages:{default:o`Scalar "${"name"}" collides with GraphQL built-in type "${"builtinName"}". This may cause unexpected behavior. Consider renaming the scalar.`}},"type-name-collision":{severity:"error",messages:{default:o`Type "${"name"}" collides with another type of the same name in the GraphQL schema. Consider renaming one of the types.`}},"operation-fields-ignored-on-input":{severity:"warning",messages:{default:o`@operationFields on \`${"model"}\` is ignored in input context — GraphQL input types cannot have operation fields.`}},"empty-schema":{severity:"warning",messages:{default:"GraphQL schema has no operations. At minimum a Query root type is required."}},"empty-enum":{severity:"error",messages:{default:o`Enum "${"name"}" must define at least one value. GraphQL enums cannot be empty.`}},"reserved-name":{severity:"error",messages:{default:o`Name "${"name"}" must not begin with "__" (two underscores), which is reserved by GraphQL for introspection.`}},"unsupported-type":{severity:"warning",messages:{default:o`Type "${"type"}" has no GraphQL equivalent. Using String as fallback.`}}},emitter:{options:hu,capabilities:{dryRun:!0}},state:{operationKind:{description:"State for the graphql operation kind decorators (@query, @mutation, @subscription)"},operationFields:{description:"State for the @operationFields decorator."},compose:{description:"State for the @compose decorator."},interface:{description:"State for the @interface decorator."},interfaceOnly:{description:"State for @interface(#{interfaceOnly: true})."},schema:{description:"State for the @schema decorator."},specifiedBy:{description:"State for the @specifiedBy decorator."}}},Ct=F(du),{reportDiagnostic:en,createDiagnostic:hw,stateKeys:wn}=Ct;function Fh(n,e){return n===e||A(n)===A(e)}function Mt(n,e,s=!1){if(!s&&n.name!==e.name)return!1;return Fh(n.type,e.type)&&n.optional===e.optional}function lu(n,e,s=!1){if(!s&&n.name!==e.name)return!1;let t=new Set(bn(n)),i=new Set(bn(e));if(t.size!==i.size)return!1;if([...t].some((h)=>![...i].some((d)=>Mt(h,d,!1))))return!1;return!0}function Hh(n,e,s=!1){if(!s&&n.name!==e.name)return!1;return Fh(n.returnType,e.returnType)&&lu(n.parameters,e.parameters,!0)}var[fu,uu]=un(wn.interface),[ww,au]=un(wn.interfaceOnly),[cu,ou,bw]=q(wn.compose);function Nh(n,e){return!!fu(n,e)}function wu(n,e){let s=!0;for(let t of e)if(!Nh(n.program,t))s=!1,en(n.program,{code:"invalid-interface",format:{interface:t.name},target:n.decoratorTarget});return s}function bu(n,e,s){let t=!Nh(n.program,e)||!s.includes(e);if(!t)en(n.program,{code:"circular-interface",target:n.decoratorTarget});return t}function ru(n,e,s){let t=!0;for(let i of bn(s))if(!e.has(i.name))t=!1,en(n.program,{code:"missing-interface-property",format:{interface:s.name,property:i.name},target:n.decoratorTarget});else if(!Mt(e.get(i.name),i))t=!1,en(n.program,{code:"incompatible-interface-property",format:{interface:s.name,property:i.name},target:n.decoratorTarget});return t}function Ru(n,e,s){let t=!0,i=new Map([...bn(e)].map((h)=>[h.name,h]));for(let h of s)if(!ru(n,i,h))t=!1;return t}var Bt=(n,e,s)=>{if(Tn(n,e,Bt),uu(n.program,e),s?.interfaceOnly)au(n.program,e)},jh=(n,e,...s)=>{wu(n,s),bu(n,e,s),Ru(n,e,s);let t=cu(n.program,e),i=s;if(t)i=[...t,...i];ou(n.program,e,i)};var[xu,mu,vw]=q(wn.operationFields);function Kt(n,e){return xu(n,e)||new Set}function Eu(n,e,s){if(Kt(n.program,e).has(s))return en(n.program,{code:"operation-field-duplicate",format:{operation:s.name,model:e.name},target:n.getArgumentTarget(0)}),!1;return!0}function vu(n,e,s){let t=[];if([...bn(e)].some((h)=>h.name===s.name))t.push("property");let i=[...Kt(n.program,e)].find((h)=>h.name===s.name);if(i&&!Hh(i,s))t.push("operation");for(let h of t)en(n.program,{code:"operation-field-conflict",format:{operation:s.name,model:e.name,conflictType:h},target:n.getArgumentTarget(0)});return t.length===0}function Dh(n,e,s){let t=Kt(n.program,e);if(!Eu(n,e,s))return;if(!vu(n,e,s))return;t.add(s),mu(n.program,e,t)}var Sh=(n,e,...s)=>{for(let t of s)if(t.kind==="Operation")Dh(n,e,t);else for(let[i,h]of t.operations)Dh(n,e,h)};var[Ch,Tu,Nw]=q(wn.operationKind);function ku(n,e){if(e.decorators.filter((t)=>Iu.includes(t.decorator)&&t.node?.kind===K.DecoratorExpression&&t.node?.parent===e.node).length>1)return en(n.program,{code:"graphql-operation-kind-duplicate",format:{entityName:e.name},target:n.decoratorTarget}),!1;return!0}function Uu(n,e,s){if(ku(n,e))Tu(n.program,e,s)}function yt(n){return(e,s)=>{Uu(e,s,n)}}var pt=yt("Mutation"),gt=yt("Query"),ni=yt("Subscription"),Iu=[pt,gt,ni];var[Mw,Au,Mh]=q(wn.schema);function Bh(n){return[...Mh(n).values()]}function Lu(n,e,s={}){let i=Mh(n).get(e)??{};Au(n,e,{...i,...s,type:e})}var ei=(n,e,s)=>{Tn(n,e,ei),Lu(n.program,e,s)};var[pw,qu]=q(wn.specifiedBy);var si=(n,e,s)=>{Tn(n,e,si),qu(n.program,e,s)};function Kh(n){let e=Bh(n);if(e.length===0)return;for(let s of e)Wu(n,s.type)}function Wu(n,e){let s=!1;if(Oe(e,{operation(t){if(Ch(n,t)!==void 0)s=!0;zu(n,t)},model(t){Gu(n,t)},enum(t){Qu(n,t)},union(t){Ju(n,t)}}),!s)en(n,{code:"empty-schema",target:e})}function Cn(n,e,s){if(e.startsWith("__"))en(n,{code:"reserved-name",format:{name:e},target:s})}function Gu(n,e){if(e.name)Cn(n,e.name,e);for(let s of e.properties.values())Cn(n,s.name,s)}function zu(n,e){Cn(n,e.name,e);for(let s of e.parameters.properties.values())Cn(n,s.name,s)}function Ju(n,e){if(!e.name)return;if(Cn(n,e.name,e),[...e.variants.values()].filter((t)=>!rs(t.type)).length===0)en(n,{code:"empty-union",target:e})}function Qu(n,e){if(e.name)Cn(n,e.name,e);for(let s of e.members.values())Cn(n,s.name,s);if(e.members.size===0)en(n,{code:"empty-enum",format:{name:e.name},target:e})}var Ou={"TypeSpec.GraphQL":{compose:jh,graphqlInterface:Bt,mutation:pt,operationFields:Sh,query:gt,schema:ei,specifiedBy:si,subscription:ni}};var ci={};an(ci,{$lib:()=>ae,$flags:()=>Pe,$decorators:()=>ai});var Rs={type:"object",additionalProperties:!1,properties:{"file-type":{type:"string",enum:["yaml","json"],nullable:!0,description:"Serialize the schema as either yaml or json."},"int64-strategy":{type:"string",enum:["string","number"],nullable:!0,description:`How to handle 64 bit integers on the wire. Options are:

* string: serialize as a string (widely interoperable)
* number: serialize as a number (not widely interoperable)`},bundleId:{type:"string",nullable:!0,description:"When provided, bundle all the schemas into a single json schema document with schemas under $defs. The provided id is the id of the root document and is also used for the file name."},emitAllModels:{type:"boolean",nullable:!0,description:"When true, emit all model declarations to JSON Schema without requiring the @jsonSchema decorator."},emitAllRefs:{type:"boolean",nullable:!0,description:"When true, emit all references as json schema files, even if the referenced type does not have the `@jsonSchema` decorator or is not within a namespace with the `@jsonSchema` decorator."},"seal-object-schemas":{type:"boolean",nullable:!0,default:!1,description:["If true, then for models emitted as object schemas we default `unevaluatedProperties` to `{ not: {} }`,","if not explicitly specified elsewhere.","Default: `false`"].join(`
`)},"polymorphic-models-strategy":{type:"string",enum:["ignore","oneOf","anyOf"],nullable:!0,default:"ignore",description:["Strategy for emitting models with the @discriminator decorator:","- ignore: Emit as regular object schema (default). Derived models use allOf to reference their base model.","- oneOf: Emit a oneOf schema with references to all derived models (closed union)","- anyOf: Emit an anyOf schema with references to all derived models (open union)","","When using oneOf or anyOf, derived models will inline all properties from their base model","instead of using allOf references. This avoids circular references in the generated schemas,","since the base model references derived models via oneOf/anyOf."].join(`
`)}},required:[]},ae=F({name:"@typespec/json-schema",diagnostics:{"invalid-default":{severity:"error",messages:{default:o`Invalid type '${"type"}' for a default value`}},"duplicate-id":{severity:"error",messages:{default:o`There are multiple types with the same id "${"id"}".`}},"unknown-scalar":{severity:"warning",messages:{default:o`Scalar '${"name"}' is not a known scalar type and doesn't extend a known scalar type.`}}},emitter:{options:Rs},state:{JsonSchema:{description:"State indexing types marked with @jsonSchema"},"JsonSchema.baseURI":{description:"Contains data configured with @baseUri decorator"},"JsonSchema.multipleOf":{description:"Contains data configured with @multipleOf decorator"},"JsonSchema.id":{description:"Contains data configured with @id decorator"},"JsonSchema.oneOf":{description:"Contains data configured with @oneOf decorator"},"JsonSchema.contains":{description:"Contains data configured with @contains decorator"},"JsonSchema.minContains":{description:"Contains data configured with @minContains decorator"},"JsonSchema.maxContains":{description:"Contains data configured with @maxContains decorator"},"JsonSchema.uniqueItems":{description:"Contains data configured with @uniqueItems decorator"},"JsonSchema.minProperties":{description:"Contains data configured with @minProperties decorator"},"JsonSchema.maxProperties":{description:"Contains data configured with @maxProperties decorator"},"JsonSchema.contentEncoding":{description:"Contains data configured with @contentEncoding decorator"},"JsonSchema.contentSchema":{description:"Contains data configured with @contentSchema decorator"},"JsonSchema.contentMediaType":{description:"Contains data configured with @contentMediaType decorator"},"JsonSchema.prefixItems":{description:"Contains data configured with @prefixItems decorator"},"JsonSchema.extension":{description:"Contains data configured with @extension decorator"}}}),Pe=Vh({}),{reportDiagnostic:xs,createStateSymbol:wb,stateKeys:C}=ae;function rn(n,e){let[s,t]=q(n);return[s,t,(...h)=>{if(e&&!e(...h))return;let[d,f,l]=h;t(d.program,f,l)}]}function ii(n){return!H(n)&&(n.templateMapper?.args===void 0||n.templateMapper.args?.length===0||n.derivedModels.length>0)}var[ms,Pu]=un(C.JsonSchema),Es=(n,e,s)=>{if(Pu(n.program,e),s)if(e.kind==="Namespace")n.call(Ze,e,s);else n.call(_e,e,s)},[di,kb,Ze]=rn(C["JsonSchema.baseURI"]);function vs(n,e){let s,t=e;do s=di(n,t),t=t.namespace;while(!s&&t);return s}function $e(n,e){let s=e;do{if(ms(n,s))return!0;s=s.namespace}while(s);return!1}function Ts(n){let e=[];function s(h){if(ms(n,h))e.push(h);t(h.models.values()),t(h.enums.values()),t(h.unions.values()),t(h.scalars.values());for(let d of h.namespaces.values())s(d)}function t(h){for(let d of h)i(d)}function i(h){if(!(h.kind!=="Enum"&&H(h))&&$e(n,h))e.push(h)}return s(n.getGlobalNamespaceType()),e}var[li,Ub,ks]=rn(C["JsonSchema.multipleOf"]);function Us(n,e){return li(n,e)?.asNumber()??void 0}var[Ye,Ib,_e]=rn(C["JsonSchema.id"]),[oe,Xu]=un(C["JsonSchema.oneOf"]),Is=(n,e)=>{Xu(n.program,e)},[As,Ab,Ls]=rn(C["JsonSchema.contains"]),[qs,Lb,Ws]=rn(C["JsonSchema.minContains"]),[Gs,qb,zs]=rn(C["JsonSchema.maxContains"]),[Js,Zu]=q(C["JsonSchema.uniqueItems"]),Qs=(n,e)=>Zu(n.program,e,!0),[Os,Wb,Ps]=rn(C["JsonSchema.minProperties"]),[Xs,Gb,Zs]=rn(C["JsonSchema.maxProperties"]),[$s,zb,Ys]=rn(C["JsonSchema.contentEncoding"]),[_s,Jb,Fs]=rn(C["JsonSchema.contentMediaType"]),[Hs,Qb,Ns]=rn(C["JsonSchema.contentSchema"]),[js,$u]=q(C["JsonSchema.prefixItems"]),Ds=(n,e,s)=>{$u(n.program,e,s)},[Yu,Ob,_u]=q(C["JsonSchema.extension"]),Ss=(n,e,s,t)=>{if(!Fu(t))t=hi(n.program,t);fi(n.program,e,s,t)};function hi(n,e){switch(typeof e){case"string":case"number":case"boolean":return e;case"object":if(e===null)return null;if(Array.isArray(e))return e.map((s)=>hi(n,s));if(Hu(e))return Mn(n,e,e.type);else{let s={};for(let[t,i]of Object.entries(e)){if(i===void 0)continue;s[t]=hi(n,i)}return s}default:return e}}function Fu(n){return typeof n==="object"&&n!==null&&mn(n)}function Hu(n){return"entityKind"in n&&n.entityKind==="Value"}function Cs(n,e){return Yu(n,e)??[]}function fi(n,e,s,t){let i=_u(n),h=i.has(e)?i.get(e):i.set(e,[]).get(e);if(Nu(t))h.push({key:s,value:Xe(t.properties.get("value").type,e)[0]});else h.push({key:s,value:t})}function Nu(n){return typeof n==="object"&&n!==null&&mn(n)&&n.kind==="Model"&&n.name==="Json"&&n.namespace?.name==="JsonSchema"}var ui=(n,e,s)=>{let[t,i]=Xe(s,e);if(i.length>0)n.program.reportDiagnostics(i)};ce("Private",ui);var ai={"TypeSpec.JsonSchema":{jsonSchema:Es,baseUri:Ze,id:_e,oneOf:Is,multipleOf:ks,contains:Ls,minContains:Ws,maxContains:zs,uniqueItems:Qs,minProperties:Ps,maxProperties:Zs,contentEncoding:Ys,prefixItems:Ds,contentMediaType:Fs,contentSchema:Ns,extension:Ss},"TypeSpec.JsonSchema.Private":{validatesRawJson:ui}};var wi={};an(wi,{$lib:()=>Fe,$decorators:()=>yu});var $b=Symbol("$scalar"),Yb=Symbol("$ref"),_b=Symbol("$map");var qn;(function(n){n[n.Duplex=3]="Duplex",n[n.In=2]="In",n[n.Out=1]="Out",n[n.None=0]="None"})(qn||(qn={}));var ju={type:"object",additionalProperties:!1,properties:{noEmit:{type:"boolean",nullable:!0,description:"If set to `true`, this emitter will not write any files. It will still validate the TypeSpec sources to ensure they are compatible with Protobuf, but the files will simply not be written to the output directory."},"omit-unreachable-types":{type:"boolean",nullable:!0,description:"By default, the emitter will create `message` declarations for any models in a namespace decorated with `@package` that have an `@field` decorator on every property. If this option is set to true, this behavior will be disabled, and only messages that are explicitly decorated with `@message` or that are reachable from a service operation will be emitted."}},required:[]},yh="@typespec/protobuf",Fe=F({name:yh,capabilities:{dryRun:!0},requireImports:[yh],diagnostics:{"field-index":{severity:"error",messages:{missing:o`field ${"name"} does not have a field index, but one is required (try using the '@field' decorator)`,invalid:o`field index ${"index"} is invalid (must be an integer greater than zero)`,"out-of-bounds":o`field index ${"index"} is out of bounds (must be less than ${"max"})`,reserved:o`field index ${"index"} falls within the implementation-reserved range of 19000-19999 inclusive`,"user-reserved":o`field index ${"index"} was reserved by a call to @reserve on this model`,"user-reserved-range":o`field index ${"index"} falls within a range reserved by a call to @reserve on this model`}},"field-name":{severity:"error",messages:{"user-reserved":o`field name '${"name"}' was reserved by a call to @reserve on this model`}},"root-operation":{severity:"error",messages:{default:"operations in the root namespace are not supported (no associated Protobuf service)"}},"unsupported-intrinsic":{severity:"error",messages:{default:o`intrinsic type ${"name"} is not supported in Protobuf`}},"unsupported-return-type":{severity:"error",messages:{default:"Protobuf methods must return a named Model"}},"unsupported-input-type":{severity:"error",messages:{"wrong-number":"Protobuf methods must accept exactly one Model input (an empty model will do)","wrong-type":"Protobuf methods may only accept a named Model as an input",unconvertible:"input parameters cannot be converted to a Protobuf message"}},"unsupported-field-type":{severity:"error",messages:{unconvertible:o`cannot convert a ${"type"} to a protobuf type (only intrinsic types and models are supported)`,"unknown-intrinsic":o`no known protobuf scalar for intrinsic type ${"name"}`,"unknown-scalar":o`no known protobuf scalar for TypeSpec scalar type ${"name"}`,"recursive-map":"a protobuf map's 'value' type may not refer to another map",union:"a message field's type may not be a union"}},"optional-array-field":{severity:"warning",messages:{default:"optional array fields cannot preserve unset versus empty in protobuf; emitting a repeated field without the 'optional' label"}},"optional-map-field":{severity:"warning",messages:{default:"optional map fields cannot preserve unset versus empty in protobuf; emitting a map field without the 'optional' label"}},"namespace-collision":{severity:"error",messages:{default:o`the package name ${"name"} has already been used`}},"unconvertible-enum":{severity:"error",messages:{default:"enums must explicitly assign exactly one integer to each member to be used in a Protobuf message","no-zero-first":"the first variant of an enum must be set to zero to be used in a Protobuf message"}},"nested-array":{severity:"error",messages:{default:"nested arrays are not supported by the Protobuf emitter"}},"invalid-package-name":{severity:"error",messages:{default:o`${"name"} is not a valid package name (must consist of letters and numbers separated by ".")`}},"illegal-reservation":{severity:"error",messages:{default:"reservation value must be a string literal, uint32 literal, or a tuple of two uint32 literals denoting a range"}},"model-not-in-package":{severity:"error",messages:{default:o`model ${"name"} is not in a namespace that uses the '@Protobuf.package' decorator`}},"anonymous-model":{severity:"error",messages:{default:"anonymous models cannot be used in Protobuf messages"}},"unspeakable-template-argument":{severity:"error",messages:{default:o`template ${"name"} cannot be converted to a Protobuf message because it has an unspeakable argument (try using the '@friendlyName' decorator on the template)`}},package:{severity:"error",messages:{"disallowed-option-type":o`option '${"name"}' with type '${"type"}' is not allowed in a package declaration (only string, boolean, and numeric types are allowed)`}}},emitter:{options:ju}}),ph=new WeakMap;function Du(n){let e=ph.get(n);if(!e)e=new Map,ph.set(n,e);return e}function Su(n,e){let s=Du(n),t=s.get(e);if(!t)t=new Set,s.set(e,t);return t}var He=Object.assign(Fe.reportDiagnostic,{once:function(n,e){let s=Su(n,e.target);if(s.has(e.code))return;s.add(e.code),Fe.reportDiagnostic(n,e)}}),Cu=["fieldIndex","package","service","externRef","stream","reserve","message","_map"],kn=Object.fromEntries(Cu.map((n)=>[n,Fe.createStateSymbol(n)]));var gh=536870911,nd=[19000,19999];function ed(n,e){n.program.stateSet(kn.service).add(e)}var sd=(n,e,s)=>{n.program.stateMap(kn.package).set(e,s)};function td(n,e){n.program.stateSet(kn._map).add(e)}var id=(n,e,s,t)=>{n.program.stateMap(kn.externRef).set(e,[s.value,t.value])},hd=(n,e,s)=>{let t={Duplex:qn.Duplex,In:qn.In,Out:qn.Out,None:qn.None}[s.name];n.program.stateMap(kn.stream).set(e,t)},dd=(n,e,...s)=>{let t=s.filter((i)=>i!=null);n.program.stateMap(kn.reserve).set(e,t)},ld=(n,e)=>{n.program.stateSet(kn.message).add(e)},oi=(n,e,s)=>{if(!Number.isInteger(s)||s<=0){He(n.program,{code:"field-index",messageId:"invalid",format:{index:String(s)},target:e});return}else if(s>gh){He(n.program,{code:"field-index",messageId:"out-of-bounds",format:{index:String(s),max:String(gh+1)},target:e});return}else if(s>=nd[0]&&s<=nd[1])He(n.program,{code:"field-index",messageId:"reserved",format:{index:String(s)},target:e});n.program.stateMap(kn.fieldIndex).set(e,s)};var yu={"TypeSpec.Protobuf":{message:ld,field:oi,reserve:dd,service:ed,package:sd,stream:hd},"TypeSpec.Protobuf.Private":{externRef:id,_map:td}};var Ri={};an(Ri,{$lib:()=>bi,$decorators:()=>ea});var fd={type:"string",enum:["parent-container","fqn","explicit-only"],default:"parent-container",description:["Determines how to generate operation IDs when `@operationId` is not used.","Avaliable options are:"," - `parent-container`: Uses the parent namespace and operation name to generate the ID."," - `fqn`: Uses the fully qualified name of the operation to generate the ID."," - `explicit-only`: Only use explicitly defined operation IDs."].join(`
`)},pu={type:"object",additionalProperties:!1,properties:{"file-type":{type:["string","array"],nullable:!0,oneOf:[{type:"string",enum:["yaml","json"]},{type:"array",items:{type:"string",enum:["yaml","json"]},uniqueItems:!0,minItems:1}],description:"If the content should be serialized as YAML or JSON. Can be a single value or an array to emit multiple formats. Default 'yaml', if not specified infer from the `output-file` extension"},"output-file":{type:"string",nullable:!0,description:["Name of the output file."," Output file will interpolate the following values:","  - service-name: Name of the service","  - service-name-if-multiple: Name of the service if multiple","  - version: Version of the service if multiple","  - file-type: The file type being emitted (json or yaml). Useful when `file-type` is an array.","",' Default: `{service-name-if-multiple}.{version}.openapi.yaml` or `.json` if `file-type` is `"json"`'," When `file-type` is an array: `{service-name-if-multiple}.{version}.openapi.{file-type}`",""," Example Single service no versioning","  - `openapi.yaml`",""," Example Multiple services no versioning","  - `openapi.Org1.Service1.yaml`","  - `openapi.Org1.Service2.yaml`",""," Example Single service with versioning","  - `openapi.v1.yaml`","  - `openapi.v2.yaml`",""," Example Multiple service with versioning","  - `openapi.Org1.Service1.v1.yaml`","  - `openapi.Org1.Service1.v2.yaml`","  - `openapi.Org1.Service2.v1.0.yaml`","  - `openapi.Org1.Service2.v1.1.yaml`    "].join(`
`)},"openapi-versions":{title:"OpenAPI Versions",type:"array",items:{type:"string",enum:["3.0.0","3.1.0","3.2.0"],nullable:!0,description:"The versions of OpenAPI to emit. Defaults to `[3.0.0]`"},nullable:!0,uniqueItems:!0,minItems:1,default:["3.0.0"]},"new-line":{type:"string",enum:["crlf","lf"],default:"lf",nullable:!0,description:"Set the newline character for emitting files."},"omit-unreachable-types":{type:"boolean",nullable:!0,description:`Omit unreachable types.
By default all types declared under the service namespace will be included. With this flag on only types references in an operation will be emitted.`},"include-x-typespec-name":{type:"string",enum:["inline-only","never"],nullable:!0,default:"never",description:"If the generated openapi types should have the `x-typespec-name` extension set with the name of the TypeSpec type that created it.\nThis extension is meant for debugging and should not be depended on."},"safeint-strategy":{type:"string",enum:["double-int","int64"],nullable:!0,default:"int64",description:["How to handle safeint type. Options are:"," - `double-int`: Will produce `type: integer, format: double-int`"," - `int64`: Will produce `type: integer, format: int64`","","Default: `int64`"].join(`
`)},"seal-object-schemas":{type:"boolean",nullable:!0,default:!1,description:["If true, then for models emitted as object schemas we default `additionalProperties` to false for","OpenAPI 3.0, and `unevaluatedProperties` to false for OpenAPI 3.1, if not explicitly specified elsewhere.","Default: `false`"].join(`
`)},"experimental-parameter-examples":{type:"string",enum:["data","serialized"],nullable:!0,description:["Determines how to emit examples on parameters.","Note: This is an experimental feature and may change in future versions.","See https://spec.openapis.org/oas/v3.0.4.html#style-examples for parameter example serialization rules","See https://github.com/OAI/OpenAPI-Specification/discussions/4622 for discussion on handling parameter examples."].join(`
`)},"operation-id-strategy":{oneOf:[fd,{type:"object",properties:{kind:fd,separator:{type:"string",nullable:!0,description:"Separator used to join segment in the operation name."}},required:["kind"]}]},"enum-strategy":{type:"string",enum:["default","annotated"],nullable:!0,default:"default",description:["How to emit TypeSpec enums and unions of literals. Options are:"," - `default`: Emit as a single schema using the `enum` keyword."," - `annotated`: Emit as a `oneOf` of `const` subschemas annotated with `title` and `description`","   from each member's/variant's `@summary` and `@doc`. Follows the OpenAPI 3.1.1 annotated enumerations pattern.","   Only supported by OpenAPI 3.1.0 and above; on 3.0.0 the `default` style is used and a warning is reported."].join(`
`)}},required:[]},bi=F({name:"@typespec/openapi3",capabilities:{dryRun:!0},diagnostics:{"oneof-union":{severity:"error",messages:{default:"@oneOf decorator can only be used on a union or a model property which type is a union."}},"inconsistent-shared-route-request-visibility":{severity:"error",messages:{default:"All operations with `@sharedRoutes` must have the same `@requestVisibility`."}},"invalid-server-variable":{severity:"error",docs:Wn.fromPackageRoot("src/diagnostics/invalid-server-variable.md"),messages:{default:o`Server variable '${"propName"}' must be assignable to 'string'. It must either be a string, enum of string or union of strings.`}},"invalid-format":{severity:"warning",messages:{default:o`Collection format '${"value"}' is not supported in OpenAPI3 ${"paramType"} parameters. Defaulting to type 'string'.`}},"invalid-style":{severity:"warning",messages:{default:o`Style '${"style"}' is not supported in OpenAPI3 ${"paramType"} parameters. Defaulting to style 'simple'.`,optionalPath:o`Style '${"style"}' is not supported in OpenAPI3 ${"paramType"} parameters. The style ${"style"} could be introduced by an optional parameter. Defaulting to style 'simple'.`}},"path-reserved-expansion":{severity:"warning",messages:{default:"Reserved expansion of path parameter with '+' operator #{allowReserved: true} is not supported in OpenAPI3."}},"resource-namespace":{severity:"error",messages:{default:"Resource goes on namespace"}},"path-query":{severity:"error",docs:Wn.fromPackageRoot("src/diagnostics/path-query.md"),messages:{default:"OpenAPI does not allow paths containing a query string."}},"duplicate-header":{severity:"error",docs:Wn.fromPackageRoot("src/diagnostics/duplicate-header.md"),messages:{default:o`The header ${"header"} is defined across multiple content types`}},"status-code-in-default-response":{severity:"error",messages:{default:"a default response should not have an explicit status code"}},"invalid-schema":{severity:"error",docs:Wn.fromPackageRoot("src/diagnostics/invalid-schema.md"),messages:{default:o`Couldn't get schema for type ${"type"}`}},"union-null":{severity:"error",docs:Wn.fromPackageRoot("src/diagnostics/union-null.md"),messages:{default:"Cannot have a union containing only null types."}},"empty-union":{severity:"error",messages:{default:"Empty unions are not supported for OpenAPI v3 - enums must have at least one value."}},"empty-enum":{severity:"error",messages:{default:"Empty enums are not supported for OpenAPI v3 - enums must have at least one value."}},"enum-unique-type":{severity:"error",messages:{default:"Enums are not supported unless all options are literals of the same type."}},"inline-cycle":{severity:"error",docs:Wn.fromPackageRoot("src/diagnostics/inline-cycle.md"),messages:{default:o`Cycle detected in '${"type"}'. Use @friendlyName decorator to assign an OpenAPI definition name and make it non-inline.`}},"unsupported-status-code-range":{severity:"error",messages:{default:o`Status code range '${"start"} to '${"end"}' is not supported. OpenAPI 3.0 can only represent range 1XX, 2XX, 3XX, 4XX and 5XX. Example: \`@minValue(400) @maxValue(499)\` for 4XX.`}},"invalid-model-property":{severity:"error",messages:{default:o`'${"type"}' cannot be specified as a model property.`}},"unsupported-auth":{severity:"warning",messages:{default:o`Authentication "${"authType"}" is not a known authentication by the openapi3 emitter, it will be ignored.`}},"xml-attribute-invalid-property-type":{severity:"warning",messages:{default:o`XML \`@attribute\` can only be primitive types in the OpenAPI 3 emitter, Property '${"name"}' type will be changed to type: string.`}},"xml-unwrapped-invalid-property-type":{severity:"warning",messages:{default:o`XML \`@unwrapped\` can only used on array properties or primitive ones in the OpenAPI 3 emitter, Property '${"name"}' will be ignored.`}},"invalid-component-fixed-field-key":{severity:"warning",messages:{default:o`Invalid key '${"value"}' used in a fixed field of the Component object. Only alphanumerics, dot (.), hyphen (-), and underscore (_) characters are allowed in keys.`}},"streams-not-supported":{severity:"warning",messages:{default:"Streams with itemSchema are only fully supported in OpenAPI 3.2.0 or above. The response will be emitted without itemSchema. Consider using OpenAPI 3.2.0 for full stream support."}},"default-not-supported":{severity:"warning",messages:{default:o`Default value is not supported in OpenAPI 3.0 ${"message"}`}},"enum-strategy-not-supported":{severity:"warning",messages:{default:"`enum-strategy: annotated` is only supported for OpenAPI 3.1.0 and above. The default enum strategy will be used for OpenAPI 3.0.0."}}},emitter:{options:pu}}),{createDiagnostic:xr,reportDiagnostic:ud,createStateSymbol:ri}=bi;var gu=ri("refs"),ad=(n,e,s)=>{n.program.stateMap(gu).set(e,s)};var na=ri("oneOf"),cd=(n,e)=>{if(e.kind==="ModelProperty"&&e.type.kind!=="Union")ud(n.program,{code:"oneof-union",target:n.decoratorTarget});n.program.stateMap(na).set(e,!0)};var ea={"TypeSpec.OpenAPI":{useRef:ad,oneOf:cd}};var th={};an(th,{$onValidate:()=>vl,$lib:()=>xi,$decorators:()=>Wc});var xi=F({name:"@typespec/sse",diagnostics:{"terminal-event-not-in-events":{severity:"error",messages:{default:"A field marked as '@terminalEvent' must be a member of a type decorated with '@TypeSpec.Events.events'."}},"sse-stream-union-not-events":{severity:"error",messages:{default:"SSEStream type parameter must be a union decorated with '@TypeSpec.Events.events'."}}},state:{terminalEvent:{description:"State for the @terminalEvent decorator."}}}),{reportDiagnostic:Ms,createDiagnostic:Ir,stateKeys:Bs}=xi;var[Wr,sa]=un(Bs.terminalEvent),od=(n,e)=>{sa(n.program,e)};var Ks=F({name:"@typespec/events",diagnostics:{"invalid-content-type-target":{severity:"error",messages:{default:"@contentType can only be specified on the top-level event envelope, or the event payload marked with @data"}},"multiple-event-payloads":{severity:"error",messages:{default:o`Event payload already applied to ${"dataPath"} but also exists under ${"currentPath"}`,payloadInIndexedModel:o`Event payload applied from inside a Record or Array at ${"dataPath"}`}}},state:{events:{description:"State for the @events decorator."},contentType:{description:"State for the @contentType decorator."},data:{description:"State for the @data decorator."}}}),{reportDiagnostic:Jr,createDiagnostic:we,stateKeys:be}=Ks;var vi={};an(vi,{$onValidate:()=>Ei,$lib:()=>Ks,$decorators:()=>Rd});var[Vs,ta]=un(be.events),wd=(n,e)=>{ta(n.program,e)};var[Ne,ia]=q(be.contentType),bd=(n,e,s)=>{ia(n.program,e,s)};var[ys,ha]=un(be.data),rd=(n,e)=>{ha(n.program,e)};function da(n,e){if(!Ne(n,e)||ys(n,e))return;return we({code:"invalid-content-type-target",target:e})}function je(n){return n.map((e)=>{switch(e.kind){case"ModelProperty":return e.name;case"Tuple":return"[]";case"Model":return}}).filter(Boolean).join(".")}function la(n,e){let s=L(),t=typeof e.name==="string"?e.name:void 0,i=Ne(n,e),h,d,f="",l=0,u=[],a=new Set;re(e.type,{modelProperty(b){u.push(b);let r=da(n,b);if(r)s.add(r);if(a.has(b.type)){s.add(we({code:"multiple-event-payloads",format:{dataPath:f,currentPath:je(u)},target:e}));return}if(!ys(n,b))return;if(u.forEach((m)=>{if(m.kind==="ModelProperty")a.add(m.type);else if(m.kind==="Model"||m.kind==="Tuple")a.add(m)}),l>0){s.add(we({code:"multiple-event-payloads",messageId:"payloadInIndexedModel",format:{dataPath:je(u.slice(0,-1))},target:e}));return}if(h){s.add(we({code:"multiple-event-payloads",format:{dataPath:f,currentPath:je(u)},target:e}));return}h=b.type,d=Ne(n,b),f=je(u)},exitModelProperty(){u.pop()},tuple(b){u.push(b),b.values.forEach((r)=>{if(a.has(r)){s.add(we({code:"multiple-event-payloads",format:{dataPath:f,currentPath:je(u)},target:e}));return}})},exitTuple(){u.pop()},model(b){if(b.indexer)l++;u.push(b)},exitModel(b){if(b.indexer)l--;u.pop()}},{});let c={eventType:t,root:e,isEventEnvelope:!!h,type:e.type,contentType:i,payloadType:h??e.type,payloadContentType:h?d:i};return s.wrap(c)}function mi(n,e){let s=L(),t=[];return e.variants.forEach((i)=>{t.push(s.pipe(la(n,i)))}),s.wrap(t)}function Ei(n){fa(n)}function fa(n){n.stateSet(be.events).forEach((e)=>{let[,s]=mi(n,e);n.reportDiagnostics(s)})}var Rd={"TypeSpec.Events":{contentType:bd,data:rd,events:wd}};var ps=F({name:"@typespec/http",diagnostics:{"http-verb-duplicate":{severity:"error",messages:{default:o`HTTP verb already applied to ${"entityName"}`}},"missing-uri-param":{severity:"error",messages:{default:o`Route reference parameter '${"param"}' but wasn't found in operation parameters`}},"incompatible-uri-param":{severity:"error",messages:{default:o`Parameter '${"param"}' is defined in the uri as a ${"uriKind"} but is annotated as a ${"annotationKind"}.`}},"use-uri-template":{severity:"error",messages:{default:o`Parameter '${"param"}' is already defined in the uri template. Explode, style and allowReserved property must be defined in the uri template as described by RFC 6570.`}},"double-slash":{severity:"warning",messages:{default:o`Route will result in duplicate slashes as parameter '${"paramName"}' use path expansion and is prefixed with a /`,optionalUnset:o`Route will result in duplicate slashes when optional parameter '${"paramName"}' is not set.`,optionalSet:o`Route will result in duplicate slashes when optional parameter '${"paramName"}' is set.`}},"missing-server-param":{severity:"error",messages:{default:o`Server url contains parameter '${"param"}' but wasn't found in given parameters`}},"duplicate-body":{severity:"error",messages:{default:"Operation has multiple @body parameters declared",duplicateUnannotated:"Operation has multiple unannotated parameters. There can only be one representing the body",bodyAndUnannotated:"Operation has a @body and an unannotated parameter. There can only be one representing the body"}},"duplicate-route-decorator":{severity:"error",messages:{namespace:"@route was defined twice on this namespace and has different values."}},"operation-param-duplicate-type":{severity:"error",messages:{default:o`Param ${"paramName"} has multiple types: [${"types"}]`}},"duplicate-operation":{severity:"error",messages:{default:o`Duplicate operation "${"operationName"}" routed at "${"verb"} ${"path"}".`}},"multiple-status-codes":{severity:"error",messages:{default:"Multiple `@statusCode` decorators defined for this operation response."}},"status-code-invalid":{severity:"error",messages:{default:"statusCode value must be a numeric or string literal or union of numeric or string literals",value:"statusCode value must be a three digit code between 100 and 599"}},"content-type-string":{severity:"error",messages:{default:"contentType parameter must be a string literal or union of string literals"}},"content-type-ignored":{severity:"warning",messages:{default:"`Content-Type` header ignored because there is no body."}},"metadata-ignored":{severity:"warning",messages:{default:o`${"kind"} property will be ignored as it is inside of a @body property. Use @bodyRoot instead if wanting to mix.`}},"response-cookie-not-supported":{severity:"warning",messages:{default:o`@cookie on response is not supported. Property '${"propName"}' will be ignored in the body. If you need 'Set-Cookie', use @header instead.`}},"no-service-found":{severity:"warning",messages:{default:o`No namespace with '@service' was found, but Namespace '${"namespace"}' contains routes. Did you mean to annotate this with '@service'?`}},"invalid-type-for-auth":{severity:"error",messages:{default:o`@useAuth ${"kind"} only accept Auth model, Tuple of auth model or union of auth model.`}},"shared-inconsistency":{severity:"error",messages:{default:o`Each operation routed at "${"verb"} ${"path"}" needs to have the @sharedRoute decorator.`}},"multipart-invalid-content-type":{severity:"error",messages:{default:o`Content type '${"contentType"}' is not a multipart content type. Supported content types are: ${"supportedContentTypes"}.`}},"multipart-model":{severity:"error",messages:{default:"Multipart request body must be a model or a tuple of http parts."}},"no-implicit-multipart":{severity:"error",messages:{default:"Using multipart payloads requires the use of @multipartBody and HttpPart<T> models."}},"multipart-part":{severity:"error",messages:{default:"Expect item to be an HttpPart model."}},"multipart-nested":{severity:"error",messages:{default:"Cannot use @multipartBody inside of an HttpPart"}},"http-file-extra-property":{severity:"error",messages:{default:o`File model cannot define extra properties. Found '${"propName"}'.`}},"http-file-disallowed-metadata":{severity:"error",messages:{default:o`File model cannot define HTTP metadata type '${"metadataType"}' on property '${"propName"}'.`}},"formdata-no-part-name":{severity:"error",messages:{default:"Part used in multipart/form-data must have a name."}},"http-file-structured":{severity:"warning",messages:{default:o`HTTP File body is serialized as a structured model in '${"contentTypes"}' instead of being treated as the contents of a file because an explicit Content-Type header is defined. Override the \`contentType\` property of the file model to declare the internal media type of the file's contents, or suppress this warning if you intend to serialize the File as a model.`,union:"An HTTP File in a union is serialized as a structured model instead of being treated as the contents of a file. Declare a separate operation using `@sharedRoute` that has only the File model as the body type to treat it as a file, or suppress this warning if you intend to serialize the File as a model."}},"http-file-content-type-not-string":{severity:"error",messages:{default:o`The 'contentType' property of the file model must be 'TypeSpec.string', a string literal, or a union of string literals. Found '${"type"}'.`}},"http-file-contents-not-scalar":{severity:"error",messages:{default:o`The 'contents' property of the file model must be a scalar type that extends 'string' or 'bytes'. Found '${"type"}'.`}},"deprecated-implicit-optionality":{severity:"warning",messages:{default:"The implicitOptionality option is deprecated. To preserve previous behavior, use an explicit patch model with optional properties. For actual merge-patch semantics, use MergePatchUpdate<T> for the @body type."}},"merge-patch-contains-null":{severity:"error",messages:{default:"Cannot convert model to a merge-patch compatible shape because it contains the 'null' intrinsic type."}},"merge-patch-content-type":{severity:"warning",messages:{default:o`The content-type of a request using a merge-patch template should be 'application/merge-patch+json' detected a header with content-type '${"contentType"}'.`}},"merge-patch-contains-metadata":{severity:"error",messages:{default:o`The MergePatch transform does not operate on http envelope metadata.  Remove any http metadata decorators ('@query', '@header', '@path', '@cookie', '@statusCode') from the model passed to the MergePatch template. Found '${"metadataType"}' decorating property '${"propertyName"}'`}}},state:{authentication:{description:"State for the @auth decorator"},header:{description:"State for the @header decorator"},cookie:{description:"State for the @cookie decorator"},query:{description:"State for the @query decorator"},path:{description:"State for the @path decorator"},body:{description:"State for the @body decorator"},bodyRoot:{description:"State for the @bodyRoot decorator"},bodyIgnore:{description:"State for the @bodyIgnore decorator"},multipartBody:{description:"State for the @bodyIgnore decorator"},statusCode:{description:"State for the @statusCode decorator"},verbs:{description:"State for the verb decorators (@get, @post, @put, etc.)"},patchOptions:{description:"State for the options of the @patch decorator"},servers:{description:"State for the @server decorator"},includeInapplicableMetadataInPayload:{description:"State for the @includeInapplicableMetadataInPayload decorator"},externalInterfaces:{},routeProducer:{},routes:{},sharedRoutes:{description:"State for the @sharedRoute decorator"},routeOptions:{},file:{description:"State for the @Private.file decorator"},httpPart:{description:"State for the @Private.httpPart decorator"},mergePatchModel:{description:"State marking mergePatch models "},mergePatchProperty:{description:"State marking merge path model property source"},mergePatchPropertyOptions:{description:"Override options for a property in a merge patch transform"}}}),{reportDiagnostic:Y,createDiagnostic:W,stateKeys:R}=ps;function Re(n){return[[],[W({code:"status-code-invalid",target:n,messageId:"value"})]]}function ua(n,e){let s=typeof n==="string"?parseInt(n,10):n;if(isNaN(s))return Re(e);if(!Number.isInteger(s))return Re(e);if(s<100||s>599)return Re(e);return[[s],[]]}function gs(n,e,s){switch(e.kind){case"String":case"Number":return ua(e.value,s);case"Union":let t=L(),i=[...e.variants.values()].flatMap((h)=>{return t.pipe(gs(n,h.type,s))});return t.wrap(i);case"Scalar":return xd(n,e,e,s);case"ModelProperty":if(e.type.kind==="Scalar")return xd(n,e,e.type,s);else return gs(n,e.type,s);default:return Re(s)}}function xd(n,e,s,t){if(!ca(n,s))return Re(t);let i=Ti(n,e,t);if(aa(i))return[[i],[]];else return Re(t)}function aa(n){return n.start!==void 0&&n.end!==void 0}function Ti(n,e,s){let t=nt(n,e),i=et(n,e),h={};if(e.kind==="ModelProperty"&&(e.type.kind==="Scalar"||e.type.kind==="ModelProperty"))h=Ti(n,e.type,s);else if(e.kind==="Scalar"&&e.baseScalar)h=Ti(n,e.baseScalar,s);return{...h,start:t,end:i}}function ca(n,e){let s=D(n);return s.type.isAssignableTo(e,s.builtin.int32,e)}function md(n){return n.match(/\{[^}]+\}/g)?.map((e)=>e.slice(1,-1))??[]}var ki=(n,e,s)=>{let t={type:"header",name:e.name.replace(/([a-z])([A-Z])/g,"$1-$2").toLowerCase()};if(s)if(typeof s==="string")t.name=s;else{let i=s.name;if(i)t.name=i;if(s.explode)t.explode=!0}n.program.stateMap(R.header).set(e,t)};function De(n,e){return n.stateMap(R.header).get(e)}function Kn(n,e){return n.stateMap(R.header).has(e)}var Ui=(n,e,s)=>{let i={type:"cookie",name:typeof s==="string"?s:s?.name??e.name.replace(/([a-z])([A-Z])/g,"$1_$2").toLowerCase()};n.program.stateMap(R.cookie).set(e,i)};function xe(n,e){return n.stateMap(R.cookie).get(e)}function Vn(n,e){return n.stateMap(R.cookie).has(e)}var[me,oa]=q(R.query),Ii=(n,e,s)=>{let t=typeof s==="string"?s:s?.name??e.name,i=typeof s==="object"?s:{};oa(n.program,e,{explode:i.explode,name:t})};function vd(n){return{explode:n.explode??!1,name:n.name}}function yn(n,e){return n.stateMap(R.query).has(e)}var[pn,wa]=q(R.path),Ee=(n,e,s)=>{let t=typeof s==="string"?s:s?.name??e.name,i=typeof s==="object"?s:{};wa(n.program,e,{explode:i.explode,allowReserved:i.allowReserved,style:i.style,name:t})};function Td(n){return{explode:n.explode??!1,allowReserved:n.allowReserved??!1,style:n.style??"simple",name:n.name}}function gn(n,e){return n.stateMap(R.path).has(e)}var Ai=(n,e)=>{n.program.stateSet(R.body).add(e)},Li=(n,e)=>{n.program.stateSet(R.bodyRoot).add(e)},qi=(n,e)=>{n.program.stateSet(R.bodyIgnore).add(e)};function ne(n,e){return n.stateSet(R.body).has(e)}function ee(n,e){return n.stateSet(R.bodyRoot).has(e)}function st(n,e){return n.stateSet(R.bodyIgnore).has(e)}var Wi=(n,e)=>{n.program.stateSet(R.multipartBody).add(e)};function se(n,e){return n.stateSet(R.multipartBody).has(e)}var Gi=(n,e)=>{n.program.stateSet(R.statusCode).add(e)};function zi(n,e,s){n.stateMap(R.statusCode).set(e,s)}function hn(n,e){return n.stateSet(R.statusCode).has(e)}function Ji(n,e){return gs(n,e,e)}function Qi(n){if(typeof n==="object")return Ed(n.start,n.end);let e=typeof n==="string"?parseInt(n,10):n;switch(e){case 200:return"The request has succeeded.";case 201:return"The request has succeeded and a new resource has been created as a result.";case 202:return"The request has been accepted for processing, but processing has not yet completed.";case 204:return"There is no content to send for this request, but the headers may be useful. ";case 301:return"The URL of the requested resource has been changed permanently. The new URL is given in the response.";case 304:return"The client has made a conditional request and the resource has not been modified.";case 400:return"The server could not understand the request due to invalid syntax.";case 401:return"Access is unauthorized.";case 403:return"Access is forbidden.";case 404:return"The server cannot find the requested resource.";case 409:return"The request conflicts with the current state of the server.";case 412:return"Precondition failed.";case 503:return"Service unavailable."}return Ed(e,e)}function Ed(n,e){if(n>=100&&e<=199)return"Informational";else if(n>=200&&e<=299)return"Successful";else if(n>=300&&e<=399)return"Redirection";else if(n>=400&&e<=499)return"Client error";else if(n>=500&&e<=599)return"Server error";return}function ba(n,e,s){ra(n,e),n.program.stateMap(R.verbs).set(e,s)}function ra(n,e){if(e.decorators.filter((t)=>va.includes(t.decorator)&&t.node?.kind===K.DecoratorExpression&&t.node?.parent===e.node).length>1)return Y(n.program,{code:"http-verb-duplicate",format:{entityName:e.name},target:n.decoratorTarget}),!1;return!0}function Gn(n,e){return n.stateMap(R.verbs).get(e)}function ve(n){return(e,s)=>{ba(e,s,n)}}var tt=ve("get"),it=ve("put"),ht=ve("post"),dt=ve("delete"),lt=ve("head"),Ra=ve("patch"),[xa,ma]=q(R.patchOptions);function Ea(n,e){let s=n.decoratorTarget;if(e.interface!==void 0&&e.node!==void 0&&e.node.parent!==e.interface.node)return!1;if(e.sourceOperation!==void 0)return!1;if(s?.kind===K.DecoratorExpression&&s.parent){if(s.parent!==e.node)return!1}return!0}var ft=(n,e,s)=>{if(Ra(n,e),s){if(s.implicitOptionality===!0){if(Ea(n,e))Y(n.program,{code:"deprecated-implicit-optionality",target:e})}ma(n.program,e,s)}};function Oi(n,e){return xa(n,e)}var va=[tt,lt,ht,it,ft,dt],Pi=(n,e,s,t,i)=>{let h=md(s),d=new Map(i?.properties??[]);for(let l of h)if(!d.get(l))Y(n.program,{code:"missing-server-param",format:{param:l},target:n.getArgumentTarget(0)}),d.delete(l);let f=n.program.stateMap(R.servers).get(e);if(f===void 0)f=[],n.program.stateMap(R.servers).set(e,f);f.push({url:s,description:t,parameters:d})};function ut(n,e,s){Tn(n,e,ut);let[t,i]=Ta(n.program,s);if(i.length>0)n.program.reportDiagnostics(i);if(t!==void 0)kd(n.program,e,t)}function kd(n,e,s){n.stateMap(R.authentication).set(e,s)}function Ta(n,e){let s=L();switch(e.kind){case"Model":let t=s.pipe(Xi(n,e,e));if(t===void 0)return s.wrap(void 0);return s.wrap({options:[{schemes:[t]}]});case"Tuple":let i=s.pipe(Ud(n,e,e));return s.wrap({options:[i]});case"Union":return ka(n,e,e);default:return[void 0,[W({code:"invalid-type-for-auth",format:{kind:e.kind},target:e})]]}}function ka(n,e,s){let t=[],i=L();for(let h of e.variants.values()){let d=h.type;switch(d.kind){case"Model":let f=i.pipe(Xi(n,d,s));if(f!==void 0)t.push({schemes:[f]});break;case"Tuple":let l=i.pipe(Ud(n,d,s));t.push(l);break;default:i.add(W({code:"invalid-type-for-auth",format:{kind:d.kind},target:d}))}}return i.wrap({options:t})}function Ud(n,e,s){let t=[],i=L();for(let h of e.values)switch(h.kind){case"Model":let d=i.pipe(Xi(n,h,s));if(d!==void 0)t.push(d);break;default:i.add(W({code:"invalid-type-for-auth",format:{kind:h.kind},target:h}))}return i.wrap({schemes:t})}function Xi(n,e,s){let[t,i]=Xe(e,s);if(t===void 0)return[t,i];let h=Un(n,e);return[{...t.type==="oauth2"?Ua(e,t):{...t,...t.type==="openIdConnect"&&{scopes:Array.isArray(t.scopes)?t.scopes:[]},model:e},id:e.name||t.type,...h&&{description:h}},i]}function Ua(n,e){let s=Array.isArray(e.flows)&&e.flows.every((i)=>typeof i==="object")?e.flows:[],t=Array.isArray(e.defaultScopes)?e.defaultScopes:[];return{id:e.id,type:e.type,model:n,flows:s.map((i)=>{let h=i.scopes?i.scopes:t;return{...i,scopes:h.map((d)=>({value:d}))}})}}function te(n,e){return n.stateMap(R.authentication).get(e)}function Zi(n,e){let s=te(n,e);if(s)return s;if(e.interface!==void 0){let i=te(n,e.interface);if(i)return i}let t=e.namespace;while(t){let i=te(n,t);if(i)return i;t=t.namespace}return}function Rn(n){let e=L();if(n.type.kind==="String")return[[n.type.value],[]];else if(n.type.kind==="Union"){let s=[];for(let t of n.type.variants.values())if(t.type.kind==="String")s.push(t.type.value);else{e.add(W({code:"content-type-string",target:n}));continue}return e.wrap(s)}else if(n.type.kind==="Scalar"&&n.type.name==="string")return[["*/*"],[]];return[[],[W({code:"content-type-string",target:n})]]}var[Ia,Aa]=q(R.sharedRoutes);function at(n,e){Aa(n,e,!0)}function zn(n,e){return Ia(n,e)===!0}var $i=(n,e)=>{at(n.program,e)};var[La,qa]=q(R.routes),ct=(n,e,s)=>{Tn(n,e,ct),Id(n,e,{path:s,shared:!1})};function Id(n,e,s){let t=La(n.program,e);if(t&&e.kind==="Namespace"){if(t!==s.path)Y(n.program,{code:"duplicate-route-decorator",messageId:"namespace",target:e})}else if(qa(n.program,e,s.path),e.kind==="Operation"&&s.shared)at(n.program,e)}var Ad=(n,e)=>{let{program:s}=n,t=["$header","$body","$query","$path","$statusCode"],[i,h,d,f,l]=[s.stateMap(R.header),s.stateSet(R.body),s.stateMap(R.query),s.stateMap(R.path),s.stateMap(R.statusCode)];for(let u of e.properties.values())u.decorators=u.decorators.filter((a)=>!t.includes(a.decorator.name)),i.delete(u),h.delete(u),d.delete(u),f.delete(u),l.delete(u)};function Wa(n){if(n.sourceModels.length)return;let e=n.templateMapper;if(!e||!e.args)return;let[s,t]=e.args;if(!s||!t)return;return{contentTypeArg:s,contentsArg:t,templateMapper:n.templateMapper}}function ot(n){if(n?.entityKind==="Type")return A(n,{printable:!0});else if(n?.type.kind==="String")return`"${n.type.value}"`;return}var Ld=(n,e)=>{let s=e.sourceModels.length>0?e:void 0,t=Wa(e),i=t?.templateMapper,h=e.properties.get("contentType").type;if(!(h.kind==="String"||n.program.checker.isStdType(h,"string")||h.kind==="Union"&&[...h.variants.values()].every((l)=>l.type.kind==="String"))){let l=f(i?.source,"ContentType",0)??t?.contentTypeArg,u=t?.contentTypeArg,a=ot(u)??ot(h)??"<unknown>";Y(n.program,{code:"http-file-content-type-not-string",format:{type:a},target:l??s??Yi})}let d=e.properties.get("contents").type;if(d.kind!=="Scalar"){let l=t?.contentsArg,u=f(i?.source,"Contents",1)??l,a=ot(l)??ot(d)??"<unknown>";Y(n.program,{code:"http-file-contents-not-scalar",format:{type:a},target:u??s??Yi})}n.program.stateSet(R.file).add(e);function f(l,u,a){if(l?.node.kind===K.TypeReference){let c=l.node.arguments.find((m)=>m.name?.sv===u);if(c)return c;let r=e.templateNode?.templateParameters.map((m,I)=>[I,m]).find(([m,I])=>I.id.sv)?.[0]??a;if(r===void 0)return;return l.node.arguments[r]}return}return{onGraphFinish:()=>{return qd(n.program,e),[]}}};function qd(n,e){for(let t of e.properties.values())switch(t.name){case"contentType":case"contents":{let i={header:De(n,t),cookie:xe(n,t),query:me(n,t),path:pn(n,t),body:ne(n,t),bodyRoot:ee(n,t),multipartBody:se(n,t),statusCode:hn(n,t)};s(t,i);break}case"filename":{let i={body:ne(n,t),bodyRoot:ee(n,t),multipartBody:se(n,t),statusCode:hn(n,t),cookie:xe(n,t)};s(t,i);break}default:Y(n,{code:"http-file-extra-property",format:{propName:t.name},target:t})}for(let t of e.derivedModels)qd(n,t);function s(t,i){let h=Object.entries(i).filter((d)=>!!d[1]);for(let[d]of h)Y(n,{code:"http-file-disallowed-metadata",format:{propName:t.name,metadataType:d},target:t})}}function Wd(n,e){return n.stateSet(R.file).has(e)}function Gd(n,e){if(e.kind!=="Model")return!1;let s=e;while(s){if(Wd(n,s))return!0;s=s.baseModel}return!1}function Se(n,e,s){if(e.kind!=="Model")return;let t=Te(e,"contentType"),i=Te(e,"filename"),h=Te(e,"contents");if(!t||!i||!h)return;if(!wt(n,t)||!wt(n,i)||!wt(n,h))return;let d=new Set(bn(e));if(s){for(let f of d)if(!s(f))d.delete(f)}if(d.size!==3)return;return{contents:h,contentType:t,filename:i,type:e}}function wt(n,e){if(e.model?Gd(n,e.model):!1)return!0;if(e.sourceProperty)return wt(n,e.sourceProperty);return!1}var zd=(n,e,s,t)=>{n.program.stateMap(R.httpPart).set(e,{type:s,options:t})};function _i(n,e){return n.stateMap(R.httpPart).get(e)}function Jd(n,e,s){n.program.stateMap(R.includeInapplicableMetadataInPayload).set(e,s)}var O;(function(n){n[n.Read=1]="Read",n[n.Create=2]="Create",n[n.Update=4]="Update",n[n.Delete=8]="Delete",n[n.Query=16]="Query",n[n.None=0]="None",n[n.All=31]="All",n[n.Item=1048576]="Item",n[n.Patch=2097152]="Patch",n[n.Synthetic=3145728]="Synthetic"})(O||(O={}));function Ga(n,e){let s=Zn(n);if(U(!e.all,"Unexpected: `all` constraint in visibility filter passed to filterToVisibility"),U(!e.none,"Unexpected: `none` constraint in visibility filter passed to filterToVisibility"),!e.any)return O.All;else{let t=O.None;for(let i of e.any??[]){if(i.enum!==s)continue;switch(i.name){case"Read":t|=O.Read;break;case"Create":t|=O.Create;break;case"Update":t|=O.Update;break;case"Delete":t|=O.Delete;break;case"Query":t|=O.Query;break;default:U(!1,`Unreachable: unrecognized Lifecycle visibility member: '${i.name}'`)}}return t}}var Qd=new WeakMap;function za(n){let e=Qd.get(n);if(!e)e=new Map,Qd.set(n,e);return e}function Pd(n,e){if(e&=~O.Synthetic,e===O.All)return{};let s=za(n),t=s.get(e);if(!t){let i=Zn(n),h={Create:i.members.get("Create"),Read:i.members.get("Read"),Update:i.members.get("Update"),Delete:i.members.get("Delete"),Query:i.members.get("Query")},d=new Set;if(e&O.Read)d.add(h.Read);if(e&O.Create)d.add(h.Create);if(e&O.Update)d.add(h.Update);if(e&O.Delete)d.add(h.Delete);if(e&O.Query)d.add(h.Query);U(d.size>0||e===O.None,"invalid visibility"),t={any:d},s.set(e,t)}return t}function Ja(n){switch(n){case"get":case"head":return O.Query;case"post":return O.Create;case"put":return O.Create|O.Update;case"patch":return O.Update;case"delete":return O.Delete;default:U(!1,`Unreachable: unrecognized HTTP verb: '${n}'`)}}function Xd(n){let e=typeof n==="string";return{parameters:(s,t)=>{let i=e?n:n?.verbSelector?.(s,t)??Gn(s,t);if(!i){let[{parameters:h}]=rt(s,t,void 0,{});i=h.verb}return Pd(s,Ja(i))},returnType:(s,t)=>{let i=Zn(s).members.get("Read");return{any:new Set([i])}}}}function Fi(n,e,s){let t=Od(n,e,Xd(s)),i=Ga(n,t);if(s==="patch"){if(Oi(n,e)?.implicitOptionality)i|=O.Patch}return i}function Hi(n,e){return Kn(n,e)||Vn(n,e)||yn(n,e)||gn(n,e)||hn(n,e)}function Ni(n,e,s){return bt(n,e,Pd(n,s))}var[Zd,Ce]=q(R.mergePatchModel),[ji,Di]=q(R.mergePatchProperty),[xt,Si]=q(R.mergePatchPropertyOptions);function Ci(n,e){return Zd(n,e)!==void 0}function $d(n,e){let s=new WeakMap;function t(i){if(s.has(i))return!1;if(s.set(e,!0),!D(n).model.is(i))return!1;if(Ci(n,i))return!0;if(D(n).array.is(i)||D(n).record.is(i))return t(i.indexer.value);return!1}switch(e.kind){case"Model":return t(e)||e.sourceModels.some((i)=>t(i.model))||[...e.properties.values()].some((i)=>ji(n,i)!==void 0||t(i.type));case"ModelProperty":return t(e.type);case"Union":return[...e.variants.values()].some((i)=>t(i.type));case"UnionVariant":return t(e.type);case"Tuple":return e.values.some((i)=>t(i));default:return!1}}function Qa(n,e,s,t={}){let i=[];function h(u){return[{...u,property:e,path:s},i]}let d={header:De(n,e),cookie:xe(n,e),query:me(n,e),path:pn(n,e),body:ne(n,e),bodyRoot:ee(n,e),multipartBody:se(n,e),statusCode:hn(n,e)},f=Object.entries(d).filter((u)=>!!u[1]),l=t.implicitParameter?.(e);if(l&&f.length>0)if(l.type==="path"&&d.path){if(d.path.explode!==void 0||d.path.style!==void 0||d.path.allowReserved!==void 0)i.push(W({code:"use-uri-template",format:{param:e.name},target:e}))}else if(l.type==="query"&&d.query){if(d.query.explode!==void 0)i.push(W({code:"use-uri-template",format:{param:e.name},target:e}))}else i.push(W({code:"incompatible-uri-param",format:{param:e.name,uriKind:l.type,annotationKind:f[0][0]},target:e}));if(l)return h({kind:l.type,options:l,property:e});if(f.length===0)return st(n,e)?h({kind:"bodyIgnore"}):h({kind:"bodyProperty"});else if(f.length>1)i.push(W({code:"operation-param-duplicate-type",format:{paramName:e.name,types:f.map((u)=>u[0]).join(", ")},target:e}));if(d.header){if(d.header.name.toLowerCase()==="content-type"&&!t.treatContentTypeAsHeader)return h({kind:"contentType"});return h({kind:"header",options:d.header})}else if(d.cookie)return h({kind:"cookie",options:d.cookie});else if(d.query)return h({kind:"query",options:vd(d.query)});else if(d.path)return h({kind:"path",options:Td(d.path)});else if(d.statusCode)return h({kind:"statusCode"});else if(d.body)return h({kind:"body"});else if(d.bodyRoot)return h({kind:"bodyRoot"});else if(d.multipartBody)return h({kind:"multipartBody"});U(!1,"Unexpected http property type")}function Yd(n,e,s,t,i={}){let h=L(),d=new Map;if(e.kind!=="Model"||e.properties.size===0&&!e.baseModel)return h.wrap([]);let f=new Set;function l(u,a){f.add(u);let c=!1,b=!1;for(let r of bn(u)){let m=[...a,r.name];if(!Ni(n,r,s))continue;let I=h.pipe(Qa(n,r,m,i));if(I.kind!=="bodyIgnore"&&Pa(I,t))I={kind:"bodyProperty",property:r,path:m};if(I.kind==="cookie"&&t===N.Response){h.add(W({code:"response-cookie-not-supported",target:r,format:{propName:r.name}}));continue}if(I.kind==="body"||I.kind==="bodyRoot"||I.kind==="multipartBody")c=!0;if(!(I.kind==="body"||I.kind==="multipartBody")&&Oa(r.type)&&!f.has(r.type)){if(l(r.type,m)){c=!0;continue}}if(I.kind==="bodyProperty")b=!0;if(I.kind!=="bodyIgnore")d.set(r,I)}return c&&!b}return l(e,[]),h.wrap([...d.values()])}function Oa(n){return n.kind==="Model"&&!n.indexer&&n.properties.size>0}function Pa(n,e){switch(e){case N.Request:return n.kind==="statusCode";case N.Response:return n.kind==="query"||n.kind==="path";case N.Multipart:return n.kind==="path"||n.kind==="query"||n.kind==="statusCode";default:return!1}}var N;(function(n){n[n.Request=0]="Request",n[n.Response=1]="Response",n[n.Multipart=2]="Multipart"})(N||(N={}));function Ue(n,e,s,t,i={}){let h=L(),d=h.pipe(Yd(n,e,s,t,i)),f=h.pipe(Xa(n,e,d,s,t));if(f){if(f.contentTypes.some((l)=>l.startsWith("multipart/"))&&f.bodyKind!=="multipart")return h.add(W({code:"no-implicit-multipart",target:f.property??e})),h.wrap({body:void 0,metadata:d})}return h.wrap({body:f,metadata:d})}function Xa(n,e,s,t,i){let h=L(),d=s.find((a)=>a.kind==="contentType"),f=Se(n,e,Sd(s));if(f!==void 0)if(!d)return h.join(Cd(f));else{let a=d&&h.pipe(Rn(d.property));Y(n,{code:"http-file-structured",format:{contentTypes:a.join(", ")},target:d.property})}if(e.kind!=="Model"||Rt(e))return h.wrap({bodyKind:"single",...h.pipe(ie(n,d,e)),type:e,isExplicit:!1,containsMetadataAnnotations:!1});let l=h.pipe(Za(n,s,d,t,i));if(l===void 0){if(e.baseModel||e.indexer)return h.wrap({bodyKind:"single",...h.pipe(ie(n,d,e)),type:e,isExplicit:!1,containsMetadataAnnotations:!1});if(e.derivedModels.length>0&&En(n,e))return h.wrap({bodyKind:"single",...h.pipe(ie(n,d,e)),type:e,isExplicit:!1,containsMetadataAnnotations:!1})}let u=jd(n,e,(a)=>s.some((c)=>c.property===a&&c.kind==="bodyProperty"));if(u.properties.size>0)if(l===void 0)return h.wrap({bodyKind:"single",...h.pipe(ie(n,d,e)),type:u,isExplicit:!1,containsMetadataAnnotations:!1});else h.add(W({code:"duplicate-body",messageId:"bodyAndUnannotated",target:e}));if(l===void 0&&d)h.add(W({code:"content-type-ignored",target:d.property}));return h.wrap(l)}function Za(n,e,s,t,i){let h=L(),d,f=new ke;for(let l of e){if(l.kind==="body"||l.kind==="bodyRoot"||l.kind==="multipartBody")f.track("body",l.property);switch(l.kind){case"body":case"bodyRoot":let u=!1;if(l.kind==="body")u=!h.pipe(Ya(n,l.property,i));let a=Se(n,l.property.type,Sd(e)),c=a!==void 0&&!s;if(a&&s){let b=h.pipe(Rn(s.property));Y(n,{code:"http-file-structured",format:{contentTypes:b.join(", ")},target:s.property})}if(l.property.type.kind==="Union"&&$a(n,l.property.type))Y(n,{code:"http-file-structured",messageId:"union",target:l.property.node?.kind===K.ModelProperty?l.property.node.value:l.property});d??=c?h.pipe(Cd(a,l.property)):{bodyKind:"single",...h.pipe(ie(n,s,l.property.type)),type:l.property.type,isExplicit:l.kind==="body",containsMetadataAnnotations:u,property:l.property};break;case"multipartBody":d=h.pipe(_a(n,l.property,s,t));break}}for(let[l,u]of f.entries())for(let a of u)h.add(W({code:"duplicate-body",target:a}));return h.wrap(d)}function $a(n,e){return s(e);function s(t,i=new Set){if(i.has(t))return!1;i.add(t);for(let{type:h}of t.variants.values())if(!!Se(n,h)||h.kind==="Union"&&s(h,i))return!0;return!1}}function Ya(n,e,s){let t=L();return re(e.type,{modelProperty:(i)=>{let h=Kn(n,i)?"header":(s===N.Request||s===N.Response)&&Vn(n,i)?"cookie":(s===N.Request||s===N.Multipart)&&yn(n,i)?"query":s===N.Request&&gn(n,i)?"path":s===N.Response&&hn(n,i)?"statusCode":void 0;if(h)t.add(W({code:"metadata-ignored",format:{kind:h},target:i}))}},{}),t.wrap(t.diagnostics.length===0)}function _a(n,e,s,t){let i=L(),h=e.type,d=s&&i.pipe(Rn(s.property));for(let f of d??[])if(!_d.includes(f))i.add(W({code:"multipart-invalid-content-type",format:{contentType:f,supportedContentTypes:_d.join(", ")},target:h}));if(h.kind==="Model")return i.join(Fa(n,e,h,s,t));else if(h.kind==="Tuple")return i.join(Ha(n,e,h,s,t));else return i.add(W({code:"multipart-model",target:e})),i.wrap(void 0)}function Fa(n,e,s,t,i){let h=L(),d=[];for(let l of s.properties.values()){let u=h.pipe(Dd(n,l.type,i,l));if(u)d.push({partKind:"model",...u,name:u.name??l.name,optional:l.optional,property:l})}let f=t?{contentTypeProperty:t.property,contentTypes:h.pipe(Rn(t.property))}:{contentTypes:[mt.formData]};return h.wrap({bodyKind:"multipart",multipartKind:"model",...f,parts:d,property:e,type:s})}var mt={formData:"multipart/form-data",mixed:"multipart/mixed"},_d=Object.values(mt);function Ha(n,e,s,t,i){let h=L(),d=[],f=t&&h.pipe(Rn(t?.property));for(let[u,a]of s.values.entries()){let c=h.pipe(Dd(n,a,i));if(c?.name===void 0&&f?.includes(mt.formData))h.add(W({code:"formdata-no-part-name",target:s.node?.values[u]??s.values[u]}));if(c)d.push({partKind:"tuple",...c,optional:!1})}let l=t?{contentTypeProperty:t.property,contentTypes:h.pipe(Rn(t.property))}:{contentTypes:[mt.formData]};return h.wrap({bodyKind:"multipart",multipartKind:"tuple",...l,parts:d,property:e,type:s})}function Dd(n,e,s,t){if(e.kind==="Model"&&Rt(e)){let[i,h]=Fd(n,e.indexer.value,s,t);if(i)return[{...i,multi:!0},h];return[i,h]}else return Fd(n,e,s,t)}function Fd(n,e,s,t){let i=L(),h=_i(n,e);if(h){let{body:d,metadata:f}=i.pipe(Ue(n,h.type,s,N.Multipart)),l=f.find((u)=>u.kind==="contentType");if(d===void 0)return i.wrap(void 0);else if(d.bodyKind==="multipart")return i.add(W({code:"multipart-nested",target:e})),i.wrap(void 0);if(d.contentTypes.length===0)d={...d,contentTypes:i.pipe(ie(n,l,d.type)).contentTypes};return i.wrap({multi:!1,property:t,name:h.options.name,body:d,optional:!1,headers:f.filter((u)=>u.kind==="header"),filename:d.bodyKind==="file"?d.filename:void 0})}return i.add(W({code:"multipart-part",target:e})),i.wrap(void 0)}function Sd(n){let e=new Map(n.map((s)=>[s.property,s]));return function(t){let i=e.get(t);if(["filename"].includes(t.name))return!0;if(!i)return!0;if(i.kind!=="bodyProperty")return!1;return!0}}function Cd(n,e){let[s,t]=Rn(n.contentType),i=h(n.contents.type);return[{bodyKind:"file",type:n.type,contents:n.contents,filename:n.filename,isText:i,contentTypeProperty:n.contentType,contentTypes:s,property:e},t];function h(d){return f(d)||!!d.baseScalar&&h(d.baseScalar);function f(l){return l.name==="string"&&!!l.namespace&&l.namespace.name==="TypeSpec"&&!!l.namespace.namespace&&l.namespace.namespace.name===""&&!l.namespace.namespace.namespace}}}function Na(n){return n.kind==="Scalar"?"text/plain":"application/json"}function ja(n){return n.kind==="String"||n.kind==="Number"||n.kind==="Boolean"||n.kind==="StringTemplate"}function ie(n,e,s,t=Na){let i=L();return i.wrap(h());function h(){let d;if($d(n,s))d="application/merge-patch+json";if(e){let l=i.pipe(Rn(e.property));if(d){let u=l.filter((a)=>!a.startsWith(d));if(u.length>0)i.add(W({code:"merge-patch-content-type",target:e.property??s,format:{contentType:u[0]}}))}return{contentTypes:l,contentTypeProperty:e.property}}if(d)return{contentTypes:[d]};if(ja(s))switch(s.kind){case"StringTemplate":case"String":s=n.checker.getStdType("string");break;case"Boolean":s=n.checker.getStdType("boolean");break;case"Number":s=n.checker.getStdType("numeric");break;default:}let f;while((s.kind==="Scalar"||s.kind==="ModelProperty")&&(f=Nd(n,s)))s=f.type;if(s.kind==="Union"){let l=[...s.variants.values()];if(l.some((c)=>c.type.kind==="Intrinsic"&&c.type.name==="null"))return{contentTypes:["application/json"]};let a=new Set;for(let c of l){let b=i.pipe(ie(n,e,c.type));for(let r of b.contentTypes)a.add(r)}return{contentTypes:[...a]}}else return{contentTypes:[Hd(n,s)??t(s)]}}}var Da=["+","#",".","/",";","?","&"],Sa=/\{([^{}]+)\}|([^{}]+)/g,Ca=/([^:*]*)(?::(\d+)|(\*))?/;function Me(n){let e=[],s=[],t=n.matchAll(Sa);for(let[i,h,d]of t)if(h){let f;if(Da.includes(h[0]))f=h[0],h=h.slice(1);let l=h.split(",");for(let u of l){let a=u.match(Ca),b={name:a[1],operator:f,modifier:a[3]?{type:"explode"}:a[2]?{type:"prefix",value:Number(a[2])}:void 0};e.push(b),s.push(b)}}else s.push(d);return{segments:s,parameters:e}}function Be(n,e,s,t,i={}){let h=(i?.verbSelector&&i.verbSelector(n,e))??Gn(n,e)??t?.verb;if(h)return Mi(n,e,h,s);let d=Mi(n,e,"post",s);return d[0].body?d:Mi(n,e,"get",s)}var Md={";":"matrix","#":"fragment",".":"label","/":"path"};function Mi(n,e,s,t){let i=L(),h=Fi(n,e,s),d=Me(t),f=[],{body:l,metadata:u}=i.pipe(Ue(n,e.parameters,h,N.Request,{implicitParameter:(c)=>{let b=c.model===e.parameters,r=pn(n,c),m=me(n,c),I=r?.name??m?.name??c.name,Q=b&&d.parameters.find((sn)=>sn.name===I);if(!Q){let sn=pn(n,c);if(sn&&c.optional)return{type:"path",name:sn.name,explode:!1,allowReserved:!1,style:Md["/"]};return}let g=Q.modifier?.type==="explode";if(Q.operator==="?"||Q.operator==="&")return{type:"query",name:Q.name,explode:g};else if(Q.operator==="+")return{type:"path",name:Q.name,explode:g,allowReserved:!0,style:"simple"};else return{type:"path",name:Q.name,explode:g,allowReserved:!1,style:(Q.operator&&Md[Q.operator])??"simple"}}}));for(let c of u)switch(c.kind){case"contentType":f.push({name:"Content-Type",type:"header",param:c.property});break;case"path":case"query":case"cookie":case"header":f.push({type:c.kind,...c.options,param:c.property});break}let a=l;return i.wrap({properties:u,parameters:f,verb:s,body:a,get bodyType(){return a?.type},get bodyParameter(){return a?.property}})}var Bd=["/",":","?"];function Ma(n){return!(n.length===0||Bd.indexOf(n[0])!==-1||n[0]==="{"&&n[1]==="/")}function Ba(n,e=!1){if(Ma(n))n=`/${n}`;if(e&&n[n.length-1]==="/")return n.slice(0,-1);return n}function Ie(n){let e="";for(let[s,t]of n.entries())e+=Ba(t,s<n.length-1);return e}function Ka(n){let e=n.length===0?"/":Ie(n);return Bd.includes(e[0])||e[0]==="{"&&e[1]==="/"?e:`/${e}`}function rt(n,e,s,t){let i=L(),{uriTemplate:h,parameters:d}=i.pipe(pa(n,e,s,t)),f=Me(h),l=new Set(d.parameters.filter(({type:a})=>a==="path"||a==="query").map((a)=>a.name));Va(f,e,d).forEach((a)=>i.add(a));for(let a of f.parameters){let c=decodeURIComponent(a.name);if(!l.has(a.name)&&!l.has(c))i.add(W({code:"missing-uri-param",format:{param:a.name},target:e}))}let u=ya(f);return i.wrap({uriTemplate:h,path:u,parameters:d})}function Va(n,e,s){let t=L();if(n.segments){let[i,...h]=n.segments,d=i;for(let f of h)if(typeof f!=="string"){let l=s.parameters.find((u)=>u.name===f.name);if(f.operator==="/"){if(typeof d==="string"&&d.endsWith("/"))t.add(W({code:"double-slash",messageId:l?.param.optional?"optionalUnset":"default",format:{paramName:f.name},target:e}))}d=f}}return t.diagnostics}function ya(n){let e="";for(let s of n.segments??[])if(typeof s==="string")e+=s;else if(s.operator!=="?"&&s.operator!=="&")e+=`{${s.name}}`;return e}function Kd(n,e){if(e===void 0)return[[],{}];let[s,t]=Kd(n,e.namespace),i=he(n,e)?.path,h=e.kind==="Namespace"?Vd(n,e)??{}:{};return[[...s,...i?[i]:[]],{...t,...h}]}function pa(n,e,s,t){let[i,h]=Kd(n,e.interface??e.namespace),d=Ut(n,e)??Et,[f,l]=d(n,e,i,s,{...h,...t});return[{uriTemplate:Ka([f.uriTemplate]),parameters:f.parameters},l]}function Et(n,e,s,t,i){let h=L(),d=he(n,e)?.path,f=!d&&t?t.uriTemplate:Ie([...s,...d?[d]:[]]),l=Me(f),u=h.pipe(Be(n,e,f,t,i.paramOptions)),a=new Map(u.parameters.filter(({type:b})=>b==="path"||b==="query").map((b)=>[b.name,b]));for(let b of l.parameters)a.delete(b.name);let c=ec(f,[...a.values()]);return h.wrap({uriTemplate:c,parameters:u})}var ga={matrix:";",label:".",simple:"",path:"/",fragment:"#"};function vt(n){return`{${n.param.optional?"/":n.allowReserved?"+":ga[n.style]}${n.name}${n.explode?"*":""}}`}function nc(n){return`${sc(n.name)}${n.explode?"*":""}`}function Tt(n,e){let s=e.filter((t)=>t.type==="query");return n+(s.length>0?`{?${s.map((t)=>nc(t)).join(",")}}`:"")}function ec(n,e){let s=e.filter((h)=>h.type==="path").map(vt),t=e.filter((h)=>h.type==="query"),i=Ie([n,...s]);return Tt(i,t)}function sc(n){return encodeURIComponent(n).replace(/[:-]/g,function(e){return"%"+e.charCodeAt(0).toString(16).toUpperCase()})}function kt(n,e,s){n.stateMap(R.routeProducer).set(e,s)}function Ut(n,e){return n.stateMap(R.routeProducer).get(e)}function Vd(n,e){return n.stateMap(R.routeOptions).get(e)}function he(n,e){let s=n.stateMap(R.routes).get(e);return s?{path:s,shared:e.kind==="Operation"&&zn(n,e)}:void 0}var pd=yd({name:"op-reference-container-route",severity:"warning",description:"Check for referenced (`op is`) operations which have a @route on one of their containers.",url:"https://typespec.io/docs/libraries/http/rules/op-reference-container-route",docs:Wn.fromPackageRoot("src/rules/op-reference-container-route.md"),messages:{default:o`Operation ${"opName"} references an operation which has a @route prefix on its namespace or interface: "${"routePrefix"}".  This operation will not carry forward the route prefix so the final route may be different than the referenced operation.`},create(n){let e=new Map;function s(i){if(i===void 0)return;if(i.kind==="Operation")return s(i.interface)??s(i.namespace);let h=he(n.program,i);return h?h.path:s(i.namespace)}function t(i,h){if(i!==void 0){let d=i.interface??i.namespace,f=h.interface??h.namespace;if(d!==f){let l=e.get(i);if(l===void 0)l=s(i),e.set(i,l);if(l){n.reportDiagnostic({target:h,format:{opName:h.name,routePrefix:l}});return}}t(i.sourceOperation,h)}}return{operation:(i)=>{t(i.sourceOperation,i)}}}});var tc=gd({rules:[pd]});function Bi(n,e,s,t){let i=n.currentStage;if(i!=="validating"&&i!=="linting"&&i!=="emitting")return t();if(!s.isFinished)return t();let h=n.stateMap(e),d=h.get(s);if(d!==void 0)return d;let f=t();return h.set(s,f),f}function Ki(n,e){let s=L(),t=new tl,i=sl(n,e.returnType);for(let{type:h,description:d}of i)ic(n,s,e,t,h,d);return s.wrap(t.values())}function sl(n,e,s){let t=D(n);if(!t.union.is(e)||t.union.getDiscriminatedUnion(e))return[{type:e,description:s}];let i=Un(n,e)??s,h=[],d=[];for(let l of e.variants.values()){if(rs(l.type))continue;let u=sl(n,l.type,Un(n,l)??i);for(let a of u)if(fc(n,a.type))h.push(a.type);else d.push(a)}let f=[];if(h.length===1)f.push({type:h[0],description:i});else if(h.length>1){let l=d.length===0?e:t.union.create(h);f.push({type:l,description:i})}return f.push(...d),f}class tl{#n=new Map;get(n){return this.#n.get(this.#e(n))}set(n,e){this.#n.set(this.#e(n),e)}values(){return[...this.#n.values()]}#e(n){if(typeof n==="number"||n==="*")return String(n);else return`${n.start}-${n.end}`}}function ic(n,e,s,t,i,h){let d=Gn(n,s),{body:f,metadata:l}=e.pipe(Ue(n,i,O.Read,N.Response,{treatContentTypeAsHeader:d==="head"})),u=e.pipe(hc(n,i,l)),a=dc(n,l);if(u.length===0)if(Yn(n,i))u.push("*");else if(It(i))f=void 0,u.push(204);else if(f===void 0||It(f.type))f=void 0,u.push(200);else u.push(200);for(let c of u){let b=t.get(c)??{statusCodes:c,type:i,description:uc(n,s,i,c,l,h),responses:[]};if(f!==void 0)b.responses.push({body:f,headers:a,properties:l});else b.responses.push({headers:a,properties:l});t.set(c,b)}}function hc(n,e,s){let t=[],i=L(),h=!1;for(let d of s)if(d.kind==="statusCode"){if(h)Y(n,{code:"multiple-status-codes",target:e});h=!0,t.push(...i.pipe(Ji(n,d.property)))}if(e.kind==="Model")for(let d=e;d;d=d.baseModel)t.push(...il(n,d));return i.wrap(t)}function il(n,e){return n.stateMap(R.statusCode).get(e)??[]}function dc(n,e){let s={};for(let t of e)if(t.kind==="header")s[t.options.name]=t.property;return s}function lc(n){return n.some((e)=>e.kind==="body"||e.kind==="bodyRoot"||e.kind==="multipartBody"||e.kind==="statusCode")}function fc(n,e){if(It(e)||Yn(n,e))return!1;if(e.kind==="Model"&&il(n,e).length>0)return!1;let[s]=Ue(n,e,O.Read,N.Response);return!s||!s.metadata.some((t)=>t.kind!=="bodyProperty")}function uc(n,e,s,t,i,h){if(h)return h;if(lc(i)){let f=Un(n,s);if(f)return f}let d=Yn(n,s)?el(n,e):nl(n,e);if(d)return d;return Qi(t)}var ac=Symbol.for("@typespec/http.httpOperationCache");function yi(n,e,s){if(!s)return Bi(n,ac,e,()=>Ve(n,e,s,new Map));return Ve(n,e,s,new Map)}function fl(n,e,s){let t=L(),i=ll(e,s?.listOptions),h=new Map,d=i.map((f)=>t.pipe(Ve(n,f,s,h)));return t.wrap(d)}function pi(n,e){let s=L(),t=pe(n),i=t.map((h)=>s.pipe(Vi(n,h.type,e)));if(t.length===0)i.push(s.pipe(Vi(n,n.getGlobalNamespaceType(),e)));return s.wrap(i)}function Vi(n,e,s){let t=L(),i=t.pipe(fl(n,e,{...s,listOptions:{recursive:e!==n.getGlobalNamespaceType()}})),h=te(n,e);cc(n,t,i);let d={namespace:e,operations:i,authentication:h};return t.wrap(d)}function cc(n,e,s){let t=new Map;for(let i of s){let{verb:h,path:d}=i;if(i.overloading!==void 0&&ul(i))continue;if(zn(n,i.operation))continue;let f=t.get(d);if(f===void 0)f=new Map,t.set(d,f);let l=f.get(h);if(l===void 0)l=[],f.set(h,l);l.push(i)}for(let[i,h]of t)for(let[d,f]of h)if(f.length>=2)for(let l of f)e.add(W({code:"duplicate-operation",format:{path:i,verb:d,operationName:l.operation.name},target:l.operation}))}function ul(n){return n.path===n.overloading.path&&n.verb===n.overloading.verb}function Ve(n,e,s,t){let i=t.get(e);if(i)return[i,[]];let h=L(),d={operation:e};t.set(e,d);let f=dl(n,e),l;if(f)l=d.overloading=h.pipe(Ve(n,f,s,t));let u=h.pipe(rt(n,e,l,s??{})),a=h.pipe(Ki(n,e)),c=Zi(n,e),b={path:u.path,uriTemplate:u.uriTemplate,verb:u.parameters.verb,container:e.interface??e.namespace??n.getGlobalNamespaceType(),parameters:u.parameters,responses:a,operation:e,authentication:c};Object.assign(d,b);let r=hl(n,e);if(r)d.overloads=r.map((m)=>h.pipe(Ve(n,m,s,t)));return h.wrap(d)}var sh={};an(sh,{$provideTypeInfo:()=>rl,$onValidate:()=>Rl,$lib:()=>ps,$functions:()=>ml,$decorators:()=>xl});var wl=(n,e,s)=>{Ce(n.program,e,s)},gi=(n,e,s)=>{Di(n.program,e,s)},oc=Symbol.for("TypeSpec.Http.MutatorResultCache");function _n(n,e,s){let t=e[oc]??=new WeakMap,i=t.get(s);if(i)return i;return i=Ke(n,[e],s),t.set(s,i),i}var wc=Symbol.for("TypeSpec.Http.MergePatchMutatorCache");function nh(n,e,s,t){let i=!1;re(e,{intrinsic:(u)=>{if(!i&&u.name==="null")Y(n.program,{code:"merge-patch-contains-null",target:e}),i=!0}},{visitDerivedTypes:!1,includeTemplateDeclaration:!1});let h=n.program[wc]??={},d=t.visibilityMode.value.name,f=(h[s]??={})[d]??=mc(n,s,d),{type:l}=_n(n.program,f,e);return U(l.kind==="Model","Expected the root of the MergePatch transform to be a Model"),l}var eh=(n,e,s,t,i)=>{let h=nh(n,s,t,i);Ce(n.program,e,s),At(n.program,e,"application/merge-patch+json"),e.properties=h.properties};function bc(n,e){let s=Zn(n),t={any:new Set([s.members.get("Update")])},i={any:new Set([s.members.get("Create"),s.members.get("Update")])},h={any:new Set([s.members.get("Create")])};switch(e){case"Update":return[t,i,h];case"CreateOrUpdate":return[i,i,h];default:U(!1,`Unexpected MergePatch visibility mode: ${e}`)}}function rc(n,e){if(e.model===void 0)return!1;let s=En(n,e.model);if(s===void 0)return!1;if(s.propertyName!==e.name)return!1;return!0}function Rc(n,e){let[s,t]=cl(n,e.union);if(!s||e.type.kind!=="Model")return;for(let[i,h]of e.type.properties)if(i===s.options.discriminatorPropertyName)xc(n,h,{optional:!1,erasable:!1})}function xc(n,e,s){let t=xt(n,e)??{};if(s.optional!==void 0)t.optional=s.optional;if(s.erasable!==void 0)t.erasable=s.erasable;if(s.updateBehavior!==void 0)t.updateBehavior=s.updateBehavior;Si(n,e,t)}function mc(n,e,s){let t=Zn(n.program),[i,h,d]=bc(n.program,s),f=e+"ReplaceOnly",l=s==="CreateOrUpdate"?e:e+"OrCreate",u=c(d,f),a=c(h,l,u);if(s==="CreateOrUpdate")return a;else return c(i,e,u,a);function c(r,m,I,Q){function g(){return I===void 0}let sn={name:`MergePatchProperty${s}`,ModelProperty:{filter:()=>$n.DoNotRecur,mutate:(E,k,z,nn)=>{let Z=[],_=xt(z,E);for(let B of E.decorators){let Pn=B.decorator;if(Pn===Lt||Pn===qt){let Qe=B.args.filter((w)=>{if(w.value.entityKind!=="Value")return!1;return!(w.value.valueKind==="EnumValue"&&w.value.value.enum===t)});if(Qe.length>0)Z.push({...B,args:Qe})}else if(!(Pn===Wt&&B.args[0]?.value===t))Z.push(B)}if(k.decorators=Z,ol(z,k,t),de(E.type)){let B=E.optional?_n(z,Q??tn,E.type):_n(z,tn,E.type);k.type=B.type}if(!g()){if(_?.erasable!==!1&&(E.optional||E.defaultValue!==void 0))k.type=b(nn,k.type);k.optional=_?.optional??(rc(z,E)?!1:!0),k.defaultValue=void 0}k.decorators.push({decorator:gi,args:[{value:E,jsValue:E}]})}}},tn={name:`MergePatch${s}`,Union:{filter:()=>$n.DoNotRecur,mutate:(E,k,z)=>{for(let[nn,Z]of E.variants)if(Rc(z,Z),de(Z.type)){let _={...Z,type:_n(z,Q??tn,Z.type).type};k.variants.set(nn,_)}if(E.name)k.decorators=[...k.decorators,{decorator:function(nn,Z){At(nn.program,Z,"application/merge-patch+json")},args:[]}];al(n.program,k,m)}},Model:{filter:()=>$n.DoNotRecur,mutate:(E,k,z,nn)=>{if(D(nn).array.is(E)&&de(E.indexer.value))k.indexer={key:E.indexer.key,value:_n(z,I??tn,E.indexer.value).type};else if(D(nn).record.is(E)&&de(E.indexer.value))k.indexer={key:E.indexer.key,value:Ke(z,[Q??tn],E.indexer.value).type};for(let[Z,_]of E.properties)if(!bt(z,_,r)){let B=k.properties.get(Z);if(B)k.properties.delete(Z),nn.remove(B)}else if(!Hi(z,_)){let Pn=Ke(z,[sn],_).type;Pn.model=k,k.properties.set(Z,Pn)}else{let B=gn(z,_)?"@path":Kn(z,_)?"@header":Vn(z,_)?"@cookie":yn(z,_)?"@query":hn(z,_)?"@statusCode":void 0;if(B)Y(z,{code:"merge-patch-contains-metadata",target:_,format:{metadataType:B,propertyName:_.name}})}k.decorators=k.decorators.filter((Z)=>Z.decorator!==eh),k.decorators.push({decorator:function(Z,_){Ce(Z.program,_,E),At(Z.program,_,"application/merge-patch+json")},args:[]}),n.program.stateMap(R.mergePatchModel).set(k,E),al(n.program,k,m)}},ModelProperty:{filter:()=>$n.DoNotRecur,mutate:(E,k,z)=>{if(de(E.type))k.type=_n(z,E.optional?Q??tn:tn,E.type).type;n.program.stateMap(R.mergePatchProperty).set(k,E)}},UnionVariant:{filter:()=>$n.DoNotRecur,mutate:(E,k,z)=>{if(de(E.type)){let nn=_n(z,Q||tn,E.type);k.type=nn.type}}},Tuple:{filter:()=>$n.DoNotRecur,mutate:(E,k,z)=>{for(let[nn,Z]of E.values.entries())if(de(Z))k.values[nn]=_n(z,I??tn,Z).type}}};return tn}function b(r,m){return D(r).union.create({variants:[D(r).unionVariant.create({type:m}),D(r).unionVariant.create({type:D(r).intrinsic.null})]})}}function de(n){return n.kind==="Model"||n.kind==="Union"||n.kind==="ModelProperty"||n.kind==="UnionVariant"||n.kind==="Tuple"}function al(n,e,s){if(D(n).array.is(e)&&e.name==="Array")return;if(e.name&&s)e.name=Ec(s,e)}function Ec(n,e){if(e.kind==="TemplateParameter")return n;return n.replace(/{(\w+)}/g,(s,t)=>{return e[t]})}var rl=bl(({program:n,target:e})=>{if(e.kind!=="Operation")return;let[s]=yi(n,e);if(!s)return;let t=[`\`HTTP Route\`: \`${s.verb.toUpperCase()} ${s.uriTemplate}\``],i=s.responses.map((h)=>vc(h.statusCodes));if(i.length>0)t.push(`\`Responses\`: ${i.map((h)=>`\`${h}\``).join(", ")}`);return{content:t.join(`

`)}});function vc(n){if(n==="*")return"*";if(typeof n==="number")return String(n);return`${n.start}-${n.end}`}function Rl(n){let[e,s]=pi(n);if(s.length>0)n.reportDiagnostics(s);kc(n,e)}function Tc(n){let e=new Map;for(let s of n){let{verb:t,path:i}=s,h=e.get(i);if(h===void 0)h=new Map,e.set(i,h);let d=h.get(t);if(d===void 0)h.set(t,[s]);else d.push(s)}return e}function kc(n,e){for(let s of e){let t=Tc(s.operations);for(let i of t.values())for(let h of i.values()){let d=!1,f=!1;for(let l of h)if(zn(n,l.operation))d=!0;else f=!0;if(d&&f)for(let l of h)Y(n,{code:"shared-inconsistency",target:l.operation,format:{verb:l.verb,path:l.path}})}}}var xl={"TypeSpec.Http":{body:Ai,bodyIgnore:qi,bodyRoot:Li,cookie:Ui,delete:dt,get:tt,header:ki,head:lt,multipartBody:Wi,patch:ft,path:Ee,post:ht,put:it,query:Ii,route:ct,server:Pi,sharedRoute:$i,statusCode:Gi,useAuth:ut},"TypeSpec.Http.Private":{httpFile:Ld,httpPart:zd,plainData:Ad,includeInapplicableMetadataInPayload:Jd,applyMergePatch:eh,mergePatchModel:wl,mergePatchProperty:gi}},ml={"TypeSpec.Http.Private":{applyMergePatchTransform:nh}};function vl(n){Ic(n),Lc(n)}function Ic(n){n.stateSet(Bs.terminalEvent).forEach((e)=>{if(!("union"in e))return;Ac(n,e)})}function Ac(n,e){if(!Vs(n,e.union))Ms(n,{code:"terminal-event-not-in-events",target:e})}function Lc(n){ye(n,{model:(e)=>{qc(n,e)}})}function qc(n,e){let s=El(n,e);if(!s)return;let t=e.properties.get("contentType");if(!t)return;let[i]=Rn(t);if(!i.includes("text/event-stream"))return;if(s.kind!=="Union"){Ms(n,{code:"sse-stream-union-not-events",target:e});return}if(!Vs(n,s))Ms(n,{code:"sse-stream-union-not-events",target:e})}var Wc={"TypeSpec.SSE":{terminalEvent:od}};var vh={};an(vh,{namespace:()=>Vc,getVersion:()=>le,getUseDependencies:()=>Nn,getTypeChangedFrom:()=>Qn,getReturnTypeChangedFrom:()=>Ae,getRenamedFromVersions:()=>so,getRenamedFrom:()=>es,getRemovedOnVersions:()=>mh,getMadeRequiredOn:()=>wh,getMadeOptionalOn:()=>bh,getAddedOnVersions:()=>xh,findVersionedNamespace:()=>rh,VersionMap:()=>Eh,$versioned:()=>to,$useDependency:()=>io,$typeChangedFrom:()=>yc,$returnTypeChangedFrom:()=>pc,$renamedFrom:()=>gc,$removed:()=>oh,$madeRequired:()=>eo,$madeOptional:()=>no,$added:()=>ch});var Gc=F({name:"@typespec/versioning",diagnostics:{"versioned-dependency-tuple":{severity:"error",messages:{default:"Versioned dependency mapping must be a tuple [SourceVersion, TargetVersion]."}},"versioned-dependency-tuple-enum-member":{severity:"error",messages:{default:"Versioned dependency mapping must be between enum members."}},"versioned-dependency-same-namespace":{severity:"error",messages:{default:"Versioned dependency mapping must all point to the same namespace but 2 versions have different namespaces 'namespace1' and 'namespace2'."}},"versioned-dependency-not-picked":{severity:"error",messages:{default:o`The versionedDependency decorator must provide a version of the dependency '${"dependency"}'.`}},"version-not-found":{severity:"error",messages:{default:o`The provided version '${"version"}' from '${"enumName"}' is not declared as a version enum. Use '@versioned(${"enumName"})' on the containing namespace.`}},"version-duplicate":{severity:"error",messages:{default:o`Multiple versions from '${"name"}' resolve to the same value. Version enums must resolve to unique values.`}},"invalid-renamed-from-value":{severity:"error",messages:{default:"@renamedFrom.oldName cannot be empty string."}},"incompatible-versioned-reference":{severity:"error",messages:{default:o`'${"sourceName"}' is referencing versioned type '${"targetName"}' but is not versioned itself.`,addedAfter:o`'${"sourceName"}' was added in version '${"sourceAddedOn"}' but referencing type '${"targetName"}' added in version '${"targetAddedOn"}'.`,dependentAddedAfter:o`'${"sourceName"}' was added in version '${"sourceAddedOn"}' but contains type '${"targetName"}' added in version '${"targetAddedOn"}'.`,removedBefore:o`'${"sourceName"}' was removed in version '${"sourceRemovedOn"}' but referencing type '${"targetName"}' removed in version '${"targetRemovedOn"}'.`,dependentRemovedBefore:o`'${"sourceName"}' was removed in version '${"sourceRemovedOn"}' but contains type '${"targetName"}' removed in version '${"targetRemovedOn"}'.`,versionedDependencyAddedAfter:o`'${"sourceName"}' is referencing type '${"targetName"}' added in version '${"targetAddedOn"}' but version used is '${"dependencyVersion"}'.`,versionedDependencyRemovedBefore:o`'${"sourceName"}' is referencing type '${"targetName"}' removed in version '${"targetAddedOn"}' but version used is '${"dependencyVersion"}'.`,doesNotExist:o`'${"sourceName"}' is referencing type '${"targetName"}' which does not exist in version '${"version"}'.`}},"incompatible-versioned-namespace-use-dependency":{severity:"error",messages:{default:"The useDependency decorator can only be used on a Namespace if the namespace is unversioned. For versioned namespaces, put the useDependency decorator on the version enum members."}},"made-optional-not-optional":{severity:"error",messages:{default:o`Property '${"name"}' marked with @madeOptional but is required. Should be '${"name"}?'`}},"made-required-optional":{severity:"error",messages:{default:o`Property '${"name"}?' marked with @madeRequired but is optional. Should be '${"name"}'`}},"renamed-duplicate-property":{severity:"error",messages:{default:o`Property '${"name"}' marked with '@renamedFrom' conflicts with existing property in version ${"version"}.`}}},state:{versionIndex:{description:"Version index"},addedOn:{description:"State for @addedOn decorator"},removedOn:{description:"State for @removedOn decorator"},versions:{description:"State for @versioned decorator"},useDependencyNamespace:{description:"State for @useDependency decorator on Namespaces"},useDependencyEnum:{description:"State for @useDependency decorator on Enums"},renamedFrom:{description:"State for @renamedFrom decorator"},madeOptional:{description:"State for @madeOptional decorator"},madeRequired:{description:"State for @madeRequired decorator"},typeChangedFrom:{description:"State for @typeChangedFrom decorator"},returnTypeChangedFrom:{description:"State for @returnTypeChangedFrom decorator"}}}),{reportDiagnostic:j,createStateSymbol:w2,stateKeys:X}=Gc;var uh={};an(uh,{getCachedNamespaceDependencies:()=>dh,$onValidate:()=>Jc});function Fn(n,e,s,t){if(typeof n==="string")return zc(n,e,s,t);return Tl(n,e,t)}function Tl(n,e,s){if(e.node===void 0)return;let t=n.enumMember,i=`@added(${t.enum.name}.${t.name})`;return[Ul("add-version-to-type",i,A(e,s),ge(e.node))]}function zc(n,e,s,t){let i=Hn(s,e)?.find((h)=>h.value===n);if(i===void 0)return;return Tl(i,e,t)}function kl(n,e,s,t){if(e.node===void 0)return;let i=Hn(s,e)?.find((f)=>f.value===n);if(i===void 0)return;let h=i.enumMember,d=`@removed(${h.enum.name}.${h.name})`;return[Ul("remove-version-from-type",d,A(e,t),ge(e.node))]}function Ul(n,e,s,t){return{id:n,label:`Add '${e}' to '${s}'`,fix:(i)=>{return i.prependText(t,`${e}
`)}}}var Wl=Symbol.for("TypeSpec.Versioning.NamespaceRelationCache");function dh(n){return n[Wl]}function Jc(n){let e=new Map;function s(t,i){if(!i||!("namespace"in i)||!i.namespace)return;let h=e.get(t)??new Set;if(i.namespace!==t)h.add(i.namespace);e.set(t,h)}n[Wl]=e,ye(n,{model:(t)=>{if(Bn(t))return;if(H(t))return;if(!t.name)return;s(t.namespace,t.sourceModel),s(t.namespace,t.baseModel);for(let i of t.properties.values()){if(s(t.namespace,i.type),ns(n,t,i,{isTargetADependent:!0}),Qn(n,i)!==void 0)Il(n,i);else Jn(n,i,i.type);Zc(n,i),$c(n,i)}ih(n,t)},union:(t)=>{if(Bn(t))return;if(H(t))return;if(t.namespace===void 0)return;for(let i of t.variants.values())s(t.namespace,i.type);ih(n,t)},operation:(t)=>{if(Bn(t))return;if(H(t))return;let i=t.namespace??t.interface?.namespace;if(s(i,t.sourceOperation),s(i,t.returnType),t.interface)ns(n,t.interface,t,{isTargetADependent:!0});Jn(n,t,t.returnType);for(let h of t.parameters.sourceModels)Jn(n,t,h.model);for(let h of t.parameters.properties.values())if(ns(n,t,h,{isTargetADependent:!0}),Qn(n,h)!==void 0)Il(n,h);else Jn(n,[h,t],h.type)},interface:(t)=>{if(H(t))return;for(let i of t.sourceInterfaces)Jn(n,t,i)},namespace:(t)=>{Xc(n,t);let i=rh(n,t),h=ah(n,t);if(h===void 0)return;for(let[d,f]of h.entries())if(i){if(Nn(n,t,!1)!==void 0)j(n,{code:"incompatible-versioned-namespace-use-dependency",target:t})}else if(f instanceof Map)j(n,{code:"versioned-dependency-not-picked",format:{dependency:hh(d)},target:t})},enum:(t)=>{ih(n,t);let i=Nn(n,t);if(!i)return;for(let[h,d]of i){let f=new Set;if(d instanceof Map)for(let l of d.values())f.add(l.namespace);else f.add(d.namespace);e.set(h,f)}}},{includeTemplateDeclaration:!0})}function Il(n,e,s){let t=Pc(n,e);if(t===void 0)return;for(let[i,h]of t){if(h===void 0)continue;Qc(n,i,h,e,s)}}function Qc(n,e,s,t,i){let h=[s];while(h.length){let d=h.pop(),l=vn(n,d)?.get(e?.name)??T.Available;if(![T.Added,T.Available].includes(l))j(n,{code:"incompatible-versioned-reference",messageId:"doesNotExist",format:{sourceName:A(t,i),targetName:A(d,i),version:Gt(e)},target:t,codefixes:Fn(e,d,n,i)});if(Bn(d)){for(let u of d.templateMapper.args)if(mn(u))h.push(u)}else if(d.kind==="Union")for(let u of d.variants.values())if(d.expression)h.push(u.type);else ns(n,u,u.type);else if(d.kind==="Tuple")for(let u of d.values)h.push(u)}}function Oc(n,e){let s=Hn(n,e);if(s===void 0)return;let t=new Map(s.map((l)=>[l,void 0])),i=vn(n,e),h=i===void 0,d=es(n,e);if(d!==void 0)for(let l of d){let{version:u,oldName:a}=l,c=s.indexOf(u);if(c!==-1)t.set(s[c-1],a)}let f=void 0;switch(e.kind){case"ModelProperty":f=e.name;break;case"UnionVariant":if(typeof e.name==="string")f=e.name;break;case"EnumMember":f=e.name;break;default:throw Error(`Not implemented '${e.kind}'.`)}for(let l of s.reverse()){if(!(h||[T.Added,T.Available].includes(i.get(l.name)))){t.set(l,void 0);continue}let a=t.get(l);if(a!==void 0)f=a;else t.set(l,f)}return t}function Pc(n,e){let s=Hn(n,e);if(s===void 0)return;let t=new Map(s.map((l)=>[l,void 0])),i=vn(n,e),h=i===void 0,d=Qn(n,e);if(d!==void 0)for(let[l,u]of d){let a=s.indexOf(l);if(a!==-1)t.set(s[a-1],u)}let f;switch(e.kind){case"ModelProperty":f=e.type;break;default:throw Error(`Not implemented '${e.kind}'.`)}for(let l of s.reverse()){if(!(h||[T.Added,T.Available].includes(i.get(l.name)))){t.set(l,void 0);continue}let a=t.get(l);if(a!==void 0)f=a;else t.set(l,f)}return t}function Xc(n,e){let[s,t]=V(n,e);if(t===void 0)return;let i=new Set(t.getVersions().map((h)=>h.value));if(t.size!==i.size){let h=t.getVersions()[0].enumMember.enum.name;j(n,{code:"version-duplicate",format:{name:h},target:e})}}function ih(n,e){let s=Hn(n,e);if(s===void 0)return;let t=new Map(s.map((h)=>[h,[]])),i=[];if(e.kind==="Model")i=e.properties.values();else if(e.kind==="Enum")i=e.members.values();else if(e.kind==="Union")i=e.variants.values();for(let h of i){let d=Oc(n,h);if(d===void 0)continue;for(let[f,l]of d){if(l===void 0)continue;t.get(f)?.push(l)}}for(let[h,d]of t.entries()){let f=new Map;for(let l of d){let u=f.get(l)??0;f.set(l,u+1)}for(let[l,u]of f.entries()){if(l===void 0)continue;if(u>1)j(n,{code:"renamed-duplicate-property",format:{name:l,version:Gt(h)},target:e})}}}function Zc(n,e){if(e.kind==="ModelProperty"){if(!bh(n,e))return;if(!e.optional){j(n,{code:"made-optional-not-optional",format:{name:e.name},target:e});return}}}function $c(n,e){if(e.kind==="ModelProperty"){if(!wh(n,e))return;if(e.optional){j(n,{code:"made-required-optional",format:{name:e.name},target:e});return}}}function Jn(n,e,s){if(ns(n,e,s),"templateMapper"in s){for(let i of s.templateMapper?.args??[])if(mn(i))Jn(n,e,i)}let t=Array.isArray(e)?e:[e];switch(s.kind){case"Model":if(!s.name)for(let i of s.properties.values())Jn(n,[i,...t],i.type);break;case"Union":if(typeof s.name!=="string")for(let i of s.variants.values())Jn(n,e,i.type);break;case"Tuple":for(let i of s.values)Jn(n,e,i);break}}function Al(n,e){let s=Array.isArray(e)?e:[e],t=s[0],i=Yc(n,s);return{type:t,map:i}}function Yc(n,e){for(let s of e){let t=vn(n,s);if(t)return t;switch(s.kind){case"Operation":{let i=s.interface&&vn(n,s.interface);if(i)return i;break}case"ModelProperty":{let i=s.model&&vn(n,s.model);if(i)return i;break}}}return}function ns(n,e,s,t={}){let i=Al(n,e),[h]=V(n,i.type);if(i.map===void 0){let u=Array.isArray(e)?e:[e],a=V(n,u[0]);for(let c of u)if(V(n,c)!==a)return}let d=Al(n,s),[f]=V(n,d.type);if(!d.map||!f)return;let l;if(h!==f){if(l=(h&&ah(n,h))?.get(f),l===void 0)return;if(d.map=_c(n,d.map,l,i.type,d.type),!d.map)return}if(t.isTargetADependent)Nc(n,i.map,d.map,i.type,d.type);else Fc(n,i.map,d.map,i.type,d.type,l instanceof Map?l:void 0)}function _c(n,e,s,t,i){if(!(s instanceof Map)){let h=s;if([T.Removed,T.Unavailable].includes(e.get(h.name))){let d=lh(h.name,T.Added,e),f=fh(h.name,T.Removed,e);if(d)j(n,{code:"incompatible-versioned-reference",messageId:"versionedDependencyAddedAfter",format:{sourceName:A(t),targetName:A(i),dependencyVersion:Gt(h),targetAddedOn:d},target:t,codefixes:Fn(h,i,n)});if(f)j(n,{code:"incompatible-versioned-reference",messageId:"versionedDependencyRemovedBefore",format:{sourceName:A(t),targetName:A(i),dependencyVersion:Gt(h),targetAddedOn:f},target:t,codefixes:Fn(h,i,n)})}return}else{let h=new Map;for(let[d,f]of s){let l=e.get(f.name);h.set(d.name,l)}return h}}function lh(n,e,s){let t=!1;for(let[i,h]of s){if(n===i){t=!0;continue}if(!t)continue;if(h===e)return i}return}function fh(n,e,s){let t=!1;for(let[i,h]of s){if([T.Added,T.Added].includes(h))t=!0;if(!t)continue;if(h===e)return i;if(i===n)break}return}function Fc(n,e,s,t,i,h){if(e===void 0){if(!jc(s)){let a=Array.from(s.entries()).filter(([c,b])=>b===T.Available||b===T.Added).map(([c,b])=>c).sort().shift();j(n,{code:"incompatible-versioned-reference",messageId:"default",format:{sourceName:A(t),targetName:A(i)},target:t,codefixes:a?Fn(a,t,n):void 0})}return}let d=[...e.keys(),...s.keys()],f=Qn(n,t);if(f!==void 0){let a=[...f.keys()].map((c)=>c.name);d=[...d,...a]}let l=Ae(n,t);if(l!==void 0){let a=[...l.keys()].map((c)=>c.name);d=[...d,...a]}let u=new Set(d);for(let a of u){let c=e.get(a),b=s.get(a);if([T.Added].includes(c)&&[T.Removed,T.Unavailable].includes(b)){let r=lh(a,T.Added,s),m=a;if(h)m=ql(a,h)??a;j(n,{code:"incompatible-versioned-reference",messageId:"addedAfter",format:{sourceName:A(t),targetName:A(i),sourceAddedOn:a,targetAddedOn:r},target:t,codefixes:Fn(m,i,n)})}if([T.Removed].includes(c)&&[T.Unavailable].includes(b)){let r=fh(a,T.Removed,s),m=a;if(h)m=ql(a,h)??a;j(n,{code:"incompatible-versioned-reference",messageId:"removedBefore",format:{sourceName:A(t),targetName:A(i),sourceRemovedOn:a,targetRemovedOn:r},target:t,codefixes:Fn(m,i,n)})}}}function Ll(n,e){if(n.kind==="ModelProperty")return Hc(n,e);return!1}function Hc(n,e){if(n.sourceProperty===void 0)return!1;let s=e==="added"?ch:oh,t=n.decorators.filter((h)=>h.decorator===s),i=n.sourceProperty.decorators.filter((h)=>h.decorator===s);return!t.some((h)=>!i.some((d)=>h.node===d.node))}function Nc(n,e,s,t,i,h,d){if(!e)return;let f=new Set([...e.keys(),...s.keys()]);for(let l of f){let u=e.get(l),a=s.get(l);if(u===a)continue;if([T.Added].includes(a)&&[T.Removed,T.Unavailable].includes(u)&&!Ll(i,"added")){let c=fh(l,T.Added,e);j(n,{code:"incompatible-versioned-reference",messageId:"dependentAddedAfter",format:{sourceName:A(t,h),targetName:A(i,d),sourceAddedOn:c,targetAddedOn:l},target:i,codefixes:Fn(l,t,n,d)})}if([T.Removed].includes(u)&&[T.Added,T.Available].includes(a)&&!Ll(i,"removed")){let c=lh(l,T.Removed,s);j(n,{code:"incompatible-versioned-reference",messageId:"dependentRemovedBefore",format:{sourceName:A(t),targetName:A(i),sourceRemovedOn:l,targetRemovedOn:c},target:i,codefixes:kl(l,i,n,d)})}}}function jc(n){for(let e of n.values())if([T.Removed,T.Unavailable].includes(e))return!1;return!0}function Gt(n){return n?.value??"<n/a>"}function ql(n,e){for(let[s,t]of e.entries())if(s.value===n)return t;return}class Gl{#n;#e;#i;#s;constructor(n,e){let s=new Set,t=new Set,i=this.#e=e.map((d)=>new Le(d));for(let d of e)for(let[f,l]of d.entries())s.add(l),t.add(f);this.#n=[...t];function h(d){for(let[f,l]of i.entries()){let u=l.getVersion(d.namespace);if(u&&d.index<u.index)return f}return-1}for(let d of t){let[,f]=V(n,d);if(f===void 0)continue;for(let l of f.getVersions())if(!s.has(l)){s.add(l);let u=h(l),a=new Le(new Map([[l.namespace,l]]));if(u===-1)i.push(a);else i.splice(u,0,a)}}this.#s=new Map,this.#i=new Map;for(let[d,f]of i.entries()){this.#i.set(f,d);for(let l of f.versions())if(!this.#s.has(l))this.#s.set(l,d)}}prettySerialize(){let n="-".repeat(this.#n.length*13+1),e=this.#e.map((s)=>{return"| "+this.#n.map((t)=>(s.getVersion(t)?.name??"").padEnd(10," ")).join(" | ")+" |"}).join(`
${n}
`);return["",n,e,n].join(`
`)}get(n){let e=this.getIndex(n);if(e===-1)if(n instanceof Le)U(!1,`Timeline moment "${n?.name}" should have been resolved`);else U(!1,`Version "${n?.name}" from ${A(n.namespace)} should have been resolved. ${this.prettySerialize()}`);return this.#e[e]}getIndex(n){let e=n instanceof Le?this.#i.get(n):this.#s.get(n);if(e===void 0)return-1;return e}isBefore(n,e){let s=this.getIndex(n),t=this.getIndex(e);return s<t}first(){return this.#e[0]}[Symbol.iterator](){return this.#e[Symbol.iterator]()}entries(){return this.#e.entries()}}class Le{name;#n;constructor(n){this.#n=n,this.name=n.values().next().value?.name??""}getVersion(n){return this.#n.get(n)}versions(){return this.#n.values()}}function ah(n,e){let s=Nn(n,e),i=dh(n)?.get(e);if(i===void 0)return s;let h=new Map(s);for(let d of i)if(!s?.has(d)){let f=le(n,d);if(f){let l=f.getVersions();h.set(d,l[l.length-1])}}return h}var zl=new WeakMap;function In(n,e){return zl.set(n,e),e}function Dc(n,e){let s=e.namespace;if(s===void 0)return[];let t=le(n,s);if(t===void 0)return[];return[s,t]}function V(n,e){let s=zl.get(e);if(s)return s;switch(e.kind){case"Namespace":return Sc(n,e);case"Operation":case"Interface":case"Model":case"Union":case"Scalar":case"Enum":if(e.namespace)return In(e,V(n,e.namespace)||[]);else if(e.kind==="Operation"&&e.interface)return In(e,V(n,e.interface)||[]);else return In(e,[]);case"ModelProperty":if(e.sourceProperty)return V(n,e.sourceProperty);else if(e.model)return V(n,e.model);else return In(e,[]);case"EnumMember":return In(e,V(n,e.enum)||[]);case"UnionVariant":return In(e,V(n,e.union)||[]);default:return In(e,[])}}function Sc(n,e){let s=le(n,e);if(s!==void 0)return In(e,[e,s]);let t=e.namespace&&V(n,e.namespace)[1],i=Nn(n,e);if(t||i)return In(e,[e,t]);else return In(e,[e,void 0])}function Hn(n,e){let[s,t]=V(n,e);if(s===void 0)return;return le(n,s)?.getVersions()}var T;(function(n){n.Unavailable="Unavailable",n.Added="Added",n.Available="Available",n.Removed="Removed"})(T||(T={}));function Cc(n,e,s){let t=void 0;if(e.kind==="ModelProperty"&&e.model!==void 0)t=vn(n,e.model);else if(e.kind==="Operation"&&e.interface!==void 0)t=vn(n,e.interface);if(t===void 0)return;for(let[i,h]of t.entries())if(h===T.Added)return s.find((d)=>d.name===i);return}function Mc(n,e,s){let t=void 0;if(e.kind==="ModelProperty"&&e.model!==void 0)t=vn(n,e.model);else if(e.kind==="Operation"&&e.interface!==void 0)t=vn(n,e.interface);if(t===void 0)return;for(let[i,h]of t.entries())if(h===T.Removed)return s.find((d)=>d.name===i);return}function Bc(n,e,s){if(!n.length&&!e.length)return[s];if(n.length){if(!e.length||n[0].index<e[0].index)return n}if(e.length){if(!n.length||e[0].index<n[0].index)return[s,...n]}return n}function Kc(n,e,s){if(e.length)return e;let t=!n.length||s&&n[0].index<s.index;if(s&&t)return[s];return[]}function vn(n,e){let s=new Map,t=Hn(n,e);if(t===void 0)return;let i=t[0],h=Cc(n,e,t)??i,d=Mc(n,e,t),f=xh(n,e)??[],l=mh(n,e)??[],u=Qn(n,e),a=Ae(n,e);if(!f.length&&!l.length&&u===void 0&&a===void 0)return;f=Bc(f,l,h),l=Kc(f,l,d);let c=!1;for(let b of t){let r=f.find((I)=>I.index===b.index);if(l.find((I)=>I.index===b.index))c=!1,s.set(b.name,T.Removed);else if(r)c=!0,s.set(b.name,T.Added);else if(c)s.set(b.name,T.Available);else s.set(b.name,T.Unavailable)}return s}function Rh(n,e){let s=e.enum,[,t]=Dc(n,s);return t?.getVersionForEnumMember(e)}var Vc="TypeSpec.Versioning";function jn(n,e,s){let t=Rh(n,e);if(!t)j(n,{code:"version-not-found",target:s,format:{version:e.name,enumName:e.enum.name}});return t}var ch=(n,e,s)=>{let{program:t}=n,i=jn(n.program,s,n.getArgumentTarget(0));if(!i)return;let h=t.stateMap(X.addedOn).get(e)??[];h.push(i),h.sort((d,f)=>d.index-f.index),t.stateMap(X.addedOn).set(e,h)};function oh(n,e,s){let{program:t}=n,i=jn(n.program,s,n.getArgumentTarget(0));if(!i)return;let h=t.stateMap(X.removedOn).get(e)??[];h.push(i),h.sort((d,f)=>d.index-f.index),t.stateMap(X.removedOn).set(e,h)}function Qn(n,e){return n.stateMap(X.typeChangedFrom).get(e)}var yc=(n,e,s,t)=>{let{program:i}=n,h=jn(n.program,s,n.getArgumentTarget(0));if(!h)return;let d=Qn(i,e)??new Map;d.set(h,t),d=new Map([...d.entries()].sort((f,l)=>f[0].index-l[0].index)),i.stateMap(X.typeChangedFrom).set(e,d)};function Ae(n,e){return n.stateMap(X.returnTypeChangedFrom).get(e)}var pc=(n,e,s,t)=>{let{program:i}=n,h=jn(n.program,s,n.getArgumentTarget(0));if(!h)return;let d=Ae(i,e)??new Map;d.set(h,t),d=new Map([...d.entries()].sort((f,l)=>f[0].index-l[0].index)),i.stateMap(X.returnTypeChangedFrom).set(e,d)},gc=(n,e,s,t)=>{let{program:i}=n,h=jn(n.program,s,n.getArgumentTarget(0));if(!h)return;if(t==="")j(i,{code:"invalid-renamed-from-value",target:e});let d=es(i,e)??[];d.push({version:h,oldName:t}),d.sort((f,l)=>f.version.index-l.version.index),i.stateMap(X.renamedFrom).set(e,d)},no=(n,e,s)=>{let{program:t}=n,i=jn(n.program,s,n.getArgumentTarget(0));if(!i)return;t.stateMap(X.madeOptional).set(e,i)},eo=(n,e,s)=>{let{program:t}=n,i=jn(n.program,s,n.getArgumentTarget(0));if(!i)return;t.stateMap(X.madeRequired).set(e,i)};function wh(n,e){return n.stateMap(X.madeRequired).get(e)}function es(n,e){return n.stateMap(X.renamedFrom).get(e)}function so(n,e){return es(n,e)?.map((s)=>s.version)}function xh(n,e){return n.stateMap(X.addedOn).get(e)}function mh(n,e){return n.stateMap(X.removedOn).get(e)}function bh(n,e){return n.stateMap(X.madeOptional).get(e)}class Eh{map=new Map;constructor(n,e){let s=0;for(let t of e.members.values())this.map.set(t,{name:t.name,value:t.value?.toString()??t.name,enumMember:t,index:s,namespace:n}),s++}getVersionForEnumMember(n){return this.map.get(n)}getVersions(){return[...this.map.values()]}get size(){return this.map.size}}var to=(n,e,s)=>{n.program.stateMap(X.versions).set(e,new Eh(e,s))};function le(n,e){return n.stateMap(X.versions).get(e)}function rh(n,e){let s=e;while(s){if(n.stateMap(X.versions).has(s))return s;s=s.namespace}return}function io(n,e,...s){let t=[];for(let i of s){let h=jn(n.program,i,n.getArgumentTarget(0));if(h)t.push(h)}if(e.kind==="Namespace"){let i=Jl(n.program,e);if(!i)i=t;else i.push(...t);n.program.stateMap(X.useDependencyNamespace).set(e,i)}else if(e.kind==="EnumMember"){let i=e.enum,h=n.program.stateMap(X.useDependencyEnum).get(i);if(!h)h=new Map;let d=h.get(e)??[];d.push(...t),h.set(e,d),n.program.stateMap(X.useDependencyEnum).set(i,h)}}function Jl(n,e){return n.stateMap(X.useDependencyNamespace).get(e)}function Nn(n,e,s=!0){let t=new Map;if(e.kind==="Namespace"){let i=e;while(i){let h=Jl(n,i);if(!h){if(s){let d=le(n,i)?.getVersions();if(d?.length){let f=Nn(n,d[0].enumMember.enum);if(f)return f}}i=i.namespace}else{for(let d of h)t.set(d.namespace,d);return t}}return}else if(e.kind==="Enum"){let i=n.stateMap(X.useDependencyEnum).get(e);if(!i)return;let h=ho(n,i);if(h instanceof Map)for(let[d,f]of h)for(let l of f){let u=l.enumMember.enum.namespace;if(!u){j(n,{code:"version-not-found",target:l.enumMember.enum,format:{version:l.enumMember.name,enumName:l.enumMember.enum.name}});return}let a=t.get(u);if(a)a.set(d,l);else a=new Map([[d,l]]);t.set(u,a)}}return t}function ho(n,e){if(!(e instanceof Map))return e;let s=new Map;for(let[t,i]of e){let h=Rh(n,t);if(h!==void 0)s.set(h,i)}return s}var Gh={};an(Gh,{$onValidate:()=>lf,$lib:()=>Th,$decorators:()=>Uo});var Th=F({name:"@typespec/rest",diagnostics:{"not-key-type":{severity:"error",messages:{default:"Cannot copy keys from a non-key type (KeysOf<T> or ParentKeysOf<T>)"}},"resource-missing-key":{severity:"error",messages:{default:o`Type '${"modelName"}' is used as a resource and therefore must have a key. Use @key to designate a property as the key.`}},"resource-missing-error":{severity:"error",messages:{default:o`Type '${"modelName"}' is used as an error and therefore must have the @error decorator applied.`}},"duplicate-key":{severity:"error",messages:{default:o`More than one key found on model type ${"resourceName"}`}},"duplicate-parent-key":{severity:"error",messages:{default:o`Resource type '${"resourceName"}' has a key property named '${"keyName"}' which conflicts with the key name of a parent or child resource.`}},"invalid-action-name":{severity:"error",messages:{default:"Action name cannot be empty string."}},"shared-route-unspecified-action-name":{severity:"error",messages:{default:o`An operation marked as '@sharedRoute' must have an explicit collection action name passed to '${"decoratorName"}'.`}},"circular-parent-resource":{severity:"error",messages:{default:o`Resource has a parent cycle (${"cycle"})`}}}}),{reportDiagnostic:cn,createDiagnostic:P2,createStateSymbol:y}=Th;class kh{#n=[];add(n){let e=this.#n.indexOf(n);if(e!==-1)return this.#n.slice(e);this.#n.push(n);return}}var Xl=y("resourceKeys"),lo=y("resourceTypeForKeyParam");function Ql(n,e,s){n.stateMap(Xl).set(e,{resourceType:e,keyProperty:s})}function On(n,e){let s=n.stateMap(Xl).get(e);if(s)return s;if(e.properties.forEach((t)=>{if(Pl(n,t))if(s)cn(n,{code:"duplicate-key",format:{resourceName:e.name},target:t});else s={resourceType:e,keyProperty:t},Ql(n,e,s.keyProperty)}),s===void 0&&e.baseModel!==void 0){if(s=On(n,e.baseModel),s!==void 0)Ql(n,e,s.keyProperty)}return s}function Jt(n,e,s){n.program.stateMap(lo).set(e,s)}ce("Private",Jt);var fo=[Lt,Wt,qt];function Uh(n,e,s){let{program:t}=n,i=ss(t,s);if(i)Uh(n,e,i);let h=On(t,s);if(h){let{keyProperty:d}=h,f=zt(t,d),l=[...d.decorators.filter((a)=>fo.every((c)=>a.decorator.name!==c.name)),{decorator:Jt,args:[{node:e.node,value:s,jsValue:s}]}];if(!d.decorators.some((a)=>a.decorator.name===Ee.name))l.push({decorator:Ee,args:[]});let u=t.checker.cloneType(d,{name:f,decorators:l,optional:!1,model:e,sourceProperty:void 0});e.properties.set(f,u)}}function Zl(n,e,s){let t=()=>cn(n.program,{code:"not-key-type",target:e}),i=e.templateMapper?.args;if(!i||i.length!==1)return t();if(i[0].kind!=="Model"){if(Ol(i[0]))return;return t()}let h=i[0];if(s==="parent"){let d=ss(n.program,h);if(d)Uh(n,e,d)}else Uh(n,e,h)}var[ss,uo]=q(y("parentResourceTypes")),$l=(n,e,s)=>{let{program:t}=n;if(!ao(t,e,s))return;uo(t,e,s)};function ao(n,e,s){let t=new kh;t.add(e);let i=s;while(i){let h=t.add(i);if(h){for(let d of h)t.add(d),cn(n,{code:"circular-parent-resource",format:{cycle:[...h,h[0]].map((f)=>A(f)).join(" -> ")},target:d});return!1}i=ss(n,i)}return!0}var Yl;try{Yl=(await import("./chunk-y914jynq.js")).getStreamOf}catch{Yl=()=>{throw Error("@typespec/streams was not found")}}function oo(n,e,s){let i=ef(n,e);if(i&&i!==""){let h=Ro(n,e)??"/";s.push(`${h}${i}`)}}function _l(n,e,s){let t=Ih(n,e);if(t&&t!=="")s.push(`/${t}`)}var wo={read:"get",create:"post",createOrUpdate:"patch",createOrReplace:"put",update:"patch",delete:"delete",list:"get"};function Fl(n,e){let s=mo(n,e);return Gn(n,e)??(s&&wo[s.operation])??(Lh(n,e)||qh(n,e)?"post":void 0)}function bo(n,e,s,t,i){let h=L(),d=he(n,e)?.path,f=[...s,...d?[d]:[]],l=[],u=new Set,a={...i?.paramOptions??{},verbSelector:Fl},c=h.pipe(Be(n,e,"",void 0,a));for(let r of c.parameters){let{type:m,param:I}=r;if(m==="path"){_l(n,I,f);let Q=i.autoRouteOptions?.routeParamFilter?.(e,I);if(Q?.routeParamString){if(f.push(`/${Q.routeParamString}`),Q?.excludeFromOperationParams===!0)continue}else if(I.type.kind==="String"){f.push(`${I.type.value}`);continue}else f.push(`${vt(r)}`)}l.push(r),u.add(r.param)}c.parameters=l;for(let r=c.properties.length-1;r>=0;r--){let m=c.properties[r];if(!["header","query","path","cookie"].includes(m.kind))continue;if(!u.has(m.property))c.properties.splice(r,1)}_l(n,e,f),oo(n,e,f);let b=Ie(f);return h.wrap({uriTemplate:Tt(b,l),parameters:{...c,parameters:l}})}var ro=y("autoRoute"),Ot=(n,e)=>{if(e.kind==="Operation")kt(n.program,e,bo);else for(let[s,t]of e.operations)n.call(Ot,t),t.decorators.push({decorator:Ot,args:[]});n.program.stateSet(ro).add(e)};var Hl=y("segments");function qe(n,e,s){n.program.stateMap(Hl).set(e,s)}function Nl(n,e){let s=On(n,e);return s?Ih(n,s.keyProperty):Ih(n,e)}var Pt=(n,e,s)=>{if(s.kind==="TemplateParameter")return;let t=Nl(n.program,s);if(t)n.call(qe,e,t)};function Ih(n,e){return n.stateMap(Hl).get(e)}var Ah=y("actionSeparator"),jl=(n,e,s)=>{n.program.stateMap(Ah).set(e,s)};function Ro(n,e){let s=n.stateMap(Ah),t=s.get(e);if(t!==void 0)return t;if(e.kind==="Operation"){if(e.interface){let i=s.get(e.interface);if(i!==void 0)return i;if(e.interface.namespace)return Qt(n,e.interface.namespace)}if(e.namespace)return Qt(n,e.namespace)}if(e.kind==="Interface"&&e.namespace)return Qt(n,e.namespace);return}function Qt(n,e){let t=n.stateMap(Ah).get(e);if(t!==void 0)return t;if(e.namespace)return Qt(n,e.namespace);return}var Dl=(n,e,s)=>{let t=On(n.program,e);if(!t){cn(n.program,{code:"resource-missing-key",format:{modelName:e.name},target:e});return}n.call(qe,t.keyProperty,s),t.keyProperty.decorators.push({decorator:qe,args:[{value:n.program.checker.createLiteralType(s),jsValue:s}]})},Sl=y("resourceOperations");function xo(n,e,s,t,i){let h={...i?.paramOptions??{},verbSelector:Fl};return Et(n,e,s,t,{...i,paramOptions:h})}function fe(n,e,s,t){if(s.kind==="TemplateParameter")return;if(n.program.stateMap(Sl).set(e,{operation:t,resourceType:s}),!Ut(n.program,e))kt(n.program,e,xo)}function mo(n,e){return n.stateMap(Sl).get(e)}var Cl=(n,e,s)=>{fe(n,e,s,"read")};function Ml(n,e,s){n.call(Pt,e,s),fe(n,e,s,"create")}function Bl(n,e,s){fe(n,e,s,"createOrReplace")}function Kl(n,e,s){fe(n,e,s,"createOrUpdate")}function Vl(n,e,s){fe(n,e,s,"update")}function yl(n,e,s){fe(n,e,s,"delete")}var pl=(n,e,s)=>{n.call(Pt,e,s),fe(n,e,s,"list")};function Eo(n){return n[0].toLocaleLowerCase()+n.substring(1)}function gl(n,e){return{name:Eo(e||n.name),kind:e?"specified":"automatic"}}var nf=y("actionSegment"),ts=(n,e,s)=>{n.program.stateMap(nf).set(e,s)};function ef(n,e){return n.stateMap(nf).get(e)}var sf=y("actions"),tf=(n,e,s)=>{if(s===""){cn(n.program,{code:"invalid-action-name",target:e});return}let t=gl(e,s);n.call(ts,e,t.name),n.program.stateMap(sf).set(e,t)};function Lh(n,e){return n.stateMap(sf).get(e)}var hf=y("collectionActions"),df=(n,e,s,t)=>{if(s.kind==="TemplateParameter")return;let i=Nl(n.program,s);if(i)n.call(qe,e,i);let h=gl(e,t);n.call(ts,e,h.name),h.name=`${i}/${h.name}`,n.program.stateMap(hf).set(e,h)};function qh(n,e){return n.stateMap(hf).get(e)}var vo=y("resourceLocations"),Wh=(n,e,s)=>{if(s.kind==="TemplateParameter")return;n.program.stateMap(vo).set(e,s)};ce("Private",Wh,ts,ef);function To(n){let e=new Set;function s(t){if(t.name==="")return;let i=new Set,h=t,d=new ke;while(h){if(i.has(h))break;else i.add(h);let f=On(n,h);if(f){let l=zt(n,f.keyProperty);d.track(l,f)}h=ss(n,h)}for(let[f,l]of d.entries())for(let u of l){let a=`${A(u.resourceType)}.${f}`;if(!e.has(a))e.add(a),cn(n,{code:"duplicate-parent-key",format:{resourceName:u.resourceType.name,keyName:f},target:u.keyProperty})}}for(let t of pe(n))Oe(t.type,{model:(i)=>s(i)})}function ko(n){for(let e of pe(n))Oe(e.type,{operation:(s)=>{let t=Lh(n,s);if(zn(n,s)&&(t?.kind==="automatic"||qh(n,s)?.kind==="automatic"))cn(n,{code:"shared-route-unspecified-action-name",target:s,format:{decoratorName:t?"@action":"@collectionAction"}})}})}function lf(n){To(n),ko(n)}var Uo={"TypeSpec.Rest":{autoRoute:Ot,segment:qe,segmentOf:Pt,actionSeparator:jl,resource:Dl,parentResource:$l,readsResource:Cl,createsResource:Ml,createsOrReplacesResource:Bl,createsOrUpdatesResource:Kl,updatesResource:Vl,deletesResource:yl,listsResource:pl,action:tf,collectionAction:df,copyResourceKeyParameters:Zl}};var zh={};an(zh,{namespace:()=>Io,$decorators:()=>qo});var Io="TypeSpec.Rest.Private",ff=y("validatedMissing"),Ao=(n,e,s)=>{if(n.program.stateSet(ff).has(s))return;if((s.kind==="Model"&&On(n.program,s))===void 0)cn(n.program,{code:"resource-missing-key",format:{modelName:A(s)},target:s}),n.program.stateSet(ff).add(s)},uf=y("validatedError"),Lo=(n,e,s)=>{if(n.program.stateSet(uf).has(s))return;if(!(s.kind==="Model"&&Yn(n.program,s)))cn(n.program,{code:"resource-missing-error",format:{modelName:A(s)},target:s}),n.program.stateSet(uf).add(s)},qo={"TypeSpec.Rest.Private":{actionSegment:ts,resourceLocation:Wh,resourceTypeForKeyParam:Jt,validateHasKey:Ao,validateIsError:Lo}};var Ph={};an(Ph,{$lib:()=>Jh,$decorators:()=>Ho});var Jh=F({name:"@typespec/openapi",diagnostics:{"invalid-extension-key":{severity:"error",messages:{default:o`OpenAPI extension must start with 'x-' but was '${"value"}'`}},"duplicate-type-name":{severity:"error",messages:{default:o`Duplicate type name: '${"value"}'. Check @friendlyName decorators and overlap with types in TypeSpec or service namespace.`,parameter:o`Duplicate parameter key: '${"value"}'. Check @friendlyName decorators and overlap with types in TypeSpec or service namespace.`}},"not-url":{severity:"error",messages:{default:o`${"property"}: ${"value"} is not a valid URL.`}},"duplicate-tag":{severity:"error",messages:{default:o`"Metadata for tag '${"tagName"}' was specified twice."`}},"mixed-tag-metadata-form":{severity:"error",messages:{default:'Cannot mix the array form and the inline form of @tagMetadata on the same namespace. Use either @tagMetadata(#[...]) or multiple @tagMetadata("name", #{...}) calls, not both.'}},"tag-metadata-array-with-metadata-arg":{severity:"error",messages:{default:"When using the array form of @tagMetadata, the second argument (tagMetadata) must not be provided. Include all tag metadata inside the array elements."}},"tag-metadata-target-service":{severity:"error",messages:{default:o`@tagMetadata must be used on the service namespace. Did you mean to annotate '${"namespace"}'  with '@service'?`}},"default-response-with-status-code":{severity:"warning",messages:{statusCode:"@defaultResponse should not be used on a model that already has a status code defined. The status code will be ignored in favor of the default response.",error:"@defaultResponse should not be used on a model that is marked with @error. Use either @defaultResponse or @error, not both."}},"license-url-identifier-conflict":{severity:"error",messages:{default:"License 'url' and 'identifier' are mutually exclusive. Specify only one of them."}}},state:{tagsMetadata:{description:"State for the @tagMetadata decorator."}}}),{createDiagnostic:is,reportDiagnostic:An,createStateSymbol:ue,stateKeys:af}=Jh;function Wo(n){return n.startsWith("x-")}function Xt(n,e,s,t){try{return new URL(s),!0}catch{return An(n,{code:"not-url",target:e,format:{property:t,value:s}}),!1}}function Zt(n,e,s,t){let i=n.resolveTypeReference(t)[0];if(s&&i){let h=Go(s,e,i);if(n.reportDiagnostics(h),h.length>0)return!1}return!0}function Go(n,e,s){let t=of(e);return cf(n,e,s,t)}function cf(n,e,s,t){let i=[];for(let h of Object.keys(n)){let d=Te(s,h),f=zo(t,h);if(d){if(d.type.kind==="Model"){let l=of(f?.value),u=cf(n[h],f?.value??e,d.type,l);i.push(...u)}}else if(!Wo(h))i.push(is({code:"invalid-extension-key",format:{value:h},target:f?.id??e}))}return i}function of(n){return n!==void 0&&"kind"in n&&n.kind===K.ObjectLiteral?n:void 0}function zo(n,e){return n?.properties.find((s)=>s.kind===K.ObjectLiteralProperty&&s.id.sv===e)}var[Jo,Qo]=q(ue("operationIds")),Rf=(n,e,s)=>{Qo(n.program,e,s)},Oo=ue("openApiExtension"),xf=(n,e,s,t)=>{U(!t||!mn(t),"OpenAPI extension value must be a value but was a type",n.getArgumentTarget(1));let i=Qh(n.program,t);Zo(n.program,e,s,i)};function Qh(n,e){switch(typeof e){case"string":case"number":case"boolean":return e;case"object":if(e===null)return null;if(Array.isArray(e))return e.map((s)=>Qh(n,s));if(Po(e))return Mn(n,e,e.type);else return Object.fromEntries(Object.entries(e).filter(([,s])=>s!==void 0).map(([s,t])=>[s,Qh(n,t)]));default:return e}}function Po(n){return"entityKind"in n&&n.entityKind==="Value"}function Xo(n,e,s){n.stateMap(_o).set(e,s)}function Zo(n,e,s,t){let i=n.stateMap(Oo),h=i.get(e)??new Map;h.set(s,t),i.set(e,h)}var $o=ue("defaultResponse"),mf=(n,e)=>{return zi(n.program,e,["*"]),n.program.stateSet($o).add(e),{onTargetFinish:()=>{let s=[];for(let t of e.properties.values())if(hn(n.program,t)){s.push(is({code:"default-response-with-status-code",messageId:"statusCode",target:e}));break}if(Yn(n.program,e))s.push(is({code:"default-response-with-status-code",messageId:"error",target:e}));return s}}};var Yo=ue("externalDocs"),Ef=(n,e,s,t)=>{let i={url:s};if(t)i.description=t;n.program.stateMap(Yo).set(e,i)};var _o=ue("info"),vf=(n,e,s)=>{if(s===void 0)return;if(!Zt(n.program,n.getArgumentTarget(0),s,"TypeSpec.OpenAPI.AdditionalInfo"))return;if(s.termsOfService){if(!Xt(n.program,n.getArgumentTarget(0),s.termsOfService,"TermsOfService"))return}if(s.license?.url!==void 0&&s.license?.identifier!==void 0){An(n.program,{code:"license-url-identifier-conflict",target:n.getArgumentTarget(0)});return}Xo(n.program,e,s)};var[wf,bf]=q(af.tagsMetadata),[rf,Fo]=un(ue("tagsMetadataArrayForm")),Tf=(n,e,s,t)=>{if(!e.decorators.some((i)=>i.definition?.name==="@service"&&i.definition?.namespace.name==="TypeSpec")){An(n.program,{code:"tag-metadata-target-service",format:{namespace:e.name},target:n.getArgumentTarget(0)});return}if(typeof s!=="string"){if(t!==void 0){An(n.program,{code:"tag-metadata-array-with-metadata-arg",target:n.getArgumentTarget(1)});return}let i=wf(n.program,e);if(i&&i.length>0||rf(n.program,e)){An(n.program,{code:"mixed-tag-metadata-form",target:n.getArgumentTarget(0)});return}let h=new Set;for(let d of s){if(h.has(d.name)){An(n.program,{code:"duplicate-tag",format:{tagName:d.name},target:n.getArgumentTarget(0)});return}if(h.add(d.name),!Zt(n.program,n.getArgumentTarget(0),d,"TypeSpec.OpenAPI.TagMetadataWithName"))return;if(d.externalDocs?.url){if(!Xt(n.program,n.getArgumentTarget(0),d.externalDocs.url,"externalDocs.url"))return}}Fo(n.program,e),bf(n.program,e,[...s])}else{if(rf(n.program,e)){An(n.program,{code:"mixed-tag-metadata-form",target:n.getArgumentTarget(0)});return}let i=wf(n.program,e)??[];if(i.some((d)=>d.name===s)){An(n.program,{code:"duplicate-tag",format:{tagName:s},target:n.getArgumentTarget(0)});return}let h=t??{};if(!Zt(n.program,n.getArgumentTarget(1),h,"TypeSpec.OpenAPI.TagMetadata"))return;if(h.externalDocs?.url){if(!Xt(n.program,n.getArgumentTarget(1),h.externalDocs.url,"externalDocs.url"))return}i.push({name:s,...h}),bf(n.program,e,i)}};var Ho={"TypeSpec.OpenAPI":{defaultResponse:mf,extension:xf,externalDocs:Ef,info:vf,operationId:Rf,tagMetadata:Tf}};var kf={"@typespec/http":{version:"1.16.0",files:{"package.json":`{
  "name": "@typespec/http",
  "version": "1.16.0",
  "author": "Microsoft Corporation",
  "description": "TypeSpec HTTP protocol binding",
  "homepage": "https://github.com/microsoft/typespec",
  "docusaurusWebsite": "https://typespec.io/docs",
  "readme": "https://github.com/microsoft/typespec/blob/main/README.md",
  "license": "MIT",
  "repository": {
    "type": "git",
    "url": "git+https://github.com/microsoft/typespec.git"
  },
  "bugs": {
    "url": "https://github.com/microsoft/typespec/issues"
  },
  "keywords": [
    "typespec"
  ],
  "type": "module",
  "main": "dist/src/index.js",
  "tspMain": "lib/main.tsp",
  "exports": {
    ".": {
      "typespec": "./lib/main.tsp",
      "types": "./dist/src/index.d.ts",
      "default": "./dist/src/index.js"
    },
    "./testing": {
      "types": "./dist/src/testing/index.d.ts",
      "default": "./dist/src/testing/index.js"
    },
    "./streams": {
      "typespec": "./lib/streams/main.tsp",
      "types": "./dist/src/streams/index.d.ts",
      "default": "./dist/src/streams/index.js"
    },
    "./experimental": {
      "types": "./dist/src/experimental/index.d.ts",
      "default": "./dist/src/experimental/index.js"
    },
    "./experimental/typekit": {
      "types": "./dist/src/experimental/typekit/index.d.ts",
      "default": "./dist/src/experimental/typekit/index.js"
    },
    "./experimental/merge-patch": {
      "import": "./dist/src/experimental/merge-patch/index.js"
    }
  },
  "imports": {
    "#test/*": "./test/*"
  },
  "engines": {
    "node": ">=22.0.0"
  },
  "files": [
    "lib/**/*.tsp",
    "tspconfig.yaml",
    "dist/**",
    "!dist/test/**"
  ],
  "peerDependencies": {
    "@typespec/compiler": "^1.16.0",
    "@typespec/streams": "^0.86.0"
  },
  "peerDependenciesMeta": {
    "@typespec/streams": {
      "optional": true
    }
  },
  "devDependencies": {
    "@types/node": "^26.3.0",
    "@vitest/coverage-v8": "^4.1.11",
    "@vitest/ui": "^4.1.11",
    "rimraf": "^6.1.3",
    "typescript": "~6.0.2",
    "vitest": "^4.1.11",
    "@typespec/library-linter": "^0.86.0",
    "@typespec/streams": "^0.86.0",
    "@typespec/tspd": "^0.77.1",
    "@typespec/compiler": "^1.16.0"
  },
  "scripts": {
    "clean": "rimraf ./dist ./temp",
    "build": "pnpm gen-extern-signature && tsc -p tsconfig.build.json && pnpm lint-typespec-library",
    "api-extractor": "api-extractor run --local --verbose",
    "watch": "tsc -p tsconfig.build.json --watch",
    "gen-extern-signature": "tspd --enable-experimental gen-extern-signature .",
    "lint-typespec-library": "tsp compile . --warn-as-error --import @typespec/library-linter --no-emit",
    "test": "vitest run",
    "test:watch": "vitest -w",
    "test:ui": "vitest --ui",
    "test:ci": "vitest run --coverage --reporter=junit --reporter=default",
    "lint": "oxlint . --deny-warnings",
    "lint:fix": "oxlint . --fix",
    "regen-docs": "tspd doc .  --enable-experimental --typekits --llmstxt --output-dir ../../website/src/content/docs/docs/libraries/http/reference --rules-dir ../rules"
  }
}`,"lib/auth.tsp":`namespace TypeSpec.Http;

@doc("Authentication type")
enum AuthType {
  @doc("HTTP")
  http,

  @doc("API key")
  apiKey,

  @doc("OAuth2")
  oauth2,

  @doc("OpenID connect")
  openIdConnect,

  @doc("Empty auth")
  noAuth,
}

/**
 * Basic authentication is a simple authentication scheme built into the HTTP protocol.
 * The client sends HTTP requests with the Authorization header that contains the word Basic word followed by a space and a base64-encoded string username:password.
 * For example, to authorize as demo / \`p@55w0rd\` the client would send
 * \`\`\`
 * Authorization: Basic ZGVtbzpwQDU1dzByZA==
 * \`\`\`
 */
@doc("")
model BasicAuth {
  @doc("Http authentication")
  type: AuthType.http;

  @doc("basic auth scheme")
  scheme: "Basic";
}

/**
 * Bearer authentication (also called token authentication) is an HTTP authentication scheme that involves security tokens called bearer tokens.
 * The name “Bearer authentication” can be understood as “give access to the bearer of this token.” The bearer token is a cryptic string, usually generated by the server in response to a login request.
 * The client must send this token in the Authorization header when making requests to protected resources:
 * \`\`\`
 * Authorization: Bearer <token>
 * \`\`\`
 */
@doc("")
model BearerAuth {
  @doc("Http authentication")
  type: AuthType.http;

  @doc("bearer auth scheme")
  scheme: "Bearer";
}

@doc("Describes the location of the API key")
enum ApiKeyLocation {
  @doc("API key is a header value")
  header,

  @doc("API key is a query parameter")
  query,

  @doc("API key is found in a cookie")
  cookie,
}

/**
 * An API key is a token that a client provides when making API calls. The key can be sent in the query string:
 *
 * \`\`\`
 * GET /something?api_key=abcdef12345
 * \`\`\`
 *
 * or as a request header
 *
 * \`\`\`
 * GET /something HTTP/1.1
 * X-API-Key: abcdef12345
 * \`\`\`
 *
 * or as a cookie
 *
 * \`\`\`
 * GET /something HTTP/1.1
 * Cookie: X-API-KEY=abcdef12345
 * \`\`\`
 *
 * @template Location The location of the API key
 * @template Name The name of the API key
 */
@doc("")
model ApiKeyAuth<Location extends ApiKeyLocation, Name extends string> {
  @doc("API key authentication")
  type: AuthType.apiKey;

  @doc("location of the API key")
  in: Location;

  @doc("name of the API key")
  name: Name;
}

/**
 * OAuth 2.0 is an authorization protocol that gives an API client limited access to user data on a web server.
 *
 * OAuth relies on authentication scenarios called flows, which allow the resource owner (user) to share the protected content from the resource server without sharing their credentials.
 * For that purpose, an OAuth 2.0 server issues access tokens that the client applications can use to access protected resources on behalf of the resource owner.
 * For more information about OAuth 2.0, see oauth.net and RFC 6749.
 *
 * @template Flows The list of supported OAuth2 flows
 * @template Scopes The list of OAuth2 scopes, which are common for every flow from \`Flows\`. This list is combined with the scopes defined in specific OAuth2 flows.
 */
@doc("")
model OAuth2Auth<Flows extends OAuth2Flow[], Scopes extends string[] = []> {
  @doc("OAuth2 authentication")
  type: AuthType.oauth2;

  @doc("Supported OAuth2 flows")
  flows: Flows;

  @doc("Oauth2 scopes of every flow. Overridden by scope definitions in specific flows")
  defaultScopes: Scopes;
}

@doc("Describes the OAuth2 flow type")
enum OAuth2FlowType {
  @doc("authorization code flow")
  authorizationCode,

  @doc("implicit flow")
  implicit,

  @doc("password flow")
  password,

  @doc("client credential flow")
  clientCredentials,
}

alias OAuth2Flow = AuthorizationCodeFlow | ImplicitFlow | PasswordFlow | ClientCredentialsFlow;

@doc("Authorization Code flow")
model AuthorizationCodeFlow {
  @doc("authorization code flow")
  type: OAuth2FlowType.authorizationCode;

  @doc("the authorization URL")
  authorizationUrl: string;

  @doc("the token URL")
  tokenUrl: string;

  @doc("the refresh URL")
  refreshUrl?: string;

  @doc("list of scopes for the credential")
  scopes?: string[];
}

@doc("Implicit flow")
model ImplicitFlow {
  @doc("implicit flow")
  type: OAuth2FlowType.implicit;

  @doc("the authorization URL")
  authorizationUrl: string;

  @doc("the refresh URL")
  refreshUrl?: string;

  @doc("list of scopes for the credential")
  scopes?: string[];
}

@doc("Resource Owner Password flow")
model PasswordFlow {
  @doc("password flow")
  type: OAuth2FlowType.password;

  @doc("the token URL")
  tokenUrl: string;

  @doc("the refresh URL")
  refreshUrl?: string;

  @doc("list of scopes for the credential")
  scopes?: string[];
}

@doc("Client credentials flow")
model ClientCredentialsFlow {
  @doc("client credential flow")
  type: OAuth2FlowType.clientCredentials;

  @doc("the token URL")
  tokenUrl: string;

  @doc("the refresh URL")
  refreshUrl?: string;

  @doc("list of scopes for the credential")
  scopes?: string[];
}

/**
 * OpenID Connect (OIDC) is an identity layer built on top of the OAuth 2.0 protocol and supported by some OAuth 2.0 providers, such as Google and Azure Active Directory.
 * It defines a sign-in flow that enables a client application to authenticate a user, and to obtain information (or "claims") about that user, such as the user name, email, and so on.
 * User identity information is encoded in a secure JSON Web Token (JWT), called ID token.
 * OpenID Connect defines a discovery mechanism, called OpenID Connect Discovery, where an OpenID server publishes its metadata at a well-known URL, typically
 *
 * \`\`\`http
 * https://server.com/.well-known/openid-configuration
 * \`\`\`
 *
 * @template ConnectUrl The openIdConnectUrl, where the OpenID provider publishes its discovery metadata. It can be specified relative to the server URL.
 * @template Scopes The scope names required for operations that use this scheme.
 */
model OpenIdConnectAuth<ConnectUrl extends string, Scopes extends string[] = []> {
  /** Auth type */
  type: AuthType.openIdConnect;

  /** Connect url. It can be specified relative to the server URL */
  openIdConnectUrl: ConnectUrl;

  /** Scope names required for operations that use this authentication scheme. */
  scopes: Scopes;
}

/**
 * This authentication option signifies that API is not secured at all.
 * It might be useful when overriding authentication on interface of operation level.
 */
@doc("")
model NoAuth {
  /** No authentication. */
  type: AuthType.noAuth;
}
`,"lib/decorators.tsp":`namespace TypeSpec.Http;

using TypeSpec.Reflection;

/**
 * Header options.
 */
model HeaderOptions {
  /**
   * Name of the header when sent over HTTP.
   */
  name?: string;

  /**
   * Equivalent of adding \`*\` in the path parameter as per [RFC-6570](https://datatracker.ietf.org/doc/html/rfc6570#section-3.2.3)
   *
   *  | Style  | Explode | Primitive value = 5 | Array = [3, 4, 5] | Object = {"role": "admin", "firstName": "Alex"} |
   *  | ------ | ------- | ------------------- | ----------------- | ----------------------------------------------- |
   *  | simple | false   | \`5   \`              | \`3,4,5\`           | \`role,admin,firstName,Alex\`                     |
   *  | simple | true    | \`5\`                 | \`3,4,5\`           | \`role=admin,firstName=Alex\`                     |
   *
   */
  explode?: boolean;
}

/**
 * Specify this property is to be sent or received as an HTTP header.
 *
 * @param headerNameOrOptions Optional name of the header when sent over HTTP or header options.
 *  By default the header name will be the property name converted from camelCase to kebab-case. (e.g. \`contentType\` -> \`content-type\`)
 *
 * @example
 *
 * \`\`\`typespec
 * op read(@header accept: string): {@header("ETag") eTag: string};
 * op create(@header({name: "X-Color", format: "csv"}) colors: string[]): void;
 * \`\`\`
 *
 * @example Implicit header name
 *
 * \`\`\`typespec
 * op read(): {@header contentType: string}; // headerName: content-type
 * op update(@header ifMatch: string): void; // headerName: if-match
 * \`\`\`
 */
extern dec header(target: ModelProperty, headerNameOrOptions?: valueof string | HeaderOptions);

/**
 * Cookie Options.
 */
model CookieOptions {
  /**
   * Name in the cookie.
   */
  name?: string;
}

/**
 * Specify this property is to be sent or received in the cookie.
 *
 * @param cookieNameOrOptions Optional name of the cookie in the cookie or cookie options.
 *  By default the cookie name will be the property name converted from camelCase to snake_case. (e.g. \`authToken\` -> \`auth_token\`)
 *
 * @example
 *
 * \`\`\`typespec
 * op read(@cookie token: string): {data: string[]};
 * op create(@cookie({name: "auth_token"}) data: string[]): void;
 * \`\`\`
 *
 * @example Implicit header name
 *
 * \`\`\`typespec
 * op read(): {@cookie authToken: string}; // headerName: auth_token
 * op update(@cookie AuthToken: string): void; // headerName: auth_token
 * \`\`\`
 */
extern dec cookie(target: ModelProperty, cookieNameOrOptions?: valueof string | CookieOptions);

/**
 * Query parameter options.
 */
model QueryOptions {
  /**
   * Name of the query when included in the url.
   */
  name?: string;

  /**
   * If true send each value in the array/object as a separate query parameter.
   * Equivalent of adding \`*\` in the path parameter as per [RFC-6570](https://datatracker.ietf.org/doc/html/rfc6570#section-3.2.3)
   *
   *  | Style  | Explode | Uri Template   | Primitive value id = 5 | Array id = [3, 4, 5]    | Object id = {"role": "admin", "firstName": "Alex"} |
   *  | ------ | ------- | -------------- | ---------------------- | ----------------------- | -------------------------------------------------- |
   *  | simple | false   | \`/users{?id}\`  | \`/users?id=5\`          | \`/users?id=3,4,5\`       | \`/users?id=role,admin,firstName,Alex\`              |
   *  | simple | true    | \`/users{?id*}\` | \`/users?id=5\`          | \`/users?id=3&id=4&id=5\` | \`/users?role=admin&firstName=Alex\`                 |
   *
   */
  explode?: boolean;
}

/**
 * Specify this property is to be sent as a query parameter.
 *
 * @param queryNameOrOptions Optional name of the query when included in the url or query parameter options.
 *
 * @example
 *
 * \`\`\`typespec
 * op read(@query select: string, @query("order-by") orderBy: string): void;
 * op list(@query(#{name: "id", explode: true}) ids: string[]): void;
 * \`\`\`
 */
extern dec query(target: ModelProperty, queryNameOrOptions?: valueof string | QueryOptions);

/**
 * Options for configuring how a path parameter is serialized into the URI template.
 */
model PathOptions {
  /** Name of the parameter in the URI template. */
  name?: string;

  /**
   * When interpolating this parameter in the case of array or object expand each value using the given style.
   * Equivalent of adding \`*\` in the path parameter as per [RFC-6570](https://datatracker.ietf.org/doc/html/rfc6570#section-3.2.3)
   */
  explode?: boolean;

  /**
   * Different interpolating styles for the path parameter.
   * - \`simple\`: No special encoding.
   * - \`label\`: Using \`.\` separator.
   * - \`matrix\`: \`;\` as separator.
   * - \`fragment\`: \`#\` as separator.
   * - \`path\`: \`/\` as separator.
   */
  style?: "simple" | "label" | "matrix" | "fragment" | "path";

  /**
   * When interpolating this parameter do not encode reserved characters.
   * Equivalent of adding \`+\` in the path parameter as per [RFC-6570](https://datatracker.ietf.org/doc/html/rfc6570#section-3.2.3)
   */
  allowReserved?: boolean;
}

/**
 * Explicitly specify that this property is to be interpolated as a path parameter.
 *
 * @param paramNameOrOptions Optional name of the parameter in the URI template or options.
 *
 * @example
 *
 * \`\`\`typespec
 * @route("/read/{explicit}/things/{implicit}")
 * op read(@path explicit: string, implicit: string): void;
 * \`\`\`
 */
extern dec path(target: ModelProperty, paramNameOrOptions?: valueof string | PathOptions);

/**
 * Explicitly specify that this property type will be exactly the HTTP body.
 *
 * This means that any properties under \`@body\` cannot be marked as headers, query parameters, or path parameters.
 * If wanting to change the resolution of the body but still mix parameters, use \`@bodyRoot\`.
 *
 * @example
 *
 * \`\`\`typespec
 * op upload(@body image: bytes): void;
 * op download(): {@body image: bytes};
 * \`\`\`
 */
extern dec body(target: ModelProperty);

/**
 * Specify that the body resolution should be resolved from that property.
 * By default the body is resolved by including all properties in the operation request/response that are not metadata.
 * This allows to nest the body in a property while still allowing to use headers, query parameters, and path parameters in the same model.
 *
 * @example
 *
 * \`\`\`typespec
 * op upload(@bodyRoot user: {name: string, @header id: string}): void;
 * op download(): {@bodyRoot user: {name: string, @header id: string}};
 * \`\`\`
 */
extern dec bodyRoot(target: ModelProperty);
/**
 * Specify that this property shouldn't be included in the HTTP body.
 * This can be useful when bundling metadata together that would result in an empty property to be included in the body.
 *
 * @example
 *
 * \`\`\`typespec
 * op upload(name: string, @bodyIgnore headers: {@header id: string}): void;
 * \`\`\`
 */
extern dec bodyIgnore(target: ModelProperty);

/**
 * Specify that the target property is the body of a multipart request or response.
 *
 * The property type must be a model or tuple whose members are all \`HttpPart\`, each describing one
 * part of the payload.
 *
 * @example
 *
 * \`\`\`tsp
 * op upload(
 *   @header \`content-type\`: "multipart/form-data",
 *   @multipartBody body: {
 *     fullName: HttpPart<string>;
 *     headShots: HttpPart<Image>[];
 *   },
 * ): void;
 * \`\`\`
 */
extern dec multipartBody(target: ModelProperty);

/**
 * Specify the status code for this response. Property type must be a status code integer or a union of status code integer.
 *
 * @example
 *
 * \`\`\`typespec
 * op read(): {
 *   @statusCode _: 200;
 *   @body pet: Pet;
 * };
 * op create(): {
 *   @statusCode _: 201 | 202;
 * };
 * \`\`\`
 */
extern dec statusCode(target: ModelProperty);

/**
 * Specify the HTTP verb for the target operation to be \`GET\`.
 *
 * @example
 *
 * \`\`\`typespec
 * @get op read(): string
 * \`\`\`
 */
extern dec get(target: Operation);

/**
 * Specify the HTTP verb for the target operation to be \`PUT\`.
 *
 * @example
 *
 * \`\`\`typespec
 * @put op set(pet: Pet): void
 * \`\`\`
 */
extern dec put(target: Operation);

/**
 * Specify the HTTP verb for the target operation to be \`POST\`.
 *
 * @example
 *
 * \`\`\`typespec
 * @post op create(pet: Pet): void
 * \`\`\`
 */
extern dec post(target: Operation);

/**
 * Options for PATCH operations.
 */
model PatchOptions {
  /**
   * If set to \`false\`, disables the implicit transform that makes the body of a
   * PATCH operation deeply optional.
   *
   * **Deprecated:** \`implicitOptionality\` is deprecated and will be removed.
   * To preserve the previous behavior, define and use an explicit patch model
   * with optional properties for your \`@body\` parameter.
   * For actual JSON Merge Patch behavior, use \`MergePatchUpdate<T>\` as the
   * \`@body\` type (for example: \`@patch op update(@body pet: MergePatchUpdate<Pet>): void;\`).
   */
  implicitOptionality?: boolean;
}

/**
 * Specify the HTTP verb for the target operation to be \`PATCH\`.
 *
 * @param options Options for the PATCH operation.
 *
 * @example
 *
 * \`\`\`typespec
 * @patch op update(pet: Pet): void;
 * \`\`\`
 *
 * @example Using MergePatch template for proper merge-patch semantics
 * \`\`\`typespec
 * @patch op update(@body pet: MergePatchUpdate<Pet>): void;
 * \`\`\`
 */
extern dec patch(target: Operation, options?: valueof PatchOptions);

/**
 * Specify the HTTP verb for the target operation to be \`DELETE\`.
 *
 * @example
 *
 * \`\`\`typespec
 * @delete op set(petId: string): void
 * \`\`\`
 */
extern dec delete(target: Operation);

/**
 * Specify the HTTP verb for the target operation to be \`HEAD\`.
 * @example
 *
 * \`\`\`typespec
 * @head op ping(petId: string): void
 * \`\`\`
 */
extern dec head(target: Operation);

/**
 * Specify an endpoint for this service. Multiple \`@server\` decorators can be used to specify multiple endpoints.
 *
 *  @param url Server endpoint
 *  @param description Description of the endpoint
 *  @param parameters Optional set of parameters used to interpolate the url.
 *
 * @example
 *
 * \`\`\`typespec
 * @service
 * @server("https://example.com")
 * namespace PetStore;
 * \`\`\`
 *
 * @example With a description
 *
 * \`\`\`typespec
 * @service
 * @server("https://example.com", "Single server endpoint")
 * namespace PetStore;
 * \`\`\`
 *
 * @example Parameterized
 *
 * \`\`\`typespec
 * @server("https://{region}.foo.com", "Regional endpoint", {
 *   @doc("Region name")
 *   region?: string = "westus",
 * })
 * \`\`\`
 *
 * @example Multiple
 * \`\`\`typespec
 * @service
 * @server("https://example.com", "Standard endpoint")
 * @server("https://{project}.private.example.com", "Private project endpoint", {
 *   project: string;
 * })
 * namespace PetStore;
 * \`\`\`
 *
 */
extern dec server(
  target: Namespace,
  url: valueof string,
  description?: valueof string,
  parameters?: Record<unknown>
);

/**
 * Specify authentication for a whole service or specific methods. See the [documentation in the Http library](https://typespec.io/docs/libraries/http/authentication) for full details.
 *
 * @param auth Authentication configuration. Can be a single security scheme, a union(either option is valid authentication) or a tuple (must use all authentication together)
 * @example
 *
 * \`\`\`typespec
 * @service
 * @useAuth(BasicAuth)
 * namespace PetStore;
 * \`\`\`
 */
extern dec useAuth(target: Namespace | Interface | Operation, auth: {} | Union | {}[]);

/**
 * Defines the relative route URI template for the target operation as defined by [RFC 6570](https://datatracker.ietf.org/doc/html/rfc6570#section-3.2.3)
 *
 * \`@route\` can only be applied to operations, namespaces, and interfaces.
 *
 * @param path URI template for this operation.
 *
 * @example Simple path parameter
 *
 * \`\`\`typespec
 * @route("/widgets/{id}") op getWidget(@path id: string): Widget;
 * \`\`\`
 *
 * @example Reserved characters
 * \`\`\`typespec
 * @route("/files{+path}") op getFile(@path path: string): bytes;
 * \`\`\`
 *
 * @example Query parameter
 * \`\`\`typespec
 * @route("/files") op list(select?: string, filter?: string): Files[];
 * @route("/files{?select,filter}") op listFullUriTemplate(select?: string, filter?: string): Files[];
 * \`\`\`
 */
extern dec route(target: Namespace | Interface | Operation, path: valueof string);

/**
 * \`@sharedRoute\` marks the operation as sharing a route path with other operations.
 *
 * When an operation is marked with \`@sharedRoute\`, it enables other operations to share the same
 * route path as long as those operations are also marked with \`@sharedRoute\`.
 *
 * \`@sharedRoute\` can only be applied directly to operations.
 *
 * \`\`\`typespec
 * @sharedRoute
 * @route("/widgets")
 * op getWidget(@path id: string): Widget;
 * \`\`\`
 */
extern dec sharedRoute(target: Operation);
`,"lib/main.tsp":`import "../dist/src/tsp-index.js";
import "./decorators.tsp";
import "./private.decorators.tsp";
import "./auth.tsp";

namespace TypeSpec.Http;

using Private;

/**
 * Describes an HTTP response.
 *
 * @template Status The status code of the response.
 */
@doc("")
model Response<Status> {
  @doc("The status code.")
  @statusCode
  statusCode: Status;
}

/**
 * Defines a model with a single property of the given type, marked with \`@body\`.
 *
 * This can be useful in situations where you cannot use a bare type as the body
 * and it is awkward to add a property.
 *
 * @template Type The type of the model's \`body\` property.
 */
@doc("")
model Body<Type> {
  @body
  @doc("The body type of the operation request or response.")
  body: Type;
}

/**
 * The Location header contains the URL where the status of the long running operation can be checked.
 */
model LocationHeader {
  @doc("The Location header contains the URL where the status of the long running operation can be checked.")
  @header
  location: string;
}

// Don't put @doc on these, change \`getStatusCodeDescription\` implementation
// to update the default descriptions for these status codes. This ensures
// that we get consistent emit between different ways to spell the same
// responses in TypeSpec.

/**
 * The request has succeeded.
 */
model OkResponse is Response<200>;
/**
 * The request has succeeded and a new resource has been created as a result.
 */
model CreatedResponse is Response<201>;
/**
 * The request has been accepted for processing, but processing has not yet completed.
 */
model AcceptedResponse is Response<202>;
/**
 * There is no content to send for this request, but the headers may be useful.
 */
model NoContentResponse is Response<204>;
/**
 * The URL of the requested resource has been changed permanently. The new URL is given in the response.
 */
model MovedResponse is Response<301> {
  ...LocationHeader;
}
/**
 * The client has made a conditional request and the resource has not been modified.
 */
model NotModifiedResponse is Response<304>;
/**
 * The server could not understand the request due to invalid syntax.
 */
model BadRequestResponse is Response<400>;
/**
 * Access is unauthorized.
 */
model UnauthorizedResponse is Response<401>;
/**
 * Access is forbidden.
 */
model ForbiddenResponse is Response<403>;
/**
 * The server cannot find the requested resource.
 */
model NotFoundResponse is Response<404>;
/**
 * The request conflicts with the current state of the server.
 */
model ConflictResponse is Response<409>;

/**
 * Produces a new model with the same properties as T, but with \`@query\`,
 * \`@header\`, \`@body\`, and \`@path\` decorators removed from all properties.
 *
 * @template Data The model to spread as the plain data.
 */
@plainData
model PlainData<Data> {
  ...Data;
}

/**
 * A file in an HTTP request, response, or multipart payload.
 *
 * Files have a special meaning that the HTTP library understands. When the body of an HTTP request, response,
 * or multipart payload is _effectively_ an instance of \`TypeSpec.Http.File\` or any type that extends it, the
 * operation is treated as a file upload or download.
 *
 * When using file bodies, the fields of the file model are defined to come from particular locations by default:
 *
 * - \`contentType\`: The \`Content-Type\` header of the request, response, or multipart payload (CANNOT be overridden or changed).
 * - \`contents\`: The body of the request, response, or multipart payload (CANNOT be overridden or changed).
 * - \`filename\`: The \`filename\` parameter value of the \`Content-Disposition\` header of the response or multipart payload
 *   (MAY be overridden or changed).
 *
 * A File may be used as a normal structured JSON object in a request or response, if the request specifies an explicit
 * \`Content-Type\` header. In this case, the entire File model is serialized as if it were any other model. In a JSON payload,
 * it will have a structure like:
 *
 * \`\`\`
 * {
 *   "contentType": <string?>,
 *   "filename": <string?>,
 *   "contents": <string, base64>
 * }
 * \`\`\`
 *
 * The \`contentType\` _within_ the file defines what media types the data inside the file can be, but if the specification
 * defines a \`Content-Type\` for the payload as HTTP metadata, that \`Content-Type\` metadata defines _how the file is
 * serialized_. See the examples below for more information.
 *
 * NOTE: The \`filename\` and \`contentType\` fields are optional. Furthermore, the default location of \`filename\`
 * (\`Content-Disposition: <disposition>; filename=<filename>\`) is only valid in HTTP responses and multipart payloads. If
 * you wish to send the \`filename\` in a request, you must use HTTP metadata decorators to describe the location of the
 * \`filename\` field. You can combine the metadata decorators with \`@visibility\` to control when the \`filename\` location
 * is overridden, as shown in the examples below.
 *
 * @template ContentType The allowed media (MIME) types of the file contents.
 * @template Contents The type of the file contents. This can be \`string\`, \`bytes\`, or any scalar that extends them.
 *
 * @example
 * \`\`\`tsp
 * // Download a file
 * @get op download(): File;
 *
 * // Upload a file
 * @post op upload(@bodyRoot file: File): void;
 * \`\`\`
 *
 * @example
 * \`\`\`tsp
 * // Upload and download files in a multipart payload
 * op multipartFormDataUpload(
 *   @multipartBody fields: {
 *     files: HttpPart<File>[];
 *   },
 * ): void;
 *
 * op multipartFormDataDownload(): {
 *   @multipartBody formFields: {
 *     files: HttpPart<File>[];
 *   }
 * };
 * \`\`\`
 *
 * @example
 * \`\`\`tsp
 * // Declare a custom type of text file, where the filename goes in the path
 * // in requests.
 * model SpecFile extends File<"application/json" | "application/yaml", string> {
 *   // Provide a header that contains the name of the file when created or updated
 *   @header("x-filename")
 *   @path filename: string;
 * }
 *
 * @get op downloadSpec(@path name: string): SpecFile;
 *
 * @post op uploadSpec(@bodyRoot spec: SpecFile): void;
 * \`\`\`
 *
 * @example
 * \`\`\`tsp
 * // Declare a custom type of binary file
 * model ImageFile extends File {
 *   contentType: "image/png" | "image/jpeg";
 *   @path filename: string;
 * }
 *
 * @get op downloadImage(@path name: string): ImageFile;
 *
 * @post op uploadImage(@bodyRoot image: ImageFile): void;
 * \`\`\`
 *
 * @example
 * \`\`\`tsp
 * // Use a File as a structured JSON object. The HTTP library will warn you that the File will be serialized as JSON,
 * // so you should suppress the warning if it's really what you want instead of a binary file upload/download.
 *
 * // The response body is a JSON object like \`{"contentType":<string?>,"filename":<string?>,"contents":<string>}\`
 * @get op downloadTextFileJson(): {
 *   @header contentType: "application/json",
 *   @body file: File<"text/plain", string>,
 * };
 *
 * // The request body is a JSON object like \`{"contentType":<string?>,"filename":<string?>,"contents":<base64>}\`
 * @post op uploadBinaryFileJson(
 *   @header contentType: "application/json",
 *   @body file: File<"image/png", bytes>,
 * ): void;
 *
 */
@summary("A file in an HTTP request, response, or multipart payload.")
@Private.httpFile
model File<ContentType extends string = string, Contents extends bytes | string = bytes> {
  /**
   * The allowed media (MIME) types of the file contents.
   *
   * In file bodies, this value comes from the \`Content-Type\` header of the request or response. In JSON bodies,
   * this value is serialized as a field in the response.
   *
   * NOTE: this is not _necessarily_ the same as the \`Content-Type\` header of the request or response, but
   * it will be for file bodies. It may be different if the file is serialized as a JSON object. It always refers to the
   * _contents_ of the file, and not necessarily the way the file itself is transmitted or serialized.
   */
  @summary("The allowed media (MIME) types of the file contents.")
  contentType?: ContentType;

  /**
   * The name of the file, if any.
   *
   * In file bodies, this value comes from the \`filename\` parameter of the \`Content-Disposition\` header of the response
   * or multipart payload. In JSON bodies, this value is serialized as a field in the response.
   *
   * NOTE: By default, \`filename\` cannot be sent in request payloads and can only be sent in responses and multipart
   * payloads, as the \`Content-Disposition\` header is not valid in requests. If you want to send the \`filename\` in a request,
   * you must extend the \`File\` model and override the \`filename\` property with a different location defined by HTTP metadata
   * decorators.
   */
  @summary("The name of the file, if any.")
  filename?: string;

  /**
   * The contents of the file.
   *
   * In file bodies, this value comes from the body of the request, response, or multipart payload. In JSON bodies,
   * this value is serialized as a field in the response.
   */
  @summary("The contents of the file.")
  contents: Contents;
}

/**
 * Options for configuring an individual part of a multipart payload.
 */
model HttpPartOptions {
  /** Name of the part when using the array form. */
  name?: string;
}

/**
 * Represents a single part of a multipart payload.
 *
 * @template Type The type of the part's content.
 * @template Options Options for this part, such as the name to use when the part is repeated.
 *
 * @example
 *
 * \`\`\`typespec
 * op upload(
 *   @header \`content-type\`: "multipart/form-data",
 *   @multipartBody body: {
 *     fullName: HttpPart<string>;
 *     headShots: HttpPart<Image>[];
 *   },
 * ): void;
 * \`\`\`
 */
@Private.httpPart(Type, Options)
model HttpPart<Type, Options extends valueof HttpPartOptions = #{}> {}

/**
 * Describes a web link as defined by [RFC 8288](https://datatracker.ietf.org/doc/html/rfc8288).
 */
model Link {
  /** The target URI of the link. */
  target: url;

  /** The relation type of the link, describing how the target relates to the current resource. */
  rel: string;

  /** Additional target attributes to serialize as link parameters. */
  attributes?: Record<unknown>;
}

/**
 * A \`Link\` header value as defined by [RFC 8288](https://datatracker.ietf.org/doc/html/rfc8288).
 *
 * @template T The links carried by the header, either as a map of relation type to URI or as a
 * list of \`Link\`.
 */
scalar LinkHeader<T extends Record<url> | Link[]> extends string;

/**
 * Create a MergePatch Request body for updating the given resource Model.
 * The MergePatch request created by this template provides a TypeSpec description of a
 * JSON MergePatch request that can successfully update the given resource.
 * The transformation follows the definition of JSON MergePatch requests in
 * rfc 7396: https://www.rfc-editor.org/rfc/rfc7396,
 * applying the merge-patch transform recursively to keyed types in the resource Model.
 *
 * Using this template in a PATCH request body overrides the \`implicitOptionality\`
 * setting for PATCH operations and sets \`application/merge-patch+json\` as the request
 * content-type.
 *
 * @template T The type of the resource to create a MergePatch update request body for.
 * @template NameTemplate A StringTemplate used to name any models created by applying
 * the merge-patch transform to the resource. The default name template is \`{name}MergePatchUpdate\`,
 * for example, the merge patch transform of model \`Widget\` is named \`WidgetMergePatchUpdate\`.
 *
 * @example
 * \`\`\`tsp
 * // An operation updating a 'Widget' using merge-patch
 * @patch op update(@body request: MergePatchUpdate<Widget>): Widget;
 * \`\`\`
 *
 * @example
 * \`\`\`tsp
 * // An operation updating a 'Widget' using merge-patch
 * @patch op update(@bodyRoot request: MergePatchUpdate<Widget>): Widget;
 * \`\`\`
 *
 * @example
 * \`\`\`tsp
 * // An operation updating a 'Widget' using merge-patch
 * @patch op update(...MergePatchUpdate<Widget>): Widget;
 * \`\`\`
 */
alias MergePatchUpdate<
  T extends Reflection.Model,
  NameTemplate extends valueof string = "{name}MergePatchUpdate"
> = applyMergePatchTransform(
  T,
  NameTemplate,
  #{ visibilityMode: Private.MergePatchVisibilityMode.Update }
);

/**
 * Create a MergePatch Request body for creating or updating the given resource Model.
 * The MergePatch request created by this template provides a TypeSpec description of a
 * JSON MergePatch request that can successfully create or update the given resource.
 * The transformation follows the definition of JSON MergePatch requests in
 * rfc 7396: https://www.rfc-editor.org/rfc/rfc7396,
 * applying the merge-patch transform recursively to keyed types in the resource Model.
 *
 * Using this template in a PATCH request body overrides the \`implicitOptionality\`
 * setting for PATCH operations and sets \`application/merge-patch+json\` as the request
 * content-type.
 *
 * @template T The type of the resource to create a MergePatch update request body for.
 * @template NameTemplate A StringTemplate used to name any models created by applying
 * the merge-patch transform to the resource. The default name template is \`{name}MergePatchCreateOrUpdate\`,
 * for example, the merge patch transform of model \`Widget\` is named \`WidgetMergePatchCreateOrUpdate\`.
 *
 * @example
 * \`\`\`tsp
 * // An operation updating a 'Widget' using merge-patch
 * @patch op update(@body request: MergePatchCreateOrUpdate<Widget>): Widget;
 * \`\`\`
 *
 * @example
 * \`\`\`tsp
 * // An operation updating a 'Widget' using merge-patch
 * @patch op update(@bodyRoot request: MergePatchCreateOrUpdate<Widget>): Widget;
 * \`\`\`
 *
 * @example
 * \`\`\`tsp
 * // An operation updating a 'Widget' using merge-patch
 * @patch op update(...MergePatchCreateOrUpdate<Widget>): Widget;
 * \`\`\`
 */
alias MergePatchCreateOrUpdate<
  T extends Reflection.Model,
  NameTemplate extends valueof string = "{name}MergePatchCreateOrUpdate"
> = applyMergePatchTransform(
  T,
  NameTemplate,
  #{ visibilityMode: Private.MergePatchVisibilityMode.CreateOrUpdate }
);
`,"lib/private.decorators.tsp":`/**
 * Private decorators. Those are meant for internal use inside Http types only.
 */
namespace TypeSpec.Http.Private;

extern dec plainData(target: TypeSpec.Reflection.Model);
extern dec httpFile(target: TypeSpec.Reflection.Model);
extern dec httpPart(
  target: TypeSpec.Reflection.Model,
  type: unknown,
  options: valueof HttpPartOptions
);

/**
 * Specify if inapplicable metadata should be included in the payload for the given entity.
 * @param value If true, inapplicable metadata will be included in the payload.
 */
extern dec includeInapplicableMetadataInPayload(target: unknown, value: valueof boolean);

/**
 * The visibility mode for the merge patch transform.
 */
enum MergePatchVisibilityMode {
  /**
   * The Update mode. This is used when a resource can be updated but must already exist.
   */
  Update,

  /**
   * The Create or Update mode. This is used when a resource can be created OR updated in a single operation.
   */
  CreateOrUpdate,
}

/**
 * Options for the \`@applyMergePatch\` decorator.
 */
model ApplyMergePatchOptions {
  /**
   * The visibility mode to use.
   */
  visibilityMode: MergePatchVisibilityMode;
}

/**
 * Performs the canonical merge-patch transformation on the given model and injects its
 * transformed properties into the target.
 */
#deprecated "applyMergePatch is deprecated and will be removed in a future release. This decorator is not intended for public use."
extern dec applyMergePatch(
  target: Reflection.Model,
  source: Reflection.Model,
  nameTemplate: valueof string,
  options: valueof ApplyMergePatchOptions
);

#suppress "experimental-feature"
internal extern fn applyMergePatchTransform(
  input: Reflection.Model,
  nameTemplate: valueof string,
  options: valueof ApplyMergePatchOptions
): Reflection.Model;

/**
 * Marks a model that was generated by applying the MergePatch
 * transform and links to its source model
 */
extern dec mergePatchModel(target: Reflection.Model, source: Reflection.Model);

/**
 * Links a modelProperty mutated as part of a mergePatch transform to
 * its source property;
 */
extern dec mergePatchProperty(target: Reflection.ModelProperty, source: Reflection.ModelProperty);
`,"lib/streams/main.tsp":`import "@typespec/streams";
import "../main.tsp";

using TypeSpec.Streams;

namespace TypeSpec.Http.Streams;

/**
 * Defines a model that represents a stream protocol type whose data is described
 * by \`Type\`.
 *
 * The \`ContentType\` and \`BodyType\` describe how the stream is encoded over the wire,
 * while \`Type\` describes the data that the stream contains.
 *
 * @template Type The type of the stream's data.
 * @template ContentType The content type of the stream.
 * @template BodyType The underlying wire type of the stream.
 */
@doc("")
model HttpStream<Type, ContentType extends valueof string, BodyType extends bytes | string = string>
  is Stream<Type> {
  @header contentType: typeof ContentType;
  @body body: BodyType;
}

/**
 * Describes a stream of JSON data with one JSON object per line and sets
 * the content type to \`application/jsonl\`.
 *
 * The JSON data is described by \`Type\`.
 *
 * @template Type The set of models describing the JSON data in the stream.
 *
 * @example
 *
 * \`\`\`typespec
 * model Message {
 *   id: string;
 *   text: string;
 * }
 *
 * @TypeSpec.Events.events
 * union Events {
 *   Message,
 * }
 *
 * op subscribe(): JsonlStream<Events>;
 * \`\`\`
 */
@doc("")
model JsonlStream<Type> is HttpStream<Type, "application/jsonl">;
`,"tspconfig.yaml":"# Opt this library into the experimental `type-info-provider` feature so that the\n# `$provideTypeInfo` provider exported from `src/type-info.ts` is registered by the compiler.\n# Per-package feature enablement is resolved from the owning package's config, so consumers do\n# not need to enable it.\n# NOTE: this file must stay listed in `package.json#files` or the opt-in is lost when published.\nkind: project\nfeatures:\n  - type-info-provider\n"}},"@typespec/rest":{version:"0.86.0",files:{"package.json":`{
  "name": "@typespec/rest",
  "version": "0.86.0",
  "author": "Microsoft Corporation",
  "description": "TypeSpec REST protocol binding",
  "homepage": "https://typespec.io",
  "readme": "https://github.com/microsoft/typespec/blob/main/README.md",
  "license": "MIT",
  "repository": {
    "type": "git",
    "url": "git+https://github.com/microsoft/typespec.git"
  },
  "bugs": {
    "url": "https://github.com/microsoft/typespec/issues"
  },
  "keywords": [
    "typespec"
  ],
  "type": "module",
  "main": "dist/src/index.js",
  "tspMain": "lib/rest.tsp",
  "exports": {
    ".": {
      "typespec": "./lib/rest.tsp",
      "types": "./dist/src/index.d.ts",
      "default": "./dist/src/index.js"
    },
    "./testing": {
      "types": "./dist/src/testing/index.d.ts",
      "default": "./dist/src/testing/index.js"
    }
  },
  "engines": {
    "node": ">=22.0.0"
  },
  "files": [
    "lib/*.tsp",
    "dist/**",
    "!dist/test/**"
  ],
  "peerDependencies": {
    "@typespec/compiler": "^1.16.0",
    "@typespec/http": "^1.16.0"
  },
  "devDependencies": {
    "@types/node": "^26.3.0",
    "@vitest/coverage-v8": "^4.1.11",
    "@vitest/ui": "^4.1.11",
    "rimraf": "^6.1.3",
    "typescript": "~6.0.2",
    "vitest": "^4.1.11",
    "@typespec/compiler": "^1.16.0",
    "@typespec/http": "^1.16.0",
    "@typespec/library-linter": "^0.86.0",
    "@typespec/tspd": "^0.77.1"
  },
  "scripts": {
    "clean": "rimraf ./dist ./temp",
    "build": "pnpm gen-extern-signature && tsc -p tsconfig.build.json && pnpm lint-typespec-library",
    "watch": "tsc -p tsconfig.build.json --watch",
    "gen-extern-signature": "tspd --enable-experimental gen-extern-signature .",
    "lint-typespec-library": "tsp compile . --warn-as-error --import @typespec/library-linter --no-emit",
    "test": "vitest run",
    "test:watch": "vitest --watch",
    "test:ui": "vitest --ui",
    "test:ci": "vitest run --coverage --reporter=junit --reporter=default",
    "lint": "oxlint . --deny-warnings",
    "lint:fix": "oxlint . --fix",
    "regen-docs": "tspd doc .  --enable-experimental --llmstxt --output-dir ../../website/src/content/docs/docs/libraries/rest/reference"
  }
}`,"lib/resource.tsp":`import "@typespec/http";
import "../dist/src/internal-decorators.js";

namespace TypeSpec.Rest.Resource;

using Http;

@doc("The default error response for resource operations.")
model ResourceError {
  @doc("The error code.")
  code: int32;

  @doc("The error message.")
  message: string;
}

/**
 * Dynamically gathers keys of the model type \`Resource\`.
 *
 * @template Resource The target resource model.
 */
@doc("Dynamically gathers keys of the model type Resource.")
@copyResourceKeyParameters
@friendlyName("{name}Key", Resource)
model KeysOf<Resource> {}

/**
 * Dynamically gathers parent keys of the model type \`Resource\`.
 *
 * @template Resource The target resource model.
 */
@doc("Dynamically gathers parent keys of the model type Resource.")
@copyResourceKeyParameters("parent")
@friendlyName("{name}ParentKey", Resource)
model ParentKeysOf<Resource> {}

/**
 * Represents operation parameters for the resource of type \`Resource\`.
 *
 * @template Resource The resource model.
 */
@doc("Represents operation parameters for resource Resource.")
model ResourceParameters<Resource extends {}> {
  ...KeysOf<Resource>;
}

/**
 * Represents collection operation parameters for the resource of type \`Resource\`.
 *
 * @template Resource The resource model.
 */
@doc("Represents collection operation parameters for resource Resource.")
model ResourceCollectionParameters<Resource extends {}> {
  ...ParentKeysOf<Resource>;
}

/**
 * Represents the resource GET operation.
 *
 * @template Resource The resource model.
 * @template Error The error response.
 */
@Private.validateHasKey(Resource)
@Private.validateIsError(Error)
interface ResourceRead<Resource extends {}, Error> {
  /**
   * Gets an instance of the resource.
   */
  @autoRoute
  @doc("Gets an instance of the resource.")
  @readsResource(Resource)
  get(...ResourceParameters<Resource>): Resource | Error;
}

/**
 * Resource create operation completed successfully.
 *
 * @template Resource The resource model that was created.
 */
@doc("Resource create operation completed successfully.")
model ResourceCreatedResponse<Resource> {
  ...CreatedResponse;

  /** The created resource. */
  @bodyRoot body: Resource;
}

/**
 * Resource create or replace operation template.
 *
 * @template Resource The resource model to create or replace.
 * @template Error The error response.
 */
interface ResourceCreateOrReplace<Resource extends {}, Error> {
  /**
   * Creates or replaces a instance of the resource.
   */
  @autoRoute
  @doc("Creates or replaces an instance of the resource.")
  @createsOrReplacesResource(Resource)
  createOrReplace(
    ...ResourceParameters<Resource>,

    /** The properties of the resource to create or replace. */
    @doc("")
    @bodyRoot
    resource: ResourceCreateModel<Resource>,
  ): Resource | ResourceCreatedResponse<Resource> | Error;
}

/**
 * Resource create or update operation model.
 *
 * @template Resource The resource model to create or update.
 */
@friendlyName("{name}Update", Resource)
model ResourceCreateOrUpdateModel<Resource extends {}>
  is OptionalProperties<UpdateableProperties<DefaultKeyVisibility<Resource, Lifecycle.Read>>>;

/**
 * Resource create or update operation template.
 *
 * @template Resource The resource model to create or update.
 * @template Error The error response.
 */
interface ResourceCreateOrUpdate<Resource extends {}, Error> {
  /**
   * Creates or update an instance of the resource.
   */
  #suppress "@typespec/http/deprecated-implicit-optionality" "for legacy behavior"
  @autoRoute
  @doc("Creates or update an instance of the resource.")
  @createsOrUpdatesResource(Resource)
  @patch(#{ implicitOptionality: true }) // for legacy behavior
  createOrUpdate(
    ...ResourceParameters<Resource>,

    /** The properties of the resource to create or update. */
    @doc("")
    @bodyRoot
    resource: ResourceCreateOrUpdateModel<Resource>,
  ): Resource | ResourceCreatedResponse<Resource> | Error;
}

/**
 * Resource create operation model.
 *
 * @template Resource The resource model to create.
 */
@friendlyName("{name}Create", Resource)
@withVisibility(Lifecycle.Create)
model ResourceCreateModel<Resource extends {}> is DefaultKeyVisibility<Resource, Lifecycle.Read>;

/**
 * Resource create operation template.
 *
 * @template Resource The resource model to create.
 * @template Error The error response.
 */
interface ResourceCreate<Resource extends {}, Error> {
  /**
   * Creates a new instance of the resource.
   */
  @autoRoute
  @doc("Creates a new instance of the resource.")
  @createsResource(Resource)
  create(
    ...ResourceCollectionParameters<Resource>,

    /** The properties of the resource to create. */
    @doc("")
    @bodyRoot
    resource: ResourceCreateModel<Resource>,
  ): Resource | ResourceCreatedResponse<Resource> | Error;
}

/**
 * Resource update operation template.
 *
 * @template Resource The resource model to update.
 * @template Error The error response.
 */
@Private.validateHasKey(Resource)
@Private.validateIsError(Error)
interface ResourceUpdate<Resource extends {}, Error> {
  /**
   * Updates an existing instance of the resource.
   */
  #suppress "@typespec/http/deprecated-implicit-optionality" "for legacy behavior"
  @autoRoute
  @doc("Updates an existing instance of the resource.")
  @updatesResource(Resource)
  @patch(#{ implicitOptionality: true }) // for legacy behavior
  update(
    ...ResourceParameters<Resource>,

    /** The properties of the resource to update. */
    @doc("")
    @bodyRoot
    properties: ResourceCreateOrUpdateModel<Resource>,
  ): Resource | Error;
}

@doc("Resource deleted successfully.")
model ResourceDeletedResponse {
  @doc("The status code.")
  @statusCode
  _: 200;
}

/**
 * Resource delete operation template.
 *
 * @template Resource The resource model to delete.
 * @template Error The error response.
 */
@Private.validateHasKey(Resource)
@Private.validateIsError(Error)
interface ResourceDelete<Resource extends {}, Error> {
  /**
   * Deletes an existing instance of the resource.
   */
  @autoRoute
  @doc("Deletes an existing instance of the resource.")
  @deletesResource(Resource)
  delete(...ResourceParameters<Resource>): ResourceDeletedResponse | Error;
}

/**
 * Structure for a paging response using \`value\` and \`nextLink\` to represent pagination.
 *
 * @template Resource The resource type of the collection.
 */
@doc("Paged response of {name} items", Resource)
@friendlyName("{name}CollectionWithNextLink", Resource)
model CollectionWithNextLink<Resource extends {}> {
  @doc("The items on this page")
  @pageItems
  value: Resource[];

  @doc("The link to the next page of items")
  @nextLink
  nextLink?: ResourceLocation<Resource>;
}

/**
 * Resource list operation template.
 *
 * @template Resource The resource model to list.
 * @template Error The error response.
 */
interface ResourceList<Resource extends {}, Error> {
  /**
   * Lists all instances of the resource.
   */
  @autoRoute
  @doc("Lists all instances of the resource.")
  @listsResource(Resource)
  list(...ResourceCollectionParameters<Resource>): CollectionWithNextLink<Resource> | Error;
}

/**
 * Resource operation templates for resource instances.
 *
 * @template Resource The resource model.
 * @template Error The error response.
 */
@Private.validateHasKey(Resource)
@Private.validateIsError(Error)
interface ResourceInstanceOperations<Resource extends {}, Error>
  extends ResourceRead<Resource, Error>,
    ResourceUpdate<Resource, Error>,
    ResourceDelete<Resource, Error> {}

/**
 * Resource operation templates for resource collections.
 *
 * @template Resource The resource model.
 * @template Error The error response.
 */
@Private.validateHasKey(Resource)
@Private.validateIsError(Error)
interface ResourceCollectionOperations<Resource extends {}, Error>
  extends ResourceCreate<Resource, Error>,
    ResourceList<Resource, Error> {}

/**
 * Resource operation templates for resources.
 *
 * @template Resource The resource model.
 * @template Error The error response.
 */
@Private.validateHasKey(Resource)
@Private.validateIsError(Error)
interface ResourceOperations<Resource extends {}, Error>
  extends ResourceInstanceOperations<Resource, Error>,
    ResourceCollectionOperations<Resource, Error> {}

/**
 * Singleton resource read operation template.
 *
 * @template Singleton The singleton resource model.
 * @template Resource The resource model.
 * @template Error The error response.
 */
@Private.validateHasKey(Resource)
@Private.validateIsError(Error)
interface SingletonResourceRead<Singleton extends {}, Resource extends {}, Error> {
  /**
   * Gets the singleton resource.
   */
  @autoRoute
  @doc("Gets the singleton resource.")
  @segmentOf(Singleton)
  @readsResource(Singleton)
  get(...ResourceParameters<Resource>): Singleton | Error;
}

/**
 * Singleton resource update operation template.
 *
 * @template Singleton The singleton resource model.
 * @template Resource The resource model.
 * @template Error The error response.
 */
@Private.validateHasKey(Resource)
@Private.validateIsError(Error)
interface SingletonResourceUpdate<Singleton extends {}, Resource extends {}, Error> {
  /**
   * Updates the singleton resource.
   */
  #suppress "@typespec/http/deprecated-implicit-optionality" "for legacy behavior"
  @autoRoute
  @doc("Updates the singleton resource.")
  @segmentOf(Singleton)
  @updatesResource(Singleton)
  @patch(#{ implicitOptionality: true }) // for legacy behavior
  update(
    ...ResourceParameters<Resource>,

    /** The properties of the singleton resource to update. */
    @doc("")
    @body
    properties: ResourceCreateOrUpdateModel<Singleton>,
  ): Singleton | Error;
}

/**
 * Singleton resource operation templates for singleton resource instances.
 *
 * @template Singleton The singleton resource model.
 * @template Resource The resource model.
 * @template Error The error response.
 */
interface SingletonResourceOperations<Singleton extends {}, Resource extends {}, Error>
  extends SingletonResourceRead<Singleton, Resource, Error>,
    SingletonResourceUpdate<Singleton, Resource, Error> {}

/**
 * Extension resource read operation template.
 *
 * @template Extension The extension resource model.
 * @template Resource The resource model.
 * @template Error The error response.
 */
@Private.validateHasKey(Resource)
@Private.validateIsError(Error)
interface ExtensionResourceRead<Extension extends {}, Resource extends {}, Error> {
  /**
   * Gets an instance of the extension resource.
   */
  @autoRoute
  @doc("Gets an instance of the extension resource.")
  @readsResource(Extension)
  get(...ResourceParameters<Resource>, ...ResourceParameters<Extension>): Extension | Error;
}

/**
 * Extension resource create or update operation template.
 *
 * @template Extension The extension resource model.
 * @template Resource The resource model.
 * @template Error The error response.
 */
interface ExtensionResourceCreateOrUpdate<Extension extends {}, Resource extends {}, Error> {
  /**
   * Creates or update an instance of the extension resource.
   */
  #suppress "@typespec/http/deprecated-implicit-optionality" "for legacy behavior"
  @autoRoute
  @doc("Creates or update an instance of the extension resource.")
  @createsOrUpdatesResource(Extension)
  @patch(#{ implicitOptionality: true }) // for legacy behavior
  createOrUpdate(
    ...ResourceParameters<Resource>,
    ...ResourceParameters<Extension>,

    /** The properties of the extension resource to create or update. */
    @doc("")
    @bodyRoot
    resource: ResourceCreateOrUpdateModel<Extension>,
  ): Extension | ResourceCreatedResponse<Extension> | Error;
}

/**
 * Extension resource create operation template.
 *
 * @template Extension The extension resource model.
 * @template Resource The resource model.
 * @template Error The error response.
 */
interface ExtensionResourceCreate<Extension extends {}, Resource extends {}, Error> {
  /**
   * Creates a new instance of the extension resource.
   */
  @autoRoute
  @doc("Creates a new instance of the extension resource.")
  @createsResource(Extension)
  create(
    ...ResourceParameters<Resource>,

    /** The properties of the extension resource to create. */
    @doc("")
    @bodyRoot
    resource: ResourceCreateModel<Extension>,
  ): Extension | ResourceCreatedResponse<Extension> | Error;
}

/**
 * Extension resource update operation template.
 *
 * @template Extension The extension resource model.
 * @template Resource The resource model.
 * @template Error The error response.
 */
interface ExtensionResourceUpdate<Extension extends {}, Resource extends {}, Error> {
  /**
   * Updates an existing instance of the extension resource.
   */
  #suppress "@typespec/http/deprecated-implicit-optionality" "for legacy behavior"
  @autoRoute
  @doc("Updates an existing instance of the extension resource.")
  @updatesResource(Extension)
  @patch(#{ implicitOptionality: true }) // for legacy behavior
  update(
    ...ResourceParameters<Resource>,
    ...ResourceParameters<Extension>,

    /** The properties of the extension resource to update. */
    @doc("")
    @body
    properties: ResourceCreateOrUpdateModel<Extension>,
  ): Extension | Error;
}

/**
 * Extension resource delete operation template.
 *
 * @template Extension The extension resource model.
 * @template Resource The resource model.
 * @template Error The error response.
 */
interface ExtensionResourceDelete<Extension extends {}, Resource extends {}, Error> {
  /**
   * Deletes an existing instance of the extension resource.
   */
  @autoRoute
  @doc("Deletes an existing instance of the extension resource.")
  @deletesResource(Extension)
  delete(...ResourceParameters<Resource>, ...ResourceParameters<Extension>):
    | ResourceDeletedResponse
    | Error;
}

/**
 * Extension resource list operation template.
 *
 * @template Extension The extension resource model.
 * @template Resource The resource model.
 * @template Error The error response.
 */
interface ExtensionResourceList<Extension extends {}, Resource extends {}, Error> {
  /**
   * Lists all instances of the extension resource.
   */
  @autoRoute
  @doc("Lists all instances of the extension resource.")
  @listsResource(Extension)
  list(...ResourceParameters<Resource>, ...ResourceCollectionParameters<Extension>):
    | CollectionWithNextLink<Extension>
    | Error;
}

/**
 * Extension resource operation templates for extension resource instances.
 *
 * @template Extension The extension resource model.
 * @template Resource The resource model.
 * @template Error The error response.
 */
interface ExtensionResourceInstanceOperations<Extension extends {}, Resource extends {}, Error>
  extends ExtensionResourceRead<Extension, Resource, Error>,
    ExtensionResourceUpdate<Extension, Resource, Error>,
    ExtensionResourceDelete<Extension, Resource, Error> {}

/**
 * Extension resource operation templates for extension resource collections.
 *
 * @template Extension The extension resource model.
 * @template Resource The resource model.
 * @template Error The error response.
 */
interface ExtensionResourceCollectionOperations<Extension extends {}, Resource extends {}, Error>
  extends ExtensionResourceCreate<Extension, Resource, Error>,
    ExtensionResourceList<Extension, Resource, Error> {}

/**
 * Extension resource operation templates for extension resource instances and collections.
 *
 * @template Extension The extension resource model.
 * @template Resource The resource model.
 * @template Error The error response.
 */
interface ExtensionResourceOperations<Extension extends {}, Resource extends {}, Error>
  extends ExtensionResourceInstanceOperations<Extension, Resource, Error>,
    ExtensionResourceCollectionOperations<Extension, Resource, Error> {}
`,"lib/rest-decorators.tsp":`namespace TypeSpec.Rest;

using TypeSpec.Reflection;

/**
 * This interface or operation should resolve its route automatically. To be used with resource types where the route segments area defined on the models.
 *
 * @example
 *
 * \`\`\`typespec
 * @autoRoute
 * interface Pets {
 *   get(@segment("pets") @path id: string): void; //-> route: /pets/{id}
 * }
 * \`\`\`
 */
extern dec autoRoute(target: Interface | Operation);

/**
 * Defines the preceding path segment for a \`@path\` parameter in auto-generated routes.
 *
 * @param name Segment that will be inserted into the operation route before the path parameter's name field.
 *
 * @example
 *
 * \`\`\`typespec
 * @autoRoute
 * interface Pets {
 *   get(@segment("pets") @path id: string): void; //-> route: /pets/{id}
 * }
 * \`\`\`
 */
extern dec segment(target: Model | ModelProperty | Operation, name: valueof string);

/**
 * Returns the URL segment of a given model if it has \`@segment\` and \`@key\` decorator.
 * @param type Target model
 */
extern dec segmentOf(target: Operation, type: Model);

/**
 * Defines the separator string that is inserted before the action name in auto-generated routes for actions.
 *
 * When applied to a namespace, the separator applies to all action operations in that namespace and its sub-namespaces.
 * When applied to an interface, the separator applies to all action operations in that interface and overrides any namespace-level separator.
 * When applied to an operation, the separator applies only to that operation and overrides any interface or namespace-level separator.
 *
 * @param seperator Seperator seperating the action segment from the rest of the url
 */
extern dec actionSeparator(
  target: Operation | Interface | Namespace,
  seperator: valueof "/" | ":" | "/:"
);

/**
 * Mark this model as a resource type with a name.
 *
 * @param collectionName type's collection name
 */
extern dec resource(target: Model, collectionName: valueof string);

/**
 * Mark model as a child of the given parent resource.
 * @param parent Parent model.
 */
extern dec parentResource(target: Model, parent: Model);

/**
 * Specify that this is a Read operation for a given resource.
 *
 * @param resourceType Resource marked with \`@resource\`
 */
extern dec readsResource(target: Operation, resourceType: Model);

/**
 * Specify that this is a Create operation for a given resource.
 *
 * @param resourceType Resource marked with \`@resource\`
 */
extern dec createsResource(target: Operation, resourceType: Model);

/**
 * Specify that this is a CreateOrReplace operation for a given resource.
 *
 * @param resourceType Resource marked with \`@resource\`
 */
extern dec createsOrReplacesResource(target: Operation, resourceType: Model);

/**
 * Specify that this is a CreatesOrUpdate operation for a given resource.
 *
 * @param resourceType Resource marked with \`@resource\`
 */
extern dec createsOrUpdatesResource(target: Operation, resourceType: Model);

/**
 * Specify that this is a Update operation for a given resource.
 *
 * @param resourceType Resource marked with \`@resource\`
 */
extern dec updatesResource(target: Operation, resourceType: Model);

/**
 * Specify that this is a Delete operation for a given resource.
 *
 * @param resourceType Resource marked with \`@resource\`
 */
extern dec deletesResource(target: Operation, resourceType: Model);

/**
 * Specify that this is a List operation for a given resource.
 *
 * @param resourceType Resource marked with \`@resource\`
 */
extern dec listsResource(target: Operation, resourceType: Model);

/**
 * Specify this operation is an action. (Scoped to a resource item /pets/{petId}/my-action)
 * @param name Name of the action. If not specified, the name of the operation will be used.
 */
extern dec action(target: Operation, name?: valueof string);

/**
 * Specify this operation is a collection action. (Scopped to a resource, /pets/my-action)
 * @param resourceType Resource marked with \`@resource\`
 * @param name Name of the action. If not specified, the name of the operation will be used.
 */
extern dec collectionAction(target: Operation, resourceType: Model, name?: valueof string);

/**
 * Copy the resource key parameters on the model
 * @param filter Filter to exclude certain properties.
 */
extern dec copyResourceKeyParameters(target: Model, filter?: valueof string);

namespace Private {
  extern dec resourceLocation(target: string, resourceType: Model);
  extern dec validateHasKey(target: unknown, value: unknown);
  extern dec validateIsError(target: unknown, value: unknown);
  extern dec actionSegment(target: Operation, value: valueof string);
  extern dec resourceTypeForKeyParam(entity: ModelProperty, resourceType: Model);
}
`,"lib/rest.tsp":`import "@typespec/http";
import "./rest-decorators.tsp";
import "./resource.tsp";
import "../dist/src/tsp-index.js";

namespace TypeSpec.Rest;

/**
 * A URL that points to a resource.
 * @template Resource The type of resource that the URL points to.
 */
@doc("The location of an instance of {name}", Resource)
@Private.resourceLocation(Resource)
scalar ResourceLocation<Resource extends {}> extends url;
`}},"@typespec/openapi":{version:"1.16.0",files:{"package.json":`{
  "name": "@typespec/openapi",
  "version": "1.16.0",
  "author": "Microsoft Corporation",
  "description": "TypeSpec library providing OpenAPI concepts",
  "homepage": "https://typespec.io",
  "readme": "https://github.com/microsoft/typespec/blob/main/README.md",
  "license": "MIT",
  "repository": {
    "type": "git",
    "url": "git+https://github.com/microsoft/typespec.git"
  },
  "bugs": {
    "url": "https://github.com/microsoft/typespec/issues"
  },
  "keywords": [
    "typespec"
  ],
  "type": "module",
  "main": "dist/src/index.js",
  "tspMain": "lib/main.tsp",
  "exports": {
    ".": {
      "typespec": "./lib/main.tsp",
      "types": "./dist/src/index.d.ts",
      "default": "./dist/src/index.js"
    },
    "./testing": {
      "types": "./dist/src/testing/index.d.ts",
      "default": "./dist/src/testing/index.js"
    }
  },
  "engines": {
    "node": ">=22.0.0"
  },
  "files": [
    "lib/*.tsp",
    "dist/**",
    "!dist/test/**"
  ],
  "peerDependencies": {
    "@typespec/compiler": "^1.16.0",
    "@typespec/http": "^1.16.0"
  },
  "devDependencies": {
    "@types/node": "^26.3.0",
    "@vitest/coverage-v8": "^4.1.11",
    "@vitest/ui": "^4.1.11",
    "rimraf": "^6.1.3",
    "typescript": "~6.0.2",
    "vitest": "^4.1.11",
    "@typespec/compiler": "^1.16.0",
    "@typespec/http": "^1.16.0",
    "@typespec/library-linter": "^0.86.0",
    "@typespec/rest": "^0.86.0",
    "@typespec/tspd": "^0.77.1"
  },
  "scripts": {
    "clean": "rimraf ./dist ./temp",
    "build": "pnpm gen-extern-signature && tsc -p tsconfig.build.json && pnpm lint-typespec-library && pnpm api-extractor",
    "watch": "tsc -p tsconfig.build.json --watch",
    "gen-extern-signature": "tspd --enable-experimental gen-extern-signature .",
    "lint-typespec-library": "tsp compile . --warn-as-error --import @typespec/library-linter --no-emit",
    "test": "vitest run",
    "test:watch": "vitest -w",
    "test:ui": "vitest --ui",
    "test:ci": "vitest run --coverage --reporter=junit --reporter=default",
    "lint": "oxlint . --deny-warnings",
    "lint:fix": "oxlint . --fix",
    "regen-docs": "tspd doc .  --enable-experimental --llmstxt --output-dir ../../website/src/content/docs/docs/libraries/openapi/reference",
    "api-extractor": "api-extractor run --local --verbose"
  }
}`,"lib/decorators.tsp":`using TypeSpec.Reflection;

namespace TypeSpec.OpenAPI;

/**
 * Specify the OpenAPI \`operationId\` property for this operation.
 *
 * @param operationId Operation id value.
 *
 * @example
 *
 * \`\`\`typespec
 * @operationId("download")
 * op read(): string;
 * \`\`\`
 */
extern dec operationId(target: Operation, operationId: valueof string);

/**
 * Attach some custom data to the OpenAPI element generated from this type.
 *
 * @param key Extension key.
 * @param value Extension value.
 *
 * @example
 *
 * \`\`\`typespec
 * @extension("x-custom", "My value")
 * @extension("x-pageable", #{nextLink: "x-next-link"})
 * op read(): string;
 * \`\`\`
 */
extern dec extension(target: unknown, key: valueof string, value: valueof unknown);

/**
 * Specify that this model is to be treated as the OpenAPI \`default\` response.
 * This differs from the compiler built-in \`@error\` decorator as this does not necessarily represent an error.
 *
 * @example
 *
 * \`\`\`typespec
 * @defaultResponse
 * model PetStoreResponse is object;
 *
 * op listPets(): Pet[] | PetStoreResponse;
 * \`\`\`
 */
extern dec defaultResponse(target: Model);

/**
 * Specify the OpenAPI \`externalDocs\` property for this type.
 *
 * @param url Url to the docs
 * @param description Description of the docs
 *
 * @example
 * \`\`\`typespec
 * @externalDocs("https://example.com/detailed.md", "Detailed information on how to use this operation")
 * op listPets(): Pet[];
 * \`\`\`
 */
extern dec externalDocs(target: unknown, url: valueof string, description?: valueof string);

/** Additional information for the OpenAPI document. */
model AdditionalInfo {
  /** The title of the API. Overrides the \`@service\` title. */
  title?: string;

  /** A short summary of the API. Overrides the \`@summary\` provided on the service namespace. */
  summary?: string;

  /** The version of the OpenAPI document (which is distinct from the OpenAPI Specification version or the API implementation version). */
  version?: string;

  /** A URL to the Terms of Service for the API. MUST be in the format of a URL. */
  termsOfService?: url;

  /** The contact information for the exposed API. */
  contact?: Contact;

  /** The license information for the exposed API. */
  license?: License;

  ...Record<unknown>;
}

/** Contact information for the exposed API. */
model Contact {
  /** The identifying name of the contact person/organization. */
  name?: string;

  /** The URL pointing to the contact information. MUST be in the format of a URL. */
  url?: url;

  /** The email address of the contact person/organization. MUST be in the format of an email address. */
  email?: string;

  ...Record<unknown>;
}

/** License information for the exposed API. */
model License {
  /** The license name used for the API. */
  name: string;

  /** A URL to the license used for the API. MUST be in the format of a URL. Mutually exclusive with \`identifier\`. */
  url?: url;

  /** An SPDX license expression for the API. Mutually exclusive with \`url\`. Only supported in OpenAPI 3.1+. For OpenAPI 3.0, this will be emitted as \`x-oai-license-identifier\`. */
  identifier?: string;

  ...Record<unknown>;
}

/**
 * Specify OpenAPI additional information.
 * The service \`title\` is already specified using \`@service\`.
 * @param additionalInfo Additional information
 */
extern dec info(target: Namespace, additionalInfo: valueof AdditionalInfo);

/** Metadata to a single tag that is used by operations. */
model TagMetadata {
  /** A description of the tag. */
  description?: string;

  /** External documentation information for the tag. */
  externalDocs?: ExternalDocs;

  /** The name of a tag that this tag is nested under. Only supported in OpenAPI 3.2. For 3.0 and 3.1, this will be converted to \`x-parent\`. */
  parent?: string;

  /** A short summary of the tag, used for display purposes. Only supported natively in OpenAPI 3.2. For 3.0 and 3.1, this will be emitted as \`x-oai-summary\`. */
  summary?: string;

  /** A machine-readable string to categorize what sort of tag it is. Any string value can be used. Only supported natively in OpenAPI 3.2. For 3.0 and 3.1, this will be emitted as \`x-oai-kind\`. */
  kind?: string;

  /** Attach some custom data, The extension key must start with \`x-\`. */
  ...Record<unknown>;
}

/** Metadata for a tag that includes the name of the tag. Used with the array form of \`@tagMetadata\`. */
model TagMetadataWithName {
  /** The name of the tag. */
  name: string;

  ...TagMetadata;
}

/** External Docs information. */
model ExternalDocs {
  /** Documentation url */
  url: string;

  /** Optional description */
  description?: string;

  /** Attach some custom data, The extension key must start with \`x-\`. */
  ...Record<unknown>;
}

/**
 * Specify OpenAPI tag metadata. Can be used in two forms:
 * - Inline form: specify a single tag by name with optional metadata.
 * - Array form: specify an ordered list of tags with their metadata in a single decorator call.
 *
 * @param name Tag name (inline form) or array of tags with metadata (array form).
 * @param tagMetadata Additional information for the tag. Only used in inline form.
 *
 * @example Inline form
 * \`\`\`typespec
 * @service()
 * @tagMetadata("Tag Name", #{description: "Tag description", externalDocs: #{url: "https://example.com", description: "More info.", \`x-custom\`: "string"}, \`x-custom\`: "string"})
 * @tagMetadata("Child Tag", #{description: "Child tag description", parent: "Tag Name"})
 * namespace PetStore {}
 * \`\`\`
 *
 * @example Array form (preserves explicit tag order)
 * \`\`\`typespec
 * @service()
 * @tagMetadata(#[
 *   #{ name: "First Tag", description: "First tag description" },
 *   #{ name: "Second Tag", description: "Second tag description" },
 * ])
 * namespace PetStore {}
 * \`\`\`
 */
extern dec tagMetadata(
  target: Namespace,
  name: valueof string | TagMetadataWithName[],
  tagMetadata?: valueof TagMetadata
);
`,"lib/main.tsp":`import "../dist/src/tsp-index.js";
import "./decorators.tsp";
`}},"@typespec/streams":{version:"0.86.0",files:{"package.json":`{
  "name": "@typespec/streams",
  "version": "0.86.0",
  "author": "Microsoft Corporation",
  "description": "TypeSpec library providing stream bindings",
  "homepage": "https://typespec.io",
  "readme": "https://github.com/microsoft/typespec/blob/main/README.md",
  "license": "MIT",
  "repository": {
    "type": "git",
    "url": "git+https://github.com/microsoft/typespec.git"
  },
  "bugs": {
    "url": "https://github.com/microsoft/typespec/issues"
  },
  "keywords": [
    "typespec"
  ],
  "type": "module",
  "main": "dist/src/index.js",
  "tspMain": "lib/main.tsp",
  "exports": {
    ".": {
      "typespec": "./lib/main.tsp",
      "types": "./dist/src/index.d.ts",
      "default": "./dist/src/index.js"
    },
    "./testing": {
      "types": "./dist/src/testing/index.d.ts",
      "default": "./dist/src/testing/index.js"
    }
  },
  "engines": {
    "node": ">=22.0.0"
  },
  "files": [
    "lib/*.tsp",
    "dist/**",
    "!dist/test/**"
  ],
  "peerDependencies": {
    "@typespec/compiler": "^1.16.0"
  },
  "devDependencies": {
    "@types/node": "^26.3.0",
    "@vitest/coverage-v8": "^4.1.11",
    "@vitest/ui": "^4.1.11",
    "rimraf": "^6.1.3",
    "typescript": "~6.0.2",
    "vitest": "^4.1.11",
    "@typespec/compiler": "^1.16.0",
    "@typespec/tspd": "^0.77.1",
    "@typespec/library-linter": "^0.86.0"
  },
  "scripts": {
    "clean": "rimraf ./dist ./temp",
    "build": "pnpm gen-extern-signature && tsc -p tsconfig.build.json && pnpm lint-typespec-library",
    "watch": "tsc -p tsconfig.build.json --watch",
    "gen-extern-signature": "tspd --enable-experimental gen-extern-signature .",
    "lint-typespec-library": "tsp compile . --warn-as-error --import @typespec/library-linter --no-emit",
    "test": "vitest run",
    "test:watch": "vitest -w",
    "test:ui": "vitest --ui",
    "test-official": "vitest run --coverage --reporter=junit --reporter=default --no-file-parallelism",
    "lint": "oxlint . --deny-warnings",
    "lint:fix": "oxlint . --fix",
    "regen-docs": "tspd doc .  --enable-experimental --llmstxt --output-dir ../../website/src/content/docs/docs/libraries/streams/reference"
  }
}`,"lib/decorators.tsp":`using TypeSpec.Reflection;

namespace TypeSpec.Streams;

/**
 * Specify that a model represents a stream protocol type whose data is described
 * by \`Type\`.
 *
 * @param type The type that models the underlying data of the stream.
 *
 * @example
 *
 * \`\`\`typespec
 * model Message {
 *   id: string;
 *   text: string;
 * }
 *
 * @streamOf(Message)
 * model Response {
 *   @body body: string;
 * }
 * \`\`\`
 */
extern dec streamOf(target: Model, type: unknown);
`,"lib/main.tsp":`import "../dist/src/tsp-index.js";
import "./decorators.tsp";
import "./types.tsp";
`,"lib/types.tsp":`namespace TypeSpec.Streams;

/**
 * Defines a model that represents a stream protocol type whose data is described
 * by \`Type\`.
 *
 * This can be useful when the underlying data type is not relevant, or to serve as
 * a base type for custom streams.
 *
 * @template Type The type of the stream's data.
 */
@doc("")
@streamOf(Type)
model Stream<Type> {}
`}},"@typespec/graphql":{version:"0.3.0",files:{"package.json":`{
  "name": "@typespec/graphql",
  "version": "0.3.0",
  "author": "Microsoft Corporation",
  "description": "TypeSpec library for emitting GraphQL",
  "homepage": "https://typespec.io",
  "readme": "https://github.com/microsoft/typespec/blob/main/README.md",
  "license": "MIT",
  "repository": {
    "type": "git",
    "url": "git+https://github.com/microsoft/typespec.git"
  },
  "bugs": {
    "url": "https://github.com/microsoft/typespec/issues"
  },
  "keywords": [
    "typespec"
  ],
  "type": "module",
  "tspMain": "lib/main.tsp",
  "main": "dist/src/index.js",
  "exports": {
    ".": {
      "typespec": "./lib/main.tsp",
      "types": "./dist/src/index.d.ts",
      "default": "./dist/src/index.js"
    },
    "./mutation-engine": {
      "types": "./dist/src/mutation-engine/index.d.ts",
      "default": "./dist/src/mutation-engine/index.js"
    }
  },
  "engines": {
    "node": ">=22.0.0"
  },
  "dependencies": {
    "@alloy-js/core": "^0.24.1",
    "@alloy-js/typescript": "^0.24.0",
    "@pinterest/alloy-graphql": "^1.1.1",
    "change-case": "^5.4.4",
    "graphql": "^17.0.2"
  },
  "files": [
    "lib/*.tsp",
    "tspconfig.yaml",
    "dist/**",
    "!dist/test/**"
  ],
  "peerDependencies": {
    "@typespec/compiler": "~1.16.0",
    "@typespec/http": "~1.16.0",
    "@typespec/emitter-framework": "~0.21.0",
    "@typespec/mutator-framework": "~0.17.1"
  },
  "devDependencies": {
    "@alloy-js/cli": "^0.24.0",
    "@alloy-js/rollup-plugin": "^0.1.2",
    "@types/node": "^26.3.0",
    "rimraf": "^6.1.3",
    "typescript": "~6.0.2",
    "vitest": "^4.1.11",
    "@typespec/compiler": "~1.16.0",
    "@typespec/http": "~1.16.0",
    "@typespec/library-linter": "^0.86.0",
    "@typespec/mutator-framework": "~0.17.1",
    "@typespec/tspd": "~0.77.1",
    "@typespec/emitter-framework": "~0.21.0"
  },
  "scripts": {
    "clean": "rimraf ./dist ./temp",
    "build": "pnpm gen-extern-signature && alloy build && pnpm lint-typespec-library",
    "lint-typespec-library": "tsp compile . --warn-as-error --import @typespec/library-linter --no-emit",
    "watch": "alloy build --watch",
    "gen-extern-signature": "tspd --enable-experimental gen-extern-signature .",
    "test": "vitest run",
    "test:watch": "vitest -w",
    "lint": "oxlint . --deny-warnings",
    "lint:fix": "oxlint . --fix",
    "regen-docs": "tspd doc . --enable-experimental --output-dir ../../website/src/content/docs/docs/emitters/graphql/reference"
  }
}`,"lib/input-type.tsp":`using TypeSpec.Reflection;

namespace TypeSpec.GraphQL;

/**
 * Mark a model as a GraphQL input type in the emitted schema.
 *
 * This decorator is applied automatically by the mutation engine when it produces
 * a model that is used in input position. The emitter uses this to emit the model
 * as an \`input\` type rather than an object \`type\`.
 */
internal auto dec inputType(target: Model);
`,"lib/interface.tsp":`using TypeSpec.Reflection;

namespace TypeSpec.GraphQL;

/**
 * Mark this model as a GraphQL Interface. Interfaces can be implemented by other models
 * using the \`@compose\` decorator.
 *
 * @param options.interfaceOnly When true, the model will only be emitted as an interface
 * (no "Interface" suffix is added to the name). Use this for abstract interfaces that
 * will never be used directly as output/input types (e.g., Node, Connection). Defaults to false.
 *
 * @example
 *
 * \`\`\`typespec
 * @graphqlInterface(#{ interfaceOnly: true })
 * model Node {
 *   id: string;
 * }
 *
 * @compose(Node)
 * model User {
 *   ...Node;
 *   name: string;
 * }
 * // Emits: interface Node { id: String! }
 * //        type User implements Node { id: String!; name: String! }
 * \`\`\`
 */
extern dec graphqlInterface(
  target: Model,
  options?: valueof {
    interfaceOnly?: boolean,
  }
);

/**
 * Specify the GraphQL interfaces that should be implemented by a model.
 * The interfaces must be decorated with the \`@graphqlInterface\` decorator,
 * and all of the interfaces' properties must be present and compatible.
 *
 * @example
 *
 * \`\`\`typespec
 * @graphqlInterface(#{ interfaceOnly: true })
 * model Node {
 *   id: string;
 * }
 *
 * @compose(Node)
 * model User {
 *   ...Node;
 *   name: string;
 * }
 * \`\`\`
 *
 * @param interfaces The models to compose into the target, each emitted as a GraphQL interface.
 */
extern dec compose(target: Model, ...interfaces: Model[]);
`,"lib/main.tsp":`import "../dist/src/tsp-index.js";
import "./interface.tsp";
import "./input-type.tsp";
import "./nullable.tsp";
import "./one-of.tsp";
import "./operation-fields.tsp";
import "./operation-kind.tsp";
import "./scalars.tsp";
import "./schema.tsp";
import "./specified-by.tsp";
`,"lib/nullable.tsp":`using TypeSpec.Reflection;

namespace TypeSpec.GraphQL;

/**
 * Mark a field, operation, or type as nullable in the emitted GraphQL schema.
 *
 * Applied automatically by the mutation engine when it strips \`| null\` from
 * union types, and can also be applied directly in TypeSpec source.
 */
internal auto dec nullable(target: ModelProperty | Operation | Union | Model);

/**
 * Mark a field or operation as having nullable array elements in the emitted GraphQL schema.
 *
 * Applied automatically by the mutation engine when it detects \`Array<T | null>\`
 * patterns. Causes the emitter to emit \`[T]\` instead of \`[T!]\`.
 */
internal auto dec nullableElements(target: ModelProperty | Operation);
`,"lib/one-of.tsp":`using TypeSpec.Reflection;

namespace TypeSpec.GraphQL;

/**
 * Mark a model as a \`@oneOf\` input object in the emitted GraphQL schema.
 *
 * This decorator is applied automatically by the mutation engine when it converts
 * a union type in input context to a synthetic input object (since GraphQL unions
 * are output-only). The emitter uses this to emit the \`@oneOf\` directive.
 *
 * @see https://spec.graphql.org/September2025/#sec-OneOf-Input-Objects
 */
internal auto dec oneOf(target: Model);
`,"lib/operation-fields.tsp":`using TypeSpec.Reflection;

namespace TypeSpec.GraphQL;

alias OperationOrInterface = Operation | Interface;

/**
 * Assign one or more operations or interfaces to act as fields with arguments on a model.
 * The operations become fields on the GraphQL type with their parameters as arguments.
 *
 * @example
 *
 * \`\`\`typespec
 * op followers(query: string): Person[];
 *
 * @operationFields(followers)
 * model Person {
 *   name: string;
 * }
 * // Emits: type Person { name: String!; followers(query: String!): [Person!]! }
 * \`\`\`
 *
 * @param operations The operations, or interfaces of operations, to add to the target as fields.
 */
extern dec operationFields(target: Model, ...operations: OperationOrInterface[]);
`,"lib/operation-kind.tsp":`using TypeSpec.Reflection;

namespace TypeSpec.GraphQL;

/**
 * Specify the GraphQL Operation kind for the target operation to be \`MUTATION\`.
 *
 * @example
 *
 * \`\`\`typespec
 * @mutation op createUser(name: string): User;
 * \`\`\`
 */
extern dec mutation(target: Operation);

/**
 * Specify the GraphQL Operation kind for the target operation to be \`QUERY\`.
 *
 * @example
 *
 * \`\`\`typespec
 * @query op getUser(id: string): User;
 * \`\`\`
 */
extern dec query(target: Operation);

/**
 * Specify the GraphQL Operation kind for the target operation to be \`SUBSCRIPTION\`.
 *
 * @example
 *
 * \`\`\`typespec
 * @subscription op onUserCreated(): User;
 * \`\`\`
 */
extern dec subscription(target: Operation);
`,"lib/scalars.tsp":`namespace TypeSpec.GraphQL;

/**
 * Represents a GraphQL ID scalar — a unique identifier serialized as a string.
 *
 * @see https://spec.graphql.org/September2025/#sec-ID
 *
 * @example
 *
 * \`\`\`typespec
 * model User {
 *   id: GraphQL.ID;
 *   name: string;
 * }
 * \`\`\`
 */
scalar ID extends string;
`,"lib/schema.tsp":`using TypeSpec.Reflection;

namespace TypeSpec.GraphQL;

namespace Schema {
  /** Options for configuring a GraphQL schema. */
  model SchemaOptions {
    /**
     * The name of the GraphQL schema. Used in the output filename when emitting
     * multiple schemas (e.g., \`{name}.graphql\`). Defaults to \`"schema"\`.
     */
    name?: string;
  }
}

/**
 * Mark this namespace as describing a GraphQL schema and configure schema properties.
 * All types and operations within the namespace will be emitted to a single GraphQL schema file.
 *
 * @example
 *
 * \`\`\`typespec
 * @schema(#{ name: "MyAPI" })
 * namespace MyAPI {
 *   model User { id: string; name: string; }
 *   @query op getUser(id: string): User;
 * }
 * // Emits: MyAPI.graphql
 * \`\`\`
 *
 * @param options Options for the schema, such as its name.
 */
extern dec schema(target: Namespace, options?: valueof Schema.SchemaOptions);
`,"lib/specified-by.tsp":`using TypeSpec.Reflection;

namespace TypeSpec.GraphQL;

/**
 * Provide a specification URL for a custom GraphQL scalar type.
 * This maps to the \`@specifiedBy\` directive in the emitted GraphQL schema.
 *
 * @param url URL to the scalar type specification
 * @example
 *
 * \`\`\`typespec
 * @specifiedBy("https://scalars.graphql.org/andimarek/date-time")
 * scalar DateTime extends utcDateTime;
 * \`\`\`
 */
extern dec specifiedBy(target: Scalar, url: valueof url);
`,"tspconfig.yaml":"# Opt this library into the experimental `auto-decorators` feature so that\n# `@nullable` / `@nullableElements` (declared as `auto dec` in lib/nullable.tsp)\n# are permitted in this library's own source. Per-package feature enablement is\n# resolved from the owning package's config, so consumers do not need to enable it.\nkind: project\nfeatures:\n  - auto-decorators\n"}},"@typespec/json-schema":{version:"1.16.0",files:{"package.json":`{
  "name": "@typespec/json-schema",
  "version": "1.16.0",
  "author": "Microsoft Corporation",
  "description": "TypeSpec library for emitting TypeSpec to JSON Schema and converting JSON Schema to TypeSpec",
  "homepage": "https://github.com/microsoft/typespec",
  "readme": "https://github.com/microsoft/typespec/blob/main/README.md",
  "license": "MIT",
  "repository": {
    "type": "git",
    "url": "git+https://github.com/microsoft/typespec.git"
  },
  "bugs": {
    "url": "https://github.com/microsoft/typespec/issues"
  },
  "keywords": [
    "TypeSpec",
    "json schema"
  ],
  "type": "module",
  "main": "dist/src/index.js",
  "exports": {
    ".": {
      "typespec": "./lib/main.tsp",
      "types": "./dist/src/index.d.ts",
      "default": "./dist/src/index.js"
    },
    "./testing": {
      "types": "./dist/src/testing/index.d.ts",
      "default": "./dist/src/testing/index.js"
    }
  },
  "tspMain": "lib/main.tsp",
  "engines": {
    "node": ">=22.0.0"
  },
  "files": [
    "lib/*.tsp",
    "dist/**",
    "!dist/test/**"
  ],
  "peerDependencies": {
    "@typespec/compiler": "^1.16.0"
  },
  "devDependencies": {
    "@types/node": "^26.3.0",
    "@vitest/coverage-v8": "^4.1.11",
    "@vitest/ui": "^4.1.11",
    "ajv": "^8.20.0",
    "ajv-formats": "^3.0.1",
    "rimraf": "^6.1.3",
    "typescript": "~6.0.2",
    "vitest": "^4.1.11",
    "@typespec/compiler": "^1.16.0",
    "@typespec/internal-build-utils": "^0.86.0",
    "@typespec/library-linter": "^0.86.0",
    "@typespec/tspd": "^0.77.1"
  },
  "dependencies": {
    "yaml": "^2.9.0",
    "@typespec/asset-emitter": "^0.79.2"
  },
  "scripts": {
    "clean": "rimraf ./dist ./temp",
    "build": "pnpm gen-extern-signature && tsc -p tsconfig.build.json && pnpm lint-typespec-library && pnpm api-extractor",
    "watch": "tsc -p tsconfig.build.json --watch",
    "gen-extern-signature": "tspd --enable-experimental gen-extern-signature .",
    "lint-typespec-library": "tsp compile . --warn-as-error --import @typespec/library-linter --no-emit",
    "test": "vitest run",
    "test:ui": "vitest --ui",
    "test:ci": "vitest run --coverage --reporter=junit --reporter=default",
    "lint": "oxlint . --deny-warnings",
    "lint:fix": "oxlint . --fix",
    "regen-docs": "tspd doc .  --enable-experimental --llmstxt  --output-dir ../../website/src/content/docs/docs/emitters/json-schema/reference",
    "api-extractor": "api-extractor run --local --verbose"
  }
}`,"lib/main.tsp":`import "../dist/src/tsp-index.js";

namespace TypeSpec.JsonSchema;

/**
 * Add to namespaces to emit models within that namespace to JSON schema.
 * Add to another declaration to emit that declaration to JSON schema.
 *
 * Optionally, for namespaces, you can provide a baseUri, and for other declarations,
 * you can provide the id.
 *
 * @param baseUri Schema IDs are interpreted as relative to this URI.
 */
extern dec jsonSchema(target: unknown, baseUri?: valueof string);

/**
 * Set the base URI for any schemas emitted from types within this namespace.
 *
 * @param baseUri The base URI. Schema IDs inside this namespace are relative to this URI.
 */
extern dec baseUri(target: Reflection.Namespace, baseUri: valueof string);

/**
 * Specify the JSON Schema id. If this model or a parent namespace has a base URI,
 * the provided ID will be relative to that base URI.
 *
 * By default, the id will be constructed based on the declaration's name.
 *
 * @param id The id of the JSON schema for this declaration.
 */
extern dec id(target: unknown, id: valueof string);

/**
 * Specify that \`oneOf\` should be used instead of \`anyOf\` for that union.
 */
extern dec oneOf(target: Reflection.Union | Reflection.ModelProperty);

/**
 * Specify that the numeric type must be a multiple of some numeric value.
 *
 * @param value The numeric type must be a multiple of this value.
 */
extern dec multipleOf(target: numeric | Reflection.ModelProperty, value: valueof numeric);

/**
 * Specify that the array must contain at least one instance of the provided type.
 * Use \`@minContains\` and \`@maxContains\` to customize how many instances to expect.
 *
 * @param value The type the array must contain.
 */
extern dec contains(target: unknown[] | Reflection.ModelProperty, value: unknown);

/**
 * Used in conjunction with the \`@contains\` decorator,
 * specifies that the array must contain at least a certain number of the types provided by the \`@contains\` decorator.
 *
 * @param value The minimum number of instances the array must contain
 */
extern dec minContains(target: unknown[] | Reflection.ModelProperty, value: valueof int32);

/**
 * Used in conjunction with the \`@contains\` decorator,
 * specifies that the array must contain at most a certain number of the types provided by the \`@contains\` decorator.
 *
 * @param value The maximum number of instances the array must contain
 */
extern dec maxContains(target: unknown[] | Reflection.ModelProperty, value: valueof int32);

/**
 * Specify that every item in the array must be unique.
 */
extern dec uniqueItems(target: unknown[] | Reflection.ModelProperty);

/**
 * Specify the minimum number of properties this object can have.
 *
 * @param value The minimum number of properties this object can have.
 */
extern dec minProperties(target: Record<unknown> | Reflection.ModelProperty, value: valueof int32);

/**
 * Specify the maximum number of properties this object can have.
 *
 * @param value The maximum number of properties this object can have.
 */
extern dec maxProperties(target: Record<unknown> | Reflection.ModelProperty, value: valueof int32);

/**
 * Specify the encoding used for the contents of a string.
 * @param value
 */
extern dec contentEncoding(target: string | Reflection.ModelProperty, value: valueof string);

/**
 * Specify that the target array must begin with the provided types.
 *
 * @param value A tuple containing the types that must be present at the start of the array
 */
extern dec prefixItems(target: unknown[] | Reflection.ModelProperty, value: unknown[]);

/**
 * Specify the content type of content stored in a string.
 *
 * @param value The media type of the string contents
 *
 */
extern dec contentMediaType(target: string | Reflection.ModelProperty, value: valueof string);

/**
 * Specify the schema for the contents of a string when interpreted according to the content's
 * media type and encoding.
 *
 * @param value The schema of the string contents
 */
extern dec contentSchema(target: string | Reflection.ModelProperty, value: unknown);

/**
 * Specify a custom property to add to the emitted schema. This is useful for adding custom keywords
 * and other vendor-specific extensions. Scalar values need to be specified using \`typeof\` to be converted to a schema.
 * 
 * For example, \`@extension("x-schema", typeof "foo")\` will emit a JSON schema value for \`x-schema\`,
 * whereas \`@extension("x-schema", "foo")\` will emit the raw code \`"foo"\`.
 *
 * The value will be treated as a raw value if any of the following are true:
 * - The value is a scalar value (e.g. string, number, boolean, etc.)
 * - The value is wrapped in the \`Json<Data>\` template
 * - The value is provided using the value syntax (e.g. \`#{}\`, \`#[]\`)
 
 * For example, \`@extension("x-schema", { x: "value" })\` will emit a JSON schema value for \`x-schema\`,
 * whereas \`@extension("x-schema", #{x: "value"})\` and \`@extension("x-schema", Json<{x: "value"}>)\`
 * will emit the raw JSON code \`{x: "value"}\`.
 *
 * @param key The name of the keyword of vendor extension, e.g. \`x-custom\`.
 * @param value The value of the keyword.
 */
extern dec extension(target: unknown, key: valueof string, value: (valueof unknown) | unknown);

/**
 * Well-known JSON Schema formats.
 */
enum Format {
  /** A date and time, as defined by the \`date-time\` production in [RFC 3339](https://datatracker.ietf.org/doc/html/rfc3339#section-5.6). */
  dateTime: "date-time",

  /** A calendar date, as defined by the \`full-date\` production in [RFC 3339](https://datatracker.ietf.org/doc/html/rfc3339#section-5.6). */
  date: "date",

  /** A time of day, as defined by the \`full-time\` production in [RFC 3339](https://datatracker.ietf.org/doc/html/rfc3339#section-5.6). */
  time: "time",

  /** An ISO 8601 duration such as \`P3DT4H5M\`, as defined by the \`duration\` production in [RFC 3339, appendix A](https://datatracker.ietf.org/doc/html/rfc3339#appendix-A). */
  duration: "duration",

  /** An email address, as defined by the \`addr-spec\` production in [RFC 5322](https://datatracker.ietf.org/doc/html/rfc5322#section-3.4.1). */
  email: "email",

  /** An internationalized email address, as defined by [RFC 6531](https://datatracker.ietf.org/doc/html/rfc6531). */
  idnEmail: "idn-email",

  /** A host name, as defined by [RFC 1123](https://datatracker.ietf.org/doc/html/rfc1123#section-2.1). */
  hostname: "hostname",

  /** An internationalized host name, as defined by [RFC 5890](https://datatracker.ietf.org/doc/html/rfc5890#section-2.3.2.3). */
  idnHostname: "idn-hostname",

  /** An IPv4 address, as defined by the \`dotted-quad\` production in [RFC 2673](https://datatracker.ietf.org/doc/html/rfc2673#section-3.2). */
  ipv4: "ipv4",

  /** An IPv6 address, as defined by [RFC 4291](https://datatracker.ietf.org/doc/html/rfc4291#section-2.2). */
  ipv6: "ipv6",

  /** A URI, as defined by [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986). */
  uri: "uri",

  /** A URI reference, which may be relative, as defined by [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986#section-4.1). */
  uriReference: "uri-reference",

  /** An internationalized resource identifier, as defined by [RFC 3987](https://datatracker.ietf.org/doc/html/rfc3987). */
  iri: "iri",

  /** An internationalized resource identifier reference, which may be relative, as defined by [RFC 3987](https://datatracker.ietf.org/doc/html/rfc3987). */
  iriReference: "iri-reference",

  /** A universally unique identifier, as defined by [RFC 4122](https://datatracker.ietf.org/doc/html/rfc4122). */
  uuid: "uuid",

  /** A JSON pointer, as defined by [RFC 6901](https://datatracker.ietf.org/doc/html/rfc6901). */
  jsonPointer: "json-pointer",

  /** A relative JSON pointer, as defined by the [relative JSON pointer draft](https://datatracker.ietf.org/doc/html/draft-handrews-relative-json-pointer-01). */
  relativeJsonPointer: "relative-json-pointer",

  /** A regular expression, as defined by [ECMA-262](https://www.ecma-international.org/publications-and-standards/standards/ecma-262/). */
  regex: "regex",
}

/**
 * Specify that the provided template argument should be emitted as raw JSON or YAML
 * as opposed to a schema. Use in combination with the \`@extension\` decorator. For example,
 * \`@extension("x-schema", { x: "value" })\` will emit a JSON schema value for \`x-schema\`,
 * whereas \`@extension("x-schema", Json<{x: "value"}>)\` will emit the raw JSON code
 * \`{x: "value"}\`.
 *
 * @template Data the type to convert to raw JSON
 */
@Private.validatesRawJson(Data)
model Json<Data> {
  /** The value to emit as raw JSON or YAML. */
  value: Data;
}

namespace Private {
  extern dec validatesRawJson(target: Reflection.Model, value: unknown);
}
`}},"@typespec/protobuf":{version:"0.86.0",files:{"package.json":`{
  "name": "@typespec/protobuf",
  "version": "0.86.0",
  "author": "Microsoft Corporation",
  "description": "TypeSpec library and emitter for Protobuf (gRPC)",
  "homepage": "https://github.com/microsoft/typespec",
  "readme": "https://github.com/microsoft/typespec/blob/main/packages/protobuf/README.md",
  "license": "MIT",
  "repository": {
    "type": "git",
    "url": "git+https://github.com/microsoft/typespec.git"
  },
  "bugs": {
    "url": "https://github.com/microsoft/typespec/issues"
  },
  "keywords": [
    "typespec",
    "protobuf",
    "grpc"
  ],
  "main": "dist/src/index.js",
  "exports": {
    ".": {
      "typespec": "./lib/proto.tsp",
      "types": "./dist/src/index.d.ts",
      "default": "./dist/src/index.js"
    },
    "./testing": "./dist/src/testing/index.js"
  },
  "type": "module",
  "tspMain": "lib/proto.tsp",
  "files": [
    "lib/*.tsp",
    "dist/**",
    "!dist/test/**"
  ],
  "peerDependencies": {
    "@typespec/compiler": "^1.16.0"
  },
  "devDependencies": {
    "@types/micromatch": "^4.0.10",
    "@types/node": "^26.3.0",
    "@vitest/coverage-v8": "^4.1.11",
    "@vitest/ui": "^4.1.11",
    "micromatch": "^4.0.8",
    "rimraf": "^6.1.3",
    "typescript": "~6.0.2",
    "vitest": "^4.1.11",
    "@typespec/compiler": "^1.16.0",
    "@typespec/library-linter": "^0.86.0",
    "@typespec/tspd": "^0.77.1"
  },
  "scripts": {
    "clean": "rimraf ./dist ./temp",
    "build": "pnpm gen-extern-signature && tsc -p tsconfig.build.json && pnpm lint-typespec-library",
    "lint-typespec-library": "tsp compile . --warn-as-error --import @typespec/library-linter --no-emit",
    "watch": "tsc -p tsconfig.build.json --watch",
    "gen-extern-signature": "tspd --enable-experimental gen-extern-signature .",
    "test": "vitest run",
    "test:ci": "vitest run --coverage --reporter=junit --reporter=default",
    "lint": "oxlint . --deny-warnings",
    "lint:fix": "oxlint . --fix",
    "regen-docs": "tspd doc .  --enable-experimental --llmstxt --output-dir ../../website/src/content/docs/docs/emitters/protobuf/reference"
  }
}`,"lib/proto.tsp":'import "../dist/src/tsp-index.js";\n\nnamespace TypeSpec.Protobuf;\n\n/**\n * A model that represents an external Protobuf reference. This type can be used to import and utilize Protobuf\n * declarations that are not declared in TypeSpec within TypeSpec sources. When the emitter encounters an `Extern`, it\n * will insert an `import` statement for the corresponding `Path` and refer to the type by `Name`.\n *\n * #### Usage\n *\n * If you have a file called `test.proto` that declares a package named `test` and a message named `Widget`, you can\n * use the `Extern` type to declare a model in TypeSpec that refers to your external definition of `test.Widget`. See\n * the example below.\n *\n * When the TypeSpec definition of `Widget` is encountered, the Protobuf emitter will represent it as a reference to\n * `test.Widget` and insert an import for it, rather than attempt to convert the model to an equivalent message.\n *\n * @template Path the relative path to a `.proto` file to import\n * @template Name the fully-qualified reference to the type this model represents within the `.proto` file\n *\n * @example\n *\n * ```typespec\n * model Widget is Extern<"path/to/test.proto", "test.Widget">;\n * ```\n */\n@Private.externRef(Path, Name)\nmodel Extern<Path extends string, Name extends string> {\n  /**\n   * Never present. This property exists only so that `getEffectiveModelType` has something to look\n   * up: without it, an `Extern` model spread into an operation parameter yields an empty model that\n   * cannot be related back to its original definition.\n   */\n  _extern: never;\n}\n\n/**\n * Contains some common well-known Protobuf types defined by the google.protobuf library.\n */\nnamespace WellKnown {\n  /**\n   * An empty message.\n   *\n   * This model references `google.protobuf.Empty` from `google/protobuf/empty.proto`.\n   */\n  model Empty is Extern<"google/protobuf/empty.proto", "google.protobuf.Empty">;\n\n  /**\n   * A timestamp.\n   *\n   * This model references `google.protobuf.Timestamp` from `google/protobuf/timestamp.proto`.\n   */\n  model Timestamp is Extern<"google/protobuf/timestamp.proto", "google.protobuf.Timestamp">;\n\n  /**\n   * Any value.\n   *\n   * This model references `google.protobuf.Any` from `google/protobuf/any.proto`.\n   */\n  model Any is Extern<"google/protobuf/any.proto", "google.protobuf.Any">;\n\n  /**\n   * A latitude and longitude.\n   *\n   * This model references `google.type.LatLng` from `google/type/latlng.proto`.\n   */\n  model LatLng is Extern<"google/type/latlng.proto", "google.type.LatLng">;\n}\n\n/**\n * A signed 32-bit integer that will use the `sint32` encoding when used in a Protobuf message.\n *\n * #### Protobuf binary format\n *\n * Uses variable-length encoding. These more efficiently encode negative numbers than regular int32s.\n */\nscalar sint32 extends int32;\n\n/**\n * A signed 64-bit integer that will use the `sint64` encoding when used in a Protobuf message.\n *\n * #### Protobuf binary format\n *\n * Uses variable-length encoding. These more efficiently encode negative numbers than regular `int64s`.\n */\nscalar sint64 extends int64;\n\n/**\n * A signed 32-bit integer that will use the `sfixed32` encoding when used in a Protobuf message.\n *\n * #### Protobuf binary format\n *\n * Always four bytes.\n */\nscalar sfixed32 extends int32;\n\n/**\n * A signed 64-bit integer that will use the `sfixed64` encoding when used in a Protobuf message.\n *\n * #### Protobuf binary format\n *\n * Always eight bytes.\n */\nscalar sfixed64 extends int64;\n\n/**\n * An unsigned 32-bit integer that will use the `fixed32` encoding when used in a Protobuf message.\n *\n * #### Protobuf binary format\n *\n * Always four bytes. More efficient than `uint32` if values are often greater than 2<sup>28</sup>.\n */\nscalar fixed32 extends uint32;\n\n/**\n * An unsigned 64-bit integer that will use the `fixed64` encoding when used in a Protobuf message.\n *\n * #### Protobuf binary format\n *\n * Always eight bytes. More efficient than `uint64` if values are often greater than 2<sup>56</sup>.\n */\nscalar fixed64 extends uint64;\n\n/**\n * Types recognized as "integral" types\n */\nalias integral = int32 | int64 | uint32 | uint64 | boolean;\n\n/**\n * A type representing a Protobuf `map`. Instances of this type in models will be converted to the built-in `map` type\n * in Protobuf.\n *\n * The key type of a Protobuf `map` must be any integral type or `string`. The value type can be any type other than\n * another `Map`.\n *\n * @template Key the key type (any integral type or string)\n * @template Value the value type (any type other than another map)\n */\n@Private._map\nmodel Map<Key extends integral | string, Value> {}\n\n/**\n * Declares that a model is a Protobuf message.\n *\n * Messages can be detected automatically if either of the following two conditions are met:\n *\n * - The model has a `@field` annotation on all of its properties.\n * - The model is referenced by any service operation.\n *\n * This decorator will force the emitter to check and emit a model.\n */\nextern dec message(target: {});\n\n/**\n * Defines the field index of a model property for conversion to a Protobuf\n * message.\n *\n * The field index of a Protobuf message must:\n *   - fall between 1 and 2<sup>29</sup> - 1, inclusive.\n *   - not fall within the implementation reserved range of 19000 to 19999, inclusive.\n *   - not fall within any range that was [marked reserved](#%40TypeSpec.Protobuf.reserve).\n *\n * #### API Compatibility Note\n *\n * Fields are accessed by index, so changing the index of a field is an API breaking change.\n *\n * #### Encoding\n *\n * Field indices between 1 and 15 are encoded using a single byte, while field indices from 16 through 2047 require two\n * bytes, so those indices between 1 and 15 should be preferred and reserved for elements that are frequently or always\n * set in the message. See the [Protobuf binary format](https://protobuf.dev/programming-guides/encoding/).\n *\n * @param index The whole-number index of the field.\n *\n * @example\n *\n * ```typespec\n * model ExampleMessage {\n *   @field(1)\n *   test: string;\n * }\n * ```\n */\nextern dec field(target: TypeSpec.Reflection.ModelProperty, index: valueof uint32);\n\n/**\n * Reserve a field index, range, or name. If a field definition collides with a reservation, the emitter will produce\n * an error.\n *\n * This decorator accepts multiple reservations. Each reservation is one of the following:\n *\n * - a `string`, in which case the reservation refers to a field name.\n * - a `uint32`, in which case the reservation refers to a field index.\n * - a tuple `[uint32, uint32]`, in which case the reservation refers to a field range that is _inclusive_ of both ends.\n *\n * Unlike in Protobuf, where field name and index reservations must be separated, you can mix string and numeric field\n * reservations in a single `@reserve` call in TypeSpec.\n *\n * #### API Compatibility Note\n *\n * Field reservations prevent users of your Protobuf specification from using the given field names or indices. This can\n * be useful if a field is removed, as it will further prevent adding a new, incompatible field and will prevent users\n * from utilizing the field index at runtime in a way that may break compatibility with users of older specifications.\n *\n * See _[Protobuf Language Guide - Reserved Fields](https://protobuf.dev/programming-guides/proto3/#reserved)_ for more\n * information.\n *\n * @param reservations a list of field reservations\n *\n * @example\n *\n * ```typespec\n * // Reserve the fields 8-15 inclusive, 100, and the field name "test" within a model.\n * @reserve([8, 15], 100, "test")\n * model Example {\n *   // ...\n * }\n * ```\n */\nextern dec reserve(target: {}, ...reservations: valueof (string | [uint32, uint32] | uint32)[]);\n\n/**\n * Declares that a TypeSpec interface constitutes a Protobuf service. The contents of the interface will be converted to\n * a `service` declaration in the resulting Protobuf file.\n */\nextern dec service(target: TypeSpec.Reflection.Interface);\n\n// FIXME: cannot link to the package decorator directly because it is detected as a broken link.\n/**\n * Details applied to a package definition by the [`@package`](./decorators#%40TypeSpec.Protobuf.package) decorator.\n */\nmodel PackageDetails {\n  /**\n   * The package\'s name.\n   *\n   * By default, the package\'s name is constructed from the namespace it is applied to.\n   */\n  name?: string;\n\n  /**\n   * The package\'s top-level options.\n   *\n   * See the [Protobuf Language Guide - Options](https://protobuf.dev/programming-guides/proto3/#options) for more information.\n   *\n   * Currently, only string, boolean, and numeric options are supported.\n   */\n  options?: Record<string | boolean | numeric>;\n}\n\n/**\n * Declares that a TypeSpec namespace constitutes a Protobuf package. The contents of the namespace will be emitted to a\n * single Protobuf file.\n *\n * @param details the optional details of the package\n */\nextern dec `package`(target: TypeSpec.Reflection.Namespace, details?: PackageDetails);\n\n/**\n * The streaming mode of an operation. One of:\n *\n * - `Duplex`: both the input and output of the operation are streaming.\n * - `In`: the input of the operation is streaming.\n * - `Out`: the output of the operation is streaming.\n * - `None`: neither the input nor the output are streaming.\n *\n * See the [`@stream`](./decorators#%40TypeSpec.Protobuf.stream) decorator.\n */\nenum StreamMode {\n  /**\n   * Both the input and output of the operation are streaming. Both the client and service will stream messages to each\n   * other until the connections are closed.\n   */\n  Duplex,\n\n  /**\n   * The input of the operation is streaming. The client will send a stream of events; and, once the stream is closed,\n   * the service will respond with a message.\n   */\n  In,\n\n  /**\n   * The output of the operation is streaming. The client will send a message to the service, and the service will send\n   * a stream of events back to the client.\n   */\n  Out,\n\n  /**\n   * Neither the input nor the output are streaming. This is the default mode of an operation without the `@stream`\n   * decorator.\n   */\n  None,\n}\n\n/**\n * Set the streaming mode of an operation. See [StreamMode](./data-types#TypeSpec.Protobuf.StreamMode) for more information.\n *\n * @param mode The streaming mode to apply to this operation.\n *\n * @example\n *\n * ```typespec\n * @stream(StreamMode.Out)\n * op logs(...LogsRequest): LogEvent;\n * ```\n *\n * @example\n *\n * ```typespec\n * @stream(StreamMode.Duplex)\n * op connectToMessageService(...Message): Message;\n * ```\n */\nextern dec stream(target: TypeSpec.Reflection.Operation, mode: StreamMode);\n\nnamespace Private {\n  extern dec externRef(target: Reflection.Model, path: string, name: string);\n  extern dec _map(target: Reflection.Model);\n}\n'}},"@typespec/versioning":{version:"0.86.0",files:{"package.json":`{
  "name": "@typespec/versioning",
  "version": "0.86.0",
  "author": "Microsoft Corporation",
  "description": "TypeSpec library for declaring and emitting versioned APIs",
  "homepage": "https://typespec.io",
  "readme": "https://github.com/microsoft/typespec/blob/main/README.md",
  "license": "MIT",
  "repository": {
    "type": "git",
    "url": "git+https://github.com/microsoft/typespec.git"
  },
  "bugs": {
    "url": "https://github.com/microsoft/typespec/issues"
  },
  "keywords": [
    "typespec"
  ],
  "type": "module",
  "main": "dist/src/index.js",
  "tspMain": "lib/main.tsp",
  "exports": {
    ".": {
      "typespec": "./lib/main.tsp",
      "types": "./dist/src/index.d.ts",
      "default": "./dist/src/index.js"
    },
    "./testing": {
      "types": "./dist/src/testing/index.d.ts",
      "default": "./dist/src/testing/index.js"
    }
  },
  "engines": {
    "node": ">=22.0.0"
  },
  "files": [
    "lib/*.tsp",
    "dist/**",
    "!dist/test/**"
  ],
  "peerDependencies": {
    "@typespec/compiler": "^1.16.0"
  },
  "devDependencies": {
    "@types/node": "^26.3.0",
    "@vitest/coverage-v8": "^4.1.11",
    "@vitest/ui": "^4.1.11",
    "rimraf": "^6.1.3",
    "typescript": "~6.0.2",
    "vitest": "^4.1.11",
    "@typespec/compiler": "^1.16.0",
    "@typespec/tspd": "^0.77.1",
    "@typespec/library-linter": "^0.86.0"
  },
  "scripts": {
    "clean": "rimraf ./dist ./temp",
    "build": "pnpm gen-extern-signature && tsc -p tsconfig.build.json && pnpm lint-typespec-library",
    "watch": "tsc -p tsconfig.build.json --watch",
    "gen-extern-signature": "tspd --enable-experimental gen-extern-signature .",
    "lint-typespec-library": "tsp compile . --warn-as-error --import @typespec/library-linter --no-emit",
    "test": "vitest run",
    "test:ui": "vitest --ui",
    "test:ci": "vitest run --coverage --reporter=junit --reporter=default",
    "lint": "oxlint . --deny-warnings",
    "lint:fix": "oxlint . --fix",
    "regen-docs": "tspd doc .  --enable-experimental --llmstxt --output-dir ../../website/src/content/docs/docs/libraries/versioning/reference"
  }
}`,"lib/decorators.tsp":`import "../dist/src/decorators.js";

using TypeSpec.Reflection;

namespace TypeSpec.Versioning;

/**
 * Identifies that the decorated namespace is versioned by the provided enum.
 * @param versions The enum that describes the supported versions.
 *
 * @example
 *
 * \`\`\`tsp
 * @versioned(Versions)
 * namespace MyService;
 * enum Versions {
 *   v1,
 *   v2,
 *   v3,
 * }
 * \`\`\`
 */
extern dec versioned(target: Namespace, versions: Enum);

/**
 * Identifies that a namespace or a given versioning enum member relies upon a versioned package.
 * @param versionRecords The dependent library version(s) for the target namespace or version.
 *
 * @example Select a single version of \`MyLib\` to use
 *
 * \`\`\`tsp
 * @useDependency(MyLib.Versions.v1_1)
 * namespace NonVersionedService;
 * \`\`\`
 *
 * @example Select which version of the library match to which version of the service.
 *
 * \`\`\`tsp
 * @versioned(Versions)
 * namespace MyService1;
 * enum Version {
 *   @useDependency(MyLib.Versions.v1_1) // V1 use lib v1_1
 *   v1,
 *   @useDependency(MyLib.Versions.v1_1) // V2 use lib v1_1
 *   v2,
 *   @useDependency(MyLib.Versions.v2) // V3 use lib v2
 *   v3,
 * }
 * \`\`\`
 */
extern dec useDependency(target: EnumMember | Namespace, ...versionRecords: EnumMember[]);

/**
 * Identifies when the target was added.
 * @param version The version that the target was added in.
 *
 * @example
 *
 * \`\`\`tsp
 * @added(Versions.v2)
 * op addedInV2(): void;
 *
 * @added(Versions.v2)
 * model AlsoAddedInV2 {}
 *
 * model Foo {
 *   name: string;
 *
 *   @added(Versions.v3)
 *   addedInV3: string;
 * }
 * \`\`\`
 */
extern dec added(
  target:
    | Model
    | ModelProperty
    | Operation
    | Enum
    | EnumMember
    | Union
    | UnionVariant
    | Scalar
    | Interface,
  version: EnumMember
);

/**
 * Identifies when the target was removed.
 * @param version The version that the target was removed in.
 *
 * @example
 * \`\`\`tsp
 * @removed(Versions.v2)
 * op removedInV2(): void;
 *
 * @removed(Versions.v2)
 * model AlsoRemovedInV2 {}
 *
 * model Foo {
 *   name: string;
 *
 *   @removed(Versions.v3)
 *   removedInV3: string;
 * }
 * \`\`\`
 */
extern dec removed(
  target:
    | Model
    | ModelProperty
    | Operation
    | Enum
    | EnumMember
    | Union
    | UnionVariant
    | Scalar
    | Interface,
  version: EnumMember
);

/**
 * Identifies when the target has been renamed.
 * @param version The version that the target was renamed in.
 * @param oldName The previous name of the target.
 *
 * @example
 * \`\`\`tsp
 * @renamedFrom(Versions.v2, "oldName")
 * op newName(): void;
 * \`\`\`
 */
extern dec renamedFrom(
  target:
    | Model
    | ModelProperty
    | Operation
    | Enum
    | EnumMember
    | Union
    | UnionVariant
    | Scalar
    | Interface,
  version: EnumMember,
  oldName: valueof string
);

/**
 * Identifies when a target was made optional.
 * @param version The version that the target was made optional in.
 *
 * @example
 *
 * \`\`\`tsp
 * model Foo {
 *   name: string;
 *   @madeOptional(Versions.v2)
 *   nickname?: string;
 * }
 * \`\`\`
 */
extern dec madeOptional(target: ModelProperty, version: EnumMember);

/**
 * Identifies when a target was made required.
 * @param version The version that the target was made required in.
 *
 * @example
 *
 * \`\`\`tsp
 * model Foo {
 *   name: string;
 *   @madeRequired(Versions.v2)
 *   nickname: string;
 * }
 * \`\`\`
 */
extern dec madeRequired(target: ModelProperty, version: EnumMember);

/**
 * Declares that the type of a model property has changed starting at a given version,
 * while keeping earlier versions consistent with the previous type.
 *
 * This decorator is used to track type changes across API versions. When applied,
 * the property will use \`oldType\` in versions before the specified \`version\`,
 * and the current type definition in the specified version and later.
 *
 * @param version The version when the type change takes effect. The new type applies
 *                from this version onwards, while the old type applies to earlier versions.
 * @param oldType The previous type used before the specified version.
 *
 * @example
 *
 * \`\`\`tsp
 * model Foo {
 *   // In v1: id is a string
 *   // In v2+: id is an int32
 *   @typeChangedFrom(Versions.v2, string)
 *   id: int32;
 * }
 * \`\`\`
 */
extern dec typeChangedFrom(target: ModelProperty, version: EnumMember, oldType: unknown);

/**
 * Declares that the return type of an operation has changed starting at a given version,
 * while keeping earlier versions consistent with the previous return type.
 *
 * This decorator is used to track return type changes across API versions. When applied,
 * the operation will return \`oldType\` in versions before the specified \`version\`,
 * and the current return type definition in the specified version and later.
 *
 * @param version The version when the return type change takes effect. The new return type applies
 *                from this version onwards, while the old return type applies to earlier versions.
 * @param oldType The previous return type used before the specified version.
 *
 * @example
 *
 * \`\`\`tsp
 * // In v1: returns a string
 * // In v2+: returns an int32
 * @returnTypeChangedFrom(Versions.v2, string)
 * op getUserId(): int32;
 * \`\`\`
 */
extern dec returnTypeChangedFrom(target: Operation, version: EnumMember, oldType: unknown);
`,"lib/main.tsp":`import "./decorators.tsp";
import "../dist/src/validate.js";
`}},"@typespec/openapi3":{version:"1.16.0",files:{"package.json":`{
  "name": "@typespec/openapi3",
  "version": "1.16.0",
  "author": "Microsoft Corporation",
  "description": "TypeSpec library for emitting OpenAPI 3.0 and OpenAPI 3.1 from the TypeSpec REST protocol binding and converting OpenAPI3 to TypeSpec",
  "homepage": "https://typespec.io",
  "readme": "https://github.com/microsoft/typespec/blob/main/README.md",
  "license": "MIT",
  "repository": {
    "type": "git",
    "url": "git+https://github.com/microsoft/typespec.git"
  },
  "bugs": {
    "url": "https://github.com/microsoft/typespec/issues"
  },
  "keywords": [
    "typespec"
  ],
  "bin": {
    "tsp-openapi3": "cmd/tsp-openapi3.js"
  },
  "type": "module",
  "main": "dist/src/index.js",
  "tspMain": "lib/main.tsp",
  "exports": {
    ".": {
      "typespec": "./lib/main.tsp",
      "types": "./dist/src/index.d.ts",
      "default": "./dist/src/index.js"
    },
    "./testing": {
      "types": "./dist/src/testing/index.d.ts",
      "default": "./dist/src/testing/index.js"
    }
  },
  "imports": {
    "#test/*": "./test/*"
  },
  "engines": {
    "node": ">=22.0.0"
  },
  "files": [
    "lib/*.tsp",
    "dist/**",
    "!dist/test/**"
  ],
  "dependencies": {
    "@scalar/json-magic": "^0.13.2",
    "@scalar/openapi-parser": "^0.28.16",
    "@scalar/openapi-types": "^0.9.5",
    "yaml": "^2.9.0",
    "@typespec/asset-emitter": "^0.79.2"
  },
  "peerDependencies": {
    "@typespec/compiler": "^1.16.0",
    "@typespec/events": "^0.86.0",
    "@typespec/http": "^1.16.0",
    "@typespec/json-schema": "^1.16.0",
    "@typespec/openapi": "^1.16.0",
    "@typespec/sse": "^0.86.0",
    "@typespec/streams": "^0.86.0",
    "@typespec/versioning": "^0.86.0"
  },
  "peerDependenciesMeta": {
    "@typespec/json-schema": {
      "optional": true
    },
    "@typespec/xml": {
      "optional": true
    },
    "@typespec/versioning": {
      "optional": true
    },
    "@typespec/streams": {
      "optional": true
    },
    "@typespec/events": {
      "optional": true
    },
    "@typespec/sse": {
      "optional": true
    }
  },
  "devDependencies": {
    "@types/node": "^26.3.0",
    "@types/yargs": "^17.0.35",
    "@vitest/coverage-v8": "^4.1.11",
    "@vitest/ui": "^4.1.11",
    "cross-env": "^10.1.0",
    "rimraf": "^6.1.3",
    "typescript": "~6.0.2",
    "vitest": "^4.1.11",
    "@typespec/json-schema": "^1.16.0",
    "@typespec/library-linter": "^0.86.0",
    "@typespec/rest": "^0.86.0",
    "@typespec/sse": "^0.86.0",
    "@typespec/openapi": "^1.16.0",
    "@typespec/streams": "^0.86.0",
    "@typespec/xml": "^0.86.0",
    "@typespec/versioning": "^0.86.0",
    "@typespec/tspd": "^0.77.1",
    "@typespec/events": "^0.86.0",
    "@typespec/http": "^1.16.0",
    "@typespec/compiler": "^1.16.0"
  },
  "scripts": {
    "clean": "rimraf ./dist ./temp",
    "build": "pnpm gen-extern-signature && pnpm quickbuild && pnpm lint-typespec-library",
    "quickbuild": "tsc -p tsconfig.build.json",
    "watch": "tsc -p tsconfig.build.json --watch",
    "gen-extern-signature": "tspd --enable-experimental gen-extern-signature .",
    "lint-typespec-library": "tsp compile . --warn-as-error --import @typespec/library-linter --no-emit",
    "test": "vitest run",
    "test:watch": "vitest -w",
    "test:ui": "vitest --ui",
    "test:ci": "vitest run --coverage --reporter=junit --reporter=default",
    "lint": "oxlint . --deny-warnings",
    "lint:fix": "oxlint . --fix",
    "regen-docs": "tspd doc .  --enable-experimental --llmstxt --output-dir ../../website/src/content/docs/docs/emitters/openapi3/reference",
    "regen-specs": "cross-env RECORD=true vitest run",
    "api-extractor": "api-extractor run --local --verbose"
  }
}`,"lib/decorators.tsp":`import "../dist/src/tsp-index.js";

namespace TypeSpec.OpenAPI;

using TypeSpec.Reflection;

/**
 * Specify that \`oneOf\` should be used instead of \`anyOf\` for that union.
 */
extern dec oneOf(target: Union | ModelProperty);
/**
 * Specify an external reference that should be used inside of emitting this type.
 * @param ref External reference(e.g. "../../common.json#/components/schemas/Foo")
 */
extern dec useRef(target: Model | ModelProperty, ref: valueof string);
`,"lib/main.tsp":`import "./decorators.tsp";
`}},"@typespec/sse":{version:"0.86.0",files:{"package.json":`{
  "name": "@typespec/sse",
  "version": "0.86.0",
  "author": "Microsoft Corporation",
  "description": "TypeSpec library providing server sent events bindings",
  "homepage": "https://typespec.io",
  "readme": "https://github.com/microsoft/typespec/blob/main/README.md",
  "license": "MIT",
  "repository": {
    "type": "git",
    "url": "git+https://github.com/microsoft/typespec.git"
  },
  "bugs": {
    "url": "https://github.com/microsoft/typespec/issues"
  },
  "keywords": [
    "typespec"
  ],
  "type": "module",
  "main": "dist/src/index.js",
  "tspMain": "lib/main.tsp",
  "exports": {
    ".": {
      "typespec": "./lib/main.tsp",
      "default": "./dist/src/index.js"
    },
    "./testing": "./dist/src/testing/index.js"
  },
  "engines": {
    "node": ">=22.0.0"
  },
  "files": [
    "lib/*.tsp",
    "dist/**",
    "!dist/test/**"
  ],
  "peerDependencies": {
    "@typespec/compiler": "^1.16.0",
    "@typespec/events": "^0.86.0",
    "@typespec/http": "^1.16.0",
    "@typespec/streams": "^0.86.0"
  },
  "devDependencies": {
    "@types/node": "^26.3.0",
    "@vitest/coverage-v8": "^4.1.11",
    "@vitest/ui": "^4.1.11",
    "rimraf": "^6.1.3",
    "typescript": "~6.0.2",
    "vitest": "^4.1.11",
    "@typespec/compiler": "^1.16.0",
    "@typespec/events": "^0.86.0",
    "@typespec/http": "^1.16.0",
    "@typespec/library-linter": "^0.86.0",
    "@typespec/tspd": "^0.77.1",
    "@typespec/streams": "^0.86.0"
  },
  "scripts": {
    "clean": "rimraf ./dist ./temp",
    "build": "pnpm gen-extern-signature && tsc -p tsconfig.build.json && pnpm lint-typespec-library",
    "watch": "tsc -p tsconfig.build.json --watch",
    "gen-extern-signature": "tspd --enable-experimental gen-extern-signature .",
    "lint-typespec-library": "tsp compile . --warn-as-error --import @typespec/library-linter --no-emit",
    "test": "vitest run",
    "test:watch": "vitest -w",
    "test:ui": "vitest --ui",
    "test-official": "vitest run --coverage --reporter=junit --reporter=default --no-file-parallelism",
    "lint": "oxlint . --deny-warnings",
    "lint:fix": "oxlint . --fix",
    "regen-docs": "tspd doc .  --enable-experimental  --output-dir ../../website/src/content/docs/docs/libraries/sse/reference"
  }
}`,"lib/decorators.tsp":`using TypeSpec.Reflection;

namespace TypeSpec.SSE;

/**
 * Indicates that the presence of this event is a terminal event,
 * and the client should disconnect from the server.
 */
extern dec terminalEvent(target: UnionVariant);
`,"lib/main.tsp":`import "../dist/src/tsp-index.js";
import "./decorators.tsp";
import "./types.tsp";
`,"lib/types.tsp":`import "@typespec/http/streams";

using Http.Streams;

namespace TypeSpec.SSE;

/**
 * Describes a stream of server-sent events.
 *
 * The content-type is set to \`text/event-stream\`.
 *
 * The server-sent events are described by \`Type\`.
 * The event type for any event can be defined by using named union variants.
 * When a union variant is not named, it is considered a 'message' event.
 *
 * @template Type The set of models describing the server-sent events.
 *
 * @example Mix of named union variants and terminal event
 *
 * \`\`\`typespec
 * model UserConnect {
 *   username: string;
 *   time: string;
 * }
 *
 * model UserMessage {
 *   username: string;
 *   time: string;
 *   text: string;
 * }
 *
 * model UserDisconnect {
 *   username: string;
 *   time: string;
 * }
 *
 * @TypeSpec.Events.events
 * union ChannelEvents {
 *   userconnect: UserConnect,
 *   usermessage: UserMessage,
 *   userdisconnect: UserDisconnect,
 *
 *   @Events.contentType("text/plain")
 *   @terminalEvent
 *   "[unsubscribe]",
 * }
 *
 * op subscribeToChannel(): SSEStream<ChannelEvents>;
 * \`\`\`
 */
@doc("")
model SSEStream<Type extends TypeSpec.Reflection.Union> is HttpStream<Type, "text/event-stream">;
`}},"@typespec/events":{version:"0.86.0",files:{"package.json":`{
  "name": "@typespec/events",
  "version": "0.86.0",
  "author": "Microsoft Corporation",
  "description": "TypeSpec library providing events bindings",
  "homepage": "https://typespec.io",
  "readme": "https://github.com/microsoft/typespec/blob/main/README.md",
  "license": "MIT",
  "repository": {
    "type": "git",
    "url": "git+https://github.com/microsoft/typespec.git"
  },
  "bugs": {
    "url": "https://github.com/microsoft/typespec/issues"
  },
  "keywords": [
    "typespec"
  ],
  "type": "module",
  "main": "dist/src/index.js",
  "tspMain": "lib/main.tsp",
  "exports": {
    ".": {
      "typespec": "./lib/main.tsp",
      "default": "./dist/src/index.js"
    },
    "./testing": "./dist/src/testing/index.js",
    "./experimental": {
      "types": "./dist/src/experimental/index.d.ts",
      "default": "./dist/src/experimental/index.js"
    }
  },
  "engines": {
    "node": ">=22.0.0"
  },
  "files": [
    "lib/*.tsp",
    "dist/**",
    "!dist/test/**"
  ],
  "peerDependencies": {
    "@typespec/compiler": "^1.16.0"
  },
  "devDependencies": {
    "@types/node": "^26.3.0",
    "@vitest/coverage-v8": "^4.1.11",
    "@vitest/ui": "^4.1.11",
    "rimraf": "^6.1.3",
    "typescript": "~6.0.2",
    "vitest": "^4.1.11",
    "@typespec/compiler": "^1.16.0",
    "@typespec/library-linter": "^0.86.0",
    "@typespec/tspd": "^0.77.1"
  },
  "scripts": {
    "clean": "rimraf ./dist ./temp",
    "build": "pnpm gen-extern-signature && tsc -p tsconfig.build.json && pnpm lint-typespec-library",
    "watch": "tsc -p tsconfig.build.json --watch",
    "gen-extern-signature": "tspd --enable-experimental gen-extern-signature .",
    "lint-typespec-library": "tsp compile . --warn-as-error --import @typespec/library-linter --no-emit",
    "test": "vitest run",
    "test:watch": "vitest -w",
    "test:ui": "vitest --ui",
    "test-official": "vitest run --coverage --reporter=junit --reporter=default --no-file-parallelism",
    "lint": "oxlint . --deny-warnings",
    "lint:fix": "oxlint . --fix",
    "regen-docs": "tspd doc .  --enable-experimental --llmstxt --output-dir ../../website/src/content/docs/docs/libraries/events/reference"
  }
}`,"lib/decorators.tsp":`using TypeSpec.Reflection;

namespace TypeSpec.Events;

/**
 * Specify that this union describes a set of events.
 *
 * @example
 *
 * \`\`\`typespec
 * @events
 * union MixedEvents {
 *   pingEvent: string;
 *
 *   doneEvent: "done";
 * }
 * \`\`\`
 */
extern dec events(target: Union);

/**
 * Specifies the content type of the event envelope, event body, or event payload.
 * When applied to an event payload, that field must also have a corresponding \`@data\`
 * decorator.
 *
 * @example
 *
 * \`\`\`typespec
 * @events union MixedEvents {
 *   @contentType("application/json")
 *   message: { id: string, text: string, }
 * }
 * \`\`\`
 *
 * @example Specify the content type of the event payload.
 *
 * \`\`\`typespec
 * @events union MixedEvents {
 *   { done: true },
 *
 *   { done: false, @data @contentType("text/plain") value: string,}
 * }
 * \`\`\`
 *
 * @param contentType The content type of the event or event payload.
 */
extern dec contentType(target: UnionVariant | ModelProperty, contentType: valueof string);

/**
 * Identifies the payload of an event.
 * Only one field in an event can be marked as the payload.
 *
 * @example
 *
 * \`\`\`typespec
 * @events union MixedEvents {
 *   { metadata: Record<string>, @data payload: string,}
 * }
 * \`\`\`
 */
extern dec data(target: ModelProperty);
`,"lib/main.tsp":`import "../dist/src/tsp-index.js";
import "./decorators.tsp";
`}}};var We=kf,If={"@typespec/graphql/dist/src/tsp-index.js":ti,"@typespec/json-schema/dist/src/tsp-index.js":ci,"@typespec/protobuf/dist/src/tsp-index.js":wi,"@typespec/openapi3/dist/src/tsp-index.js":Ri,"@typespec/sse/dist/src/tsp-index.js":th,"@typespec/events/dist/src/tsp-index.js":vi,"@typespec/versioning/dist/src/decorators.js":vh,"@typespec/versioning/dist/src/validate.js":uh,"@typespec/http/dist/src/tsp-index.js":sh,"@typespec/rest/dist/src/tsp-index.js":Gh,"@typespec/rest/dist/src/internal-decorators.js":zh,"@typespec/openapi/dist/src/tsp-index.js":Ph,"@typespec/streams/dist/src/tsp-index.js":Uf};var Xh={id:"umf.typespec",version:"0.1.0",coreVersion:"0.1.0",description:"TypeSpec source bundles with pinned syntax and supplied-file compilation",schema:{$schema:"https://json-schema.org/draft/2020-12/schema",$id:"urn:umf:typespec:0.1.0",type:"object",required:["profile","entrypoint","files"],properties:{profile:{const:"typespec-1.16.0-sources"},entrypoint:{type:"string",minLength:1},files:{type:"object",minProperties:1,propertyNames:{type:"string",minLength:1},additionalProperties:{type:"string"}},libraries:{type:"object",propertyNames:{type:"string",minLength:1},additionalProperties:{type:"string",minLength:1}}}},scopes:["element"],semantics:"CONTRACT-013. Exact source files and explicit entrypoint; compiler validity and downstream projection fidelity are separate.",capabilities:{validation:"semantic",directions:["import","export"],native:{system:"TypeSpec",version:"1.16.0",subset:"Supplied .tsp sources with pinned compiler/standard library and eleven explicitly selected official library versions; library-owned configuration retained; separate pinned JSON Schema emission API; no arbitrary project JavaScript or lossless projection claim"},evidence:["tests/typespec/native.test.ts","fixtures/typespec/compiler-oracle-results.json","spec/extensions/typespec/standard-manifest.json","tests/typespec/semantic.test.ts","fixtures/typespec/semantic-oracle-results.json","tests/typespec/corpus.test.ts","fixtures/typespec/upstream/manifest.json","fixtures/typespec/corpus-results.json","fixtures/typespec/corpus-oracle-results.json","tests/typespec/libraries.test.ts","spec/extensions/typespec/libraries-manifest.json","fixtures/typespec/library-corpus-results.json","fixtures/typespec/library-corpus-oracle-results.json","tests/typespec/all-libraries.test.ts","fixtures/typespec/all-library-corpus-results.json","fixtures/typespec/all-library-corpus-oracle-results.json","tests/typespec/emission.test.ts","fixtures/typespec/emission-oracle-results.json","fixtures/typespec/emission-json-oracle-results.json","fixtures/typespec/emission/numeric.tsp","fixtures/typespec/emission/numeric-oracle-results.json","tests/typespec/projection.test.ts","fixtures/typespec/emission/projection-oracle-results.json"]}};var Yh={};an(Yh,{setExtension:()=>fi,namespace:()=>Bo,isOneOf:()=>oe,isJsonSchemaDeclaration:()=>$e,getUniqueItems:()=>Js,getPrefixItems:()=>js,getMultipleOfAsNumeric:()=>li,getMultipleOf:()=>Us,getMinProperties:()=>Os,getMinContains:()=>qs,getMaxProperties:()=>Xs,getMaxContains:()=>Gs,getJsonSchemaTypes:()=>Ts,getJsonSchema:()=>ms,getId:()=>Ye,getExtensions:()=>Cs,getContentSchema:()=>Hs,getContentMediaType:()=>_s,getContentEncoding:()=>$s,getContains:()=>As,getBaseUri:()=>di,findBaseUri:()=>vs,JsonSchemaEmitter:()=>as,EmitterOptionsSchema:()=>Rs,$uniqueItems:()=>Qs,$prefixItems:()=>Ds,$oneOf:()=>Is,$onEmit:()=>Sf,$multipleOf:()=>ks,$minProperties:()=>Ps,$minContains:()=>Ws,$maxProperties:()=>Zs,$maxContains:()=>zs,$lib:()=>ae,$jsonSchema:()=>Es,$id:()=>_e,$flags:()=>Pe,$extension:()=>Ss,$decorators:()=>ai,$contentSchema:()=>Ns,$contentMediaType:()=>Fs,$contentEncoding:()=>Ys,$contains:()=>Ls,$baseUri:()=>Ze});class Dn{#n=new Map;#e;constructor(n){this.#e=n}get(n){return this.#n.get(this.#e(n))}set(n,e){let s=this.#e(n);this.#n.set(s,e)}static objectKeyer(){let n=new WeakMap,e=0;return{getKey(s){if(n.has(s))return n.get(s);let t=e;return e++,n.set(s,t),t}}}}class S{#n=[];setValue(n){for(let e of this.#n)e(n)}onValue(n){this.#n.push(n)}}function Af(n){let e=[];while(n)e.unshift(n),n=n.parentScope;return e}function $t(n,e){let s=n.scope,t=Af(s),i=Af(e),h=0;while(t[h]&&i[h]&&t[h]===i[h])h++;let d=i.slice(h),f=t.slice(h),l=t[h-1]??null;return{pathUp:d,pathDown:f,commonScope:l}}class hs{containsDeclaration;#n;constructor(n){let e=n.findIndex((s)=>s.entity.kind==="declaration");this.containsDeclaration=e!==-1,this.#n=this.containsDeclaration?[...n.slice(e),...n.slice(0,e)]:n}get first(){return this.#n[0]}[Symbol.iterator](){return this.#n[Symbol.iterator]()}[Symbol.toStringTag](){return[...this.#n,this.#n[0]].map((n)=>A(n.type)).join(" -> ")}toString(){return this[Symbol.toStringTag]()}}class Yt extends S{segments=[];#n=new Set;#e(){let n=this.segments.join("");this.setValue(n)}#i(n,e){for(let[s,t]of this.segments.entries())if(t===n)this.segments[s]=e;if(this.#n.delete(n),this.#n.size===0)this.#e()}pushLiteralSegment(n){if(this.#s())this.segments[this.segments.length-1]+=n;else this.segments.push(n)}pushPlaceholder(n){this.#n.add(n),n.onValue((e)=>{this.#i(n,e)}),this.segments.push(n)}pushStringBuilder(n){for(let e of n.segments)this.push(e)}push(n){if(typeof n==="string")this.pushLiteralSegment(n);else if(n instanceof Yt)this.pushStringBuilder(n);else this.pushPlaceholder(n)}reduce(){if(this.#n.size===0)return this.segments.join("");return this}#s(){return this.segments.length>0&&typeof this.segments[this.segments.length-1]==="string"}}class ls{emitter;constructor(n){this.emitter=n}programContext(n){return{}}namespace(n){for(let e of n.namespaces.values())this.emitter.emitType(e);for(let e of n.models.values())if(!H(e))this.emitter.emitType(e);for(let e of n.operations.values())if(!H(e))this.emitter.emitType(e);for(let e of n.enums.values())this.emitter.emitType(e);for(let e of n.unions.values())if(!H(e))this.emitter.emitType(e);for(let e of n.interfaces.values())if(!H(e))this.emitter.emitType(e);for(let e of n.scalars.values())this.emitter.emitType(e);return this.emitter.result.none()}namespaceContext(n){return{}}namespaceReferenceContext(n){return{}}modelLiteral(n){if(n.baseModel)this.emitter.emitType(n.baseModel);return this.emitter.emitModelProperties(n),this.emitter.result.none()}modelLiteralContext(n){return{}}modelLiteralReferenceContext(n){return{}}modelDeclaration(n,e){if(n.baseModel)this.emitter.emitType(n.baseModel);return this.emitter.emitModelProperties(n),this.emitter.result.none()}modelDeclarationContext(n,e){return{}}modelDeclarationReferenceContext(n,e){return{}}modelInstantiation(n,e){if(n.baseModel)this.emitter.emitType(n.baseModel);return this.emitter.emitModelProperties(n),this.emitter.result.none()}modelInstantiationContext(n,e){return{}}modelInstantiationReferenceContext(n,e){return{}}modelProperties(n){for(let e of n.properties.values())this.emitter.emitModelProperty(e);return this.emitter.result.none()}modelPropertiesContext(n){return{}}modelPropertiesReferenceContext(n){return{}}modelPropertyLiteral(n){return this.emitter.emitTypeReference(n.type),this.emitter.result.none()}modelPropertyLiteralContext(n){return{}}modelPropertyLiteralReferenceContext(n){return{}}modelPropertyReference(n){return this.emitter.emitTypeReference(n.type)}enumMemberReference(n){return this.emitter.result.none()}arrayDeclaration(n,e,s){return this.emitter.emitType(n.indexer.value),this.emitter.result.none()}arrayDeclarationContext(n,e,s){return{}}arrayDeclarationReferenceContext(n,e,s){return{}}arrayLiteral(n,e){return this.emitter.result.none()}arrayLiteralContext(n,e){return{}}arrayLiteralReferenceContext(n,e){return{}}scalarDeclaration(n,e){if(n.baseScalar)this.emitter.emitType(n.baseScalar);return this.emitter.result.none()}scalarDeclarationContext(n,e){return{}}scalarDeclarationReferenceContext(n,e){return{}}scalarInstantiation(n,e){return this.emitter.result.none()}scalarInstantiationContext(n,e){return{}}intrinsic(n,e){return this.emitter.result.none()}intrinsicContext(n,e){return{}}booleanLiteralContext(n){return{}}booleanLiteral(n){return this.emitter.result.none()}stringTemplateContext(n){return{}}stringTemplate(n){return this.emitter.result.none()}stringLiteralContext(n){return{}}stringLiteral(n){return this.emitter.result.none()}numericLiteralContext(n){return{}}numericLiteral(n){return this.emitter.result.none()}operationDeclaration(n,e){return this.emitter.emitOperationParameters(n),this.emitter.emitOperationReturnType(n),this.emitter.result.none()}operationDeclarationContext(n,e){return{}}operationDeclarationReferenceContext(n,e){return{}}interfaceDeclarationOperationsContext(n){return{}}interfaceDeclarationOperationsReferenceContext(n){return{}}interfaceOperationDeclarationContext(n,e){return{}}interfaceOperationDeclarationReferenceContext(n,e){return{}}operationParameters(n,e){return this.emitter.result.none()}operationParametersContext(n,e){return{}}operationParametersReferenceContext(n,e){return{}}operationReturnType(n,e){return this.emitter.result.none()}operationReturnTypeContext(n,e){return{}}operationReturnTypeReferenceContext(n,e){return{}}interfaceDeclaration(n,e){return this.emitter.emitInterfaceOperations(n),this.emitter.result.none()}interfaceDeclarationContext(n,e){return{}}interfaceDeclarationReferenceContext(n,e){return{}}interfaceDeclarationOperations(n){for(let e of n.operations.values())this.emitter.emitInterfaceOperation(e);return this.emitter.result.none()}interfaceOperationDeclaration(n,e){return this.emitter.emitOperationParameters(n),this.emitter.emitOperationReturnType(n),this.emitter.result.none()}enumDeclaration(n,e){return this.emitter.emitEnumMembers(n),this.emitter.result.none()}enumDeclarationContext(n,e){return{}}enumDeclarationReferenceContext(n,e){return{}}enumMembers(n){for(let e of n.members.values())this.emitter.emitType(e);return this.emitter.result.none()}enumMembersContext(n){return{}}enumMember(n){return this.emitter.result.none()}enumMemberContext(n){return{}}unionDeclaration(n,e){return this.emitter.emitUnionVariants(n),this.emitter.result.none()}unionDeclarationContext(n){return{}}unionDeclarationReferenceContext(n){return{}}unionInstantiation(n,e){return this.emitter.emitUnionVariants(n),this.emitter.result.none()}unionInstantiationContext(n,e){return{}}unionInstantiationReferenceContext(n,e){return{}}unionLiteral(n){return this.emitter.emitUnionVariants(n),this.emitter.result.none()}unionLiteralContext(n){return{}}unionLiteralReferenceContext(n){return{}}unionVariants(n){for(let e of n.variants.values())this.emitter.emitType(e);return this.emitter.result.none()}unionVariantsContext(){return{}}unionVariantsReferenceContext(){return{}}unionVariant(n){return this.emitter.emitTypeReference(n.type),this.emitter.result.none()}unionVariantContext(n){return{}}unionVariantReferenceContext(n){return{}}tupleLiteral(n){return this.emitter.emitTupleLiteralValues(n),this.emitter.result.none()}tupleLiteralContext(n){return{}}tupleLiteralValues(n){for(let e of n.values.values())this.emitter.emitType(e);return this.emitter.result.none()}tupleLiteralValuesContext(n){return{}}tupleLiteralValuesReferenceContext(n){return{}}tupleLiteralReferenceContext(n){return{}}sourceFile(n){let e={path:n.path,contents:""};for(let s of n.globalScope.declarations)e.contents+=s.value+`
`;return e}async writeOutput(n){for(let e of n){let s=await this.emitter.emitSourceFile(e);await ds(this.emitter.getProgram(),{path:s.path,content:s.contents})}}reference(n,e,s,t){return this.emitter.result.none()}circularReference(n,e,s){if(!s.containsDeclaration)throw Error(`Circular references to non-declarations are not supported by this emitter. Cycle:
${s}`);if(n.kind!=="declaration")return n;U(e,"Emit context must have a scope set in order to create references to declarations.");let{pathUp:t,pathDown:i,commonScope:h}=$t(n,e);return this.reference(n,t,i,h)}declarationName(n){if(U(n.name!==void 0,"Can't emit a declaration that doesn't have a name."),n.kind==="Enum"||n.kind==="Intrinsic")return n.name;if(n.kind==="Operation"&&n.interface)return n.name;if(!n.templateMapper)return n.name;let e=!1,s=n.templateMapper.args.map((t)=>{if(t.entityKind==="Indeterminate")t=t.type;if(!("kind"in t))return;switch(t.kind){case"Model":case"Scalar":case"Interface":case"Operation":case"Enum":case"Union":case"Intrinsic":if(!t.name){e=!0;return}let i=this.emitter.emitDeclarationName(t);if(i===void 0){e=!0;return}return i[0].toUpperCase()+i.slice(1);default:e=!0;return}});if(e)return;return n.name+s.join("")}}class dn{}class fs extends dn{name;scope;value;kind="declaration";meta={};constructor(n,e,s){if(s instanceof S)s.onValue((t)=>this.value=t);super();this.name=n,this.scope=e,this.value=s}}class _t extends dn{value;kind="code";constructor(n){if(n instanceof S)n.onValue((e)=>this.value=e);super();this.value=n}}class Ft extends dn{kind="none"}class Ht extends dn{emitEntityKey;kind="circular";constructor(n){super();this.emitEntityKey=n}}function Zh(n,e,s){let t=[],i={noEmit:n.compilerOptions.dryRun??!1,emitterOutputDir:s.emitterOutputDir,...s.options},h=Dn.objectKeyer(),d=Dn.objectKeyer(),f=Dn.objectKeyer(),l=new Dn(([w,v,x])=>{return`${w}-${h.getKey(v)}-${d.getKey(x)}`}),u=new Dn(([w,v,x])=>{return`${w}-${h.getKey(v)}-${d.getKey(x)}`}),a=new Dn(([w,v])=>{return`${f.getKey(w)}-${d.getKey(v)}`}),c=[],b=[],r={lexicalContext:{},referenceContext:{}},m=null,I=null,Q=null,g=qf(),sn=qf(),tn={getContext(){return{...r.lexicalContext,...r.referenceContext}},getOptions(){return i},getProgram(){return n},result:{declaration(w,v){let x=Qe();return U(x,"Emit context must have a scope set in order to create declarations. Consider setting scope to a new source file's global scope in the `programContext` method of `TypeEmitter`."),new fs(w,x,v)},rawCode(w){return new _t(w)},none(){return new Ft}},createScope(w,v,x=null){let G;if(!x)G={kind:"sourceFile",name:v,sourceFile:w,parentScope:x,childScopes:[],declarations:[]};else G={kind:"namespace",name:v,namespace:w,childScopes:[],declarations:[],parentScope:x};return x?.childScopes.push(G),G},createSourceFile(w){let v=i.emitterOutputDir,x={globalScope:void 0,path:Nt(v,Mo(w)),imports:new Map,meta:{}};return x.globalScope=this.createScope(x,""),t.push(x),x},emitTypeReference(w,v){return _(v?.referenceContext,()=>{let x=I,G=Q;I=r.referenceContext??null,Q=I?w:null;let $;if(w.kind==="ModelProperty")$=k("modelPropertyReference",w);else if(w.kind==="EnumMember")$=k("enumMemberReference",w);if($)return I=x,Q=G,$;let J=this.emitType(w);I=x,Q=G;let fn=null;if(J.kind==="circular"){let P=u.get(J.emitEntityKey);if(!P)P=[],u.set(J.emitEntityKey,P);let on=b;return P.push({state:{lexicalTypeStack:c,context:r},cb:(Xn)=>Ln(this,Xn,!0,Co(on,J,l))}),fn=new S,this.result.rawCode(fn)}else return Ln(this,J,!1);function Ln(P,on,Xn,ws){let xn,St=Qe();if(Xn)xn=E.circularReference(on,St,ws);else{if(on.kind!=="declaration")return on;U(St,"Emit context must have a scope set in order to create references to declarations.");let{pathUp:su,pathDown:tu,commonScope:iu}=$t(on,St);xn=E.reference(on,su,tu,iu)}if(!(xn instanceof dn))xn=P.result.rawCode(xn);if(fn)switch(U(xn.kind!=="circular","TypeEmitter `reference` returned circular emit"),U(xn.kind==="none"||!(xn.value instanceof S),"TypeEmitter's `reference` method cannot return a placeholder."),xn.kind){case"code":case"declaration":fn.setValue(xn.value);break;case"none":fn.setValue("");break}return xn}})},emitDeclarationName(w){return E.declarationName(w)},async writeOutput(){return E.writeOutput(t)},getSourceFiles(){return t},emitType(w,v){if(v?.referenceContext)I=v?.referenceContext??I,Q=w??Q;let x=Lf(w)&&w.kind!=="Namespace"?E.declarationName(w):null,G=Pn(w),$;switch(G){case"scalarDeclaration":case"scalarInstantiation":case"modelDeclaration":case"modelInstantiation":case"operationDeclaration":case"interfaceDeclaration":case"interfaceOperationDeclaration":case"enumDeclaration":case"unionDeclaration":case"unionInstantiation":$=[x];break;case"arrayDeclaration":let fn=w.indexer.value;$=[x,fn];break;case"arrayLiteral":$=[w.indexer.value];break;case"intrinsic":$=[x];break;default:$=[]}return k(G,w,...$)},emitProgram(w){let v=n.getGlobalNamespaceType();if(w?.emitGlobalNamespace){this.emitType(v);return}for(let x of v.namespaces.values()){if(x.name==="TypeSpec"&&!w?.emitTypeSpecNamespace)continue;this.emitType(x)}for(let x of v.models.values())if(!H(x))this.emitType(x);for(let x of v.operations.values())if(!H(x))this.emitType(x);for(let x of v.enums.values())this.emitType(x);for(let x of v.unions.values())if(!H(x))this.emitType(x);for(let x of v.interfaces.values())if(!H(x))this.emitType(x);for(let x of v.scalars.values())this.emitType(x)},emitModelProperties(w){let v=k("modelProperties",w);if(v instanceof dn)return v;else return this.result.rawCode(v)},emitModelProperty(w){return k("modelPropertyLiteral",w)},emitOperationParameters(w){return k("operationParameters",w,w.parameters)},emitOperationReturnType(w){return k("operationReturnType",w,w.returnType)},emitInterfaceOperations(w){return k("interfaceDeclarationOperations",w)},emitInterfaceOperation(w){let v=E.declarationName(w);if(v===void 0)U(!1,"Unnamed operations are not supported");return k("interfaceOperationDeclaration",w,v)},emitEnumMembers(w){return k("enumMembers",w)},emitUnionVariants(w){return k("unionVariants",w)},emitTupleLiteralValues(w){return k("tupleLiteralValues",w)},async emitSourceFile(w){return await E.sourceFile(w)}},E=new e(tn);return tn;function k(w,...v){let x=v[0],G,$,J=!1;if(Z(w,v,()=>{$=[w,x,r];let P=l.get($);if(P){G=P,J=!0;return}l.set($,new Ht($)),U(E[w],`TypeEmitter doesn't have a method named ${w}.`),G=Ln(E[w](...v))}),J)return G;if(G instanceof S)return G.onValue((P)=>fn(P)),G;return fn(G),G;function fn(P){l.set($,P);let on=u.get($);if(on){for(let Xn of on)B(Xn.state,()=>{Xn.cb(P)});u.set($,[])}if(P.kind==="declaration")P.scope.declarations.push(P)}function Ln(P){if(P instanceof dn)return P;return tn.result.rawCode(P)}}function z(w){return w==="interfaceDeclarationOperations"||w==="interfaceOperationDeclaration"||w==="operationParameters"||w==="operationReturnType"||w==="modelProperties"||w==="enumMembers"||w==="tupleLiteralValues"||w==="unionVariants"}function nn(w,v){let x=v[0],G,$=(w==="modelInstantiation"||w==="unionInstantiation")&&v[1]===void 0;if(Lf(x)&&x.kind!=="Intrinsic"&&!z(w)&&!$){G=[sn.intern({method:w,args:sn.intern(v)})];let J=x.namespace;while(J){if(J.name==="")break;G.unshift(sn.intern({method:"namespace",args:sn.intern([J])})),J=J.namespace}}else G=[...c,sn.intern({method:w,args:sn.intern(v)})];if(c=G,!m)m=g.intern({lexicalContext:E.programContext(n),referenceContext:g.intern({})});r=m;for(let J of c){if(I&&J.args[0]===Q)r=g.intern({lexicalContext:r.lexicalContext,referenceContext:g.intern({...r.referenceContext,...I})});let fn=a.get([J,r]);if(fn){r=fn;continue}let Ln=J.method+"Context",P=J.method+"ReferenceContext";if(Wf(J.method))U(E[Ln],`TypeEmitter doesn't have a method named ${Ln}`);if(Gf(J.method))U(E[P],`TypeEmitter doesn't have a method named ${P}`);let on=Wf(J.method)?E[Ln](...J.args):{},Xn=Gf(J.method)?E[P](...J.args):{},ws=g.intern({lexicalContext:g.intern({...r.lexicalContext,...on}),referenceContext:g.intern({...r.referenceContext,...Xn})});a.set([J,r],ws),r=ws}if(!z(w))b=[...b,sn.intern({method:w,type:x,context:r})]}function Z(w,v,x){let G=r,$=c,J=b;nn(w,v),x(),r=G,c=$,b=J}function _(w,v){if(w!==void 0){let x=r;r=g.intern({lexicalContext:r.lexicalContext,referenceContext:g.intern({...r.referenceContext,...w})});let G=v();return r=x,G}else return v()}function B(w,v){let x=r,G=c;r=w.context,c=w.lexicalTypeStack,v(),r=x,c=G}function Pn(w){switch(w.kind){case"Model":if(D(n).array.is(w)&&w.name==="Array")return"arrayLiteral";if(w.name==="")return"modelLiteral";if(w.templateMapper)return"modelInstantiation";if(w.indexer&&w.indexer.key.name==="integer")return"arrayDeclaration";return"modelDeclaration";case"Namespace":return"namespace";case"ModelProperty":return"modelPropertyLiteral";case"StringTemplate":return"stringTemplate";case"Boolean":return"booleanLiteral";case"String":return"stringLiteral";case"Number":return"numericLiteral";case"Operation":if(w.interface)return"interfaceOperationDeclaration";else return"operationDeclaration";case"Interface":return"interfaceDeclaration";case"Enum":return"enumDeclaration";case"EnumMember":return"enumMember";case"Union":if(!w.name)return"unionLiteral";if(w.templateMapper)return"unionInstantiation";return"unionDeclaration";case"UnionVariant":return"unionVariant";case"Tuple":return"tupleLiteral";case"Scalar":if(w.templateMapper)return"scalarInstantiation";else return"scalarDeclaration";case"Intrinsic":return"intrinsic";default:U(!1,`Encountered type ${w.kind} which we don't know how to emit.`)}}function Qe(){return r.referenceContext?.scope??r.lexicalContext?.scope??null}}function Lf(n){switch(n.kind){case"Namespace":case"Interface":case"Enum":case"Operation":case"Scalar":case"Intrinsic":return!0;case"Model":return n.name?n.name!==""&&n.name!=="Array":!1;case"Union":return n.name?n.name!=="":!1;default:return!1}}function qf(){let n={},e=new Map;function s(t){if(t===null||typeof t!=="object")return t;let i=Object.keys(t);if(i.length===0)return n;let h=e.get(i.length);if(!h)h=new Map,e.set(i.length,h);let d=i.sort(),f=h;for(let c of d){if(!f.has(c))f.set(c,new Map);f=f.get(c)}let l=f.valueNode;if(!l)l=new Map,f.valueNode=l;let u=d.map((c)=>t[c]),a=l;for(let c=0;c<u.length;c++){let b=u[c],r=b&&typeof b==="object",m;if(r){if(!a.has("obj"))a.set("obj",new WeakMap);if(m=a.get("obj"),!m.has(b))m.set(b,new Map);m=m.get(b)}else{if(!a.has("prim"))a.set("prim",new Map);if(m=a.get("prim"),!m.has(b))m.set(b,new Map);m=m.get(b)}a=m}if(a.has("interned"))return a.get("interned");return a.set("interned",t),t}return{intern:s}}var zf=new Set(["modelPropertyReference","enumMemberReference"]);function Wf(n){return!zf.has(n)}var So=new Set([...zf,"booleanLiteral","stringTemplate","stringLiteral","numericLiteral","scalarInstantiation","enumMember","enumMembers","intrinsic"]);function Gf(n){return!So.has(n)}function Co(n,e,s){for(let t=n.length-1;t>=0;t--)if(n[t].type===e.emitEntityKey[1])return new hs(n.slice(t).map((i)=>{return{type:i.type,entity:s.get([i.method,i.type,i.context])}}));throw Error(`Couldn't resolve the circular reference stack for ${A(e.emitEntityKey[1])}`)}function Mo(n){return n.split(/[/\\]/).filter((e)=>e!==""&&e!=="."&&e!=="..").map(jt).join("/")}class Sn extends Array{#n(n,e){for(let[s,t]of this.entries())if(t===n)this[s]=e}push(...n){for(let e of n){let s;if(e instanceof dn)if(U(e.kind!=="circular","Can't push a circular emit result."),e.kind==="none")s=void 0;else s=e.value;else s=e;if(s instanceof S)s.onValue((t)=>this.#n(s,t));super.push(s)}return n.length}}var Ge=Symbol("placeholder"),us=Symbol("ObjectBuilder.set");class p{static SET=us;[Ge];constructor(n={}){let e=(t)=>{for(let[i,h]of Object.entries(t))this[us](i,h)},s=(t)=>{t.onValue(e)};if(n instanceof p){if(n[Ge])this[Ge]=n[Ge],s(n[Ge]);e(n)}else if(n instanceof S)this[Ge]=n,s(n);else e(n)}set(n,e){this[us](n,e)}[us](n,e){let s=e;if(e instanceof dn)if(U(e.kind!=="circular","Can't set a circular emit result."),e.kind==="none"){this[n]=void 0;return}else s=e.value;if(s instanceof S)s.onValue((t)=>{this[n]=t});this[n]=s}}function M(n,e,s){n[us](e,s)}class as extends ls{#n=new ke;#e=new Map;#i=new Map;#s(n,e){if(e.indexer){M(n,"unevaluatedProperties",this.emitter.emitTypeReference(e.indexer.value));return}if(!this.emitter.getOptions()["seal-object-schemas"])return;if(!e.derivedModels.filter(ii).length)M(n,"unevaluatedProperties",{not:{}})}modelDeclaration(n,e){let s=En(this.emitter.getProgram(),n),t=this.emitter.getOptions()["polymorphic-models-strategy"];if((t==="oneOf"||t==="anyOf")&&s&&n.derivedModels.length>0)return this.#L(n,e,s,t);let i=n.baseModel&&this.#m(n.baseModel),h=this.#l(n,e,{type:"object",properties:i?this.#v(n):this.emitter.emitModelProperties(n),required:i?this.#E(n):this.#u(n)});if(n.baseModel&&!i){let d=new Sn;d.push(this.emitter.emitTypeReference(n.baseModel)),M(h,"allOf",d)}return this.#s(h,n),this.#t(n,h),this.#d(n,e,h)}#m(n){let e=En(this.emitter.getProgram(),n),s=this.emitter.getOptions()["polymorphic-models-strategy"];return(s==="oneOf"||s==="anyOf")&&!!e&&n.derivedModels.length>0}modelLiteral(n){let e=new p({type:"object",properties:this.emitter.emitModelProperties(n),required:this.#u(n)});return this.#s(e,n),e}modelInstantiation(n,e){if(!e)return this.modelLiteral(n);return this.modelDeclaration(n,e)}arrayDeclaration(n,e,s){let t=this.#l(n,e,{type:"array",items:this.emitter.emitTypeReference(s)});return this.#t(n,t),this.#d(n,e,t)}arrayLiteral(n,e){return new p({type:"array",items:this.emitter.emitTypeReference(e)})}#u(n){let e=[];for(let t of n.properties.values())if(!t.optional)e.push(t.name);let s=En(this.emitter.getProgram(),n);if(s&&!n.properties.has(s.propertyName))e.push(s.propertyName);return e.length>0?e:void 0}#E(n){let e=[],s=new Set,t=(i)=>{if(s.has(i))return;if(s.add(i),i.baseModel)t(i.baseModel);for(let d of i.properties.values())if(!d.optional&&!e.includes(d.name))e.push(d.name);let h=En(this.emitter.getProgram(),i);if(h&&!i.properties.has(h.propertyName)&&!e.includes(h.propertyName))e.push(h.propertyName)};return t(n),e.length>0?e:void 0}#v(n){let e=new p,s=new Set,t=(i)=>{if(s.has(i))return;if(s.add(i),i.baseModel)t(i.baseModel);for(let[d,f]of i.properties){let l=this.emitter.emitModelProperty(f);M(e,d,l)}let h=En(this.emitter.getProgram(),i);if(h&&!(h.propertyName in e))M(e,h.propertyName,{type:"string",description:`Discriminator property for ${i.name}.`})};return t(n),e}modelProperties(n){let e=new p;for(let[t,i]of n.properties){let h=this.emitter.emitModelProperty(i);M(e,t,h)}let s=En(this.emitter.getProgram(),n);if(s&&!(s.propertyName in e))M(e,s.propertyName,{type:"string",description:`Discriminator property for ${n.name}.`});return e}modelPropertyLiteral(n){let e=this.emitter.emitTypeReference(n.type);U(e.kind==="code","Unexpected non-code result from emit reference");let s=new p(e.value);if(n.defaultValue)s.default=this.#T(n,n.defaultValue);if(s.anyOf&&oe(this.emitter.getProgram(),n))s.oneOf=s.anyOf,delete s.anyOf;return this.#t(n,s),s}#T(n,e){return Mn(this.emitter.getProgram(),e,n)}booleanLiteral(n){return{type:"boolean",const:n.value}}stringLiteral(n){return{type:"string",const:n.value}}stringTemplate(n){if(n.stringValue!==void 0)return{type:"string",const:n.stringValue};let e=Df(n);return this.emitter.getProgram().reportDiagnostics(e.map((s)=>({...s,severity:"warning"}))),{type:"string"}}numericLiteral(n){return{type:"number",const:n.value}}enumDeclaration(n,e){let s=new Set,t=new Set;for(let d of n.members.values())s.add(typeof d.value==="number"?"number":"string"),t.add(d.value??d.name);let i=[...s],h=this.#l(n,e,{type:i.length===1?i[0]:i,enum:[...t]});return this.#t(n,h),this.#d(n,e,h)}enumMemberReference(n){switch(typeof n.value){case"undefined":return{type:"string",const:n.name};case"string":return{type:"string",const:n.value};case"number":return{type:"number",const:n.value}}}tupleLiteral(n){return new p({type:"array",prefixItems:this.emitter.emitTupleLiteralValues(n)})}tupleLiteralValues(n){let e=new Sn;for(let s of n.values.values())e.push(this.emitter.emitType(s));return e}unionInstantiation(n,e){if(!e)return this.unionLiteral(n);return this.unionDeclaration(n,e)}unionDeclaration(n,e){let s=oe(this.emitter.getProgram(),n)?"oneOf":"anyOf",t=this.#l(n,e,{[s]:this.emitter.emitUnionVariants(n)});return this.#t(n,t),this.#d(n,e,t)}unionLiteral(n){let e=oe(this.emitter.getProgram(),n)?"oneOf":"anyOf";return new p({[e]:this.emitter.emitUnionVariants(n)})}unionVariants(n){let e=new Sn;for(let s of n.variants.values())e.push(this.emitter.emitType(s));return e}unionVariant(n){let e=this.emitter.emitTypeReference(n.type);U(e.kind==="code","Unexpected non-code result from emit reference");let s=new p(e.value);return this.#t(n,s),s}modelPropertyReference(n){let e=this.emitter.emitTypeReference(n.type);U(e.kind==="code","Unexpected non-code result from emit reference");let s=new p(e.value);return this.#t(n,s),s}reference(n,e,s,t){if(n.value instanceof S)throw Error("Can't form reference to declaration that hasn't been created yet");let i=e[e.length-1],h=s[0];if(h&&i&&!h.sourceFile.meta.shouldEmit)i.sourceFile.meta.bundledRefs.push(n);if(n.value.$id)return{$ref:n.value.$id};if(!t)if(h&&!h.sourceFile.meta.shouldEmit)return{$ref:"#/$defs/"+n.name};else return{$ref:$h(Qf(i.sourceFile.path),h.sourceFile.path,!1)};if(!i&&!h)return{$ref:"#/$defs/"+n.name};throw Error("JSON Pointer refs to arbitrary schemas is not supported")}scalarInstantiation(n,e){if(!e)return this.#a(n);return this.scalarDeclaration(n,e)}scalarInstantiationContext(n,e){if(e===void 0)return{};else return this.#h(n)}scalarDeclaration(n,e){let s=this.#f(n),t=this.#a(n);if(s)return t;let i=this.#l(n,e,t);return this.#d(n,e,i)}#a(n){let e,s=this.#f(n);if(s)e=this.#k(n);else if(n.baseScalar)e=this.#a(n.baseScalar);else return xs(this.emitter.getProgram(),{code:"unknown-scalar",format:{name:n.name},target:n}),{};let t=new p(e);if(this.#t(n,t),s)delete t.description;return t}#k(n){switch(n.name){case"uint8":return{type:"integer",minimum:0,maximum:255};case"uint16":return{type:"integer",minimum:0,maximum:65535};case"uint32":return{type:"integer",minimum:0,maximum:4294967295};case"int8":return{type:"integer",minimum:-128,maximum:127};case"int16":return{type:"integer",minimum:-32768,maximum:32767};case"int32":case"unixTimestamp32":return{type:"integer",minimum:-2147483648,maximum:2147483647};case"int64":if((this.emitter.getOptions()["int64-strategy"]??"string")==="string")return{type:"string"};else return{type:"integer"};case"uint64":if((this.emitter.getOptions()["int64-strategy"]??"string")==="string")return{type:"string"};else return{type:"integer"};case"decimal":case"decimal128":return{type:"string"};case"integer":return{type:"integer"};case"safeint":return{type:"integer"};case"float":return{type:"number"};case"float32":return{type:"number"};case"float64":return{type:"number"};case"numeric":return{type:"number"};case"string":return{type:"string"};case"boolean":return{type:"boolean"};case"plainDate":return{type:"string",format:"date"};case"plainTime":return{type:"string",format:"time"};case"offsetDateTime":case"utcDateTime":return{type:"string",format:"date-time"};case"duration":return{type:"string",format:"duration"};case"url":return{type:"string",format:"uri"};case"bytes":return{type:"string",contentEncoding:"base64"};default:return xs(this.emitter.getProgram(),{code:"unknown-scalar",format:{name:n.name},target:n}),{}}}#U(n,e){let s=this.emitter.getProgram(),t=jf(s,n);if(t.length>0)M(e,"examples",t.map((i)=>Mn(s,i.value,n)))}#t(n,e){let s=(d,f)=>{let l=d(this.emitter.getProgram(),n);if(l!==void 0)e[f]=l},t=(d,f)=>{let l=d(this.emitter.getProgram(),n);if(l){let u=this.emitter.emitTypeReference(l);U(u.kind==="code","Unexpected non-code result from emit reference"),M(e,f,u.value)}};if(n.kind!=="UnionVariant")this.#U(n,e);if(s(Xf,"minLength"),s(Zf,"maxLength"),s(nt,"minimum"),s(Of,"exclusiveMinimum"),s(et,"maximum"),s(Pf,"exclusiveMaximum"),s(Hf,"pattern"),s($f,"minItems"),s(Yf,"maxItems"),!this.#f(n)||n.name!=="url")s(Ff,"format");s(Us,"multipleOf"),t(As,"contains"),s(qs,"minContains"),s(Gs,"maxContains"),s(Js,"uniqueItems"),s(Os,"minProperties"),s(Xs,"maxProperties"),s($s,"contentEncoding"),s(_s,"contentMediaType"),t(Hs,"contentSchema"),s(Un,"description"),s(Oh,"title"),s((d,f)=>Nf(d,f)!==void 0?!0:void 0,"deprecated");let i=js(this.emitter.getProgram(),n);if(i){let d=new Sn;for(let f of i.values)d.push(this.emitter.emitTypeReference(f));M(e,"prefixItems",d)}let h=Cs(this.emitter.getProgram(),n);for(let{key:d,value:f}of h)if(this.#I(f))M(e,d,this.emitter.emitTypeReference(f));else M(e,d,f)}#I(n){return typeof n==="object"&&n!==null&&mn(n)}#d(n,e,s){let t=this.emitter.result.declaration(e,s),i=t.scope.sourceFile;i.meta.shouldEmit=this.#w(n);let h=Ye(this.emitter.getProgram(),n);if(h)this.#i.set(t,h);return t}#l(n,e,s){let t=this.#w(n)?this.#A(n,e):{};return new p({...t,...s})}#A(n,e){return{$schema:"https://json-schema.org/draft/2020-12/schema",$id:this.#Q(n,e)}}#w(n){return this.emitter.getOptions().emitAllRefs||this.emitter.getOptions().emitAllModels||$e(this.emitter.getProgram(),n)}#f(n){return this.emitter.getProgram().checker.isStdType(n)}#L(n,e,s,t){let i=new Sn,h=[];for(let f of n.derivedModels){if(!ii(f))continue;let l=this.emitter.emitTypeReference(f);i.push(l);let u=this.#W(f,s.propertyName);h.push(...u)}if(this.#q(n,s.propertyName)){let f=this.#G(n,s.propertyName,h);i.push(f)}let d=this.#l(n,e,{type:"object",properties:this.emitter.emitModelProperties(n),required:this.#u(n),[t]:i});return this.#t(n,d),this.#d(n,e,d)}#q(n,e){let s=n.properties.get(e);if(!s)return!1;return this.#b(s.type)}#b(n){let e=this.emitter.getProgram();switch(n.kind){case"Scalar":return _f(e,n);case"Union":for(let s of n.variants.values())if(this.#b(s.type))return!0;return!1;default:return!1}}#W(n,e){let s=n.properties.get(e);if(!s)return[];return this.#c(s.type)}#c(n){switch(n.kind){case"String":return[n.value];case"Union":return[...n.variants.values()].flatMap((e)=>this.#c(e.type));case"UnionVariant":return this.#c(n.type);case"EnumMember":return typeof n.value!=="number"?[n.value??n.name]:[];default:return[]}}#G(n,e,s){let t=new p;for(let[h,d]of n.properties)if(h===e)M(t,h,{type:"string",not:{enum:s}});else{let f=this.emitter.emitModelProperty(d);M(t,h,f)}if(!n.properties.has(e))M(t,e,{type:"string",not:{enum:s}});let i=this.#u(n);return{type:"object",properties:t,...i&&{required:i}}}intrinsic(n,e){switch(n.name){case"null":return{type:"null"};case"unknown":return{};case"never":case"void":return{not:{}};case"ErrorType":return{};default:let s=n.name;U(!1,"Unreachable")}}#z(){for(let[n,e]of this.#n.entries())for(let s of e)xs(this.emitter.getProgram(),{code:"duplicate-id",format:{id:n},target:s})}async writeOutput(n){if(this.emitter.getProgram().compilerOptions.dryRun)return;this.#z();let e=[],s=this.emitter.getOptions().bundleId;if(s){let t={$schema:"https://json-schema.org/draft/2020-12/schema",$id:s,$defs:{}};for(let i of n)if(i.meta.shouldEmit){let h=i.globalScope.declarations[0];t.$defs[this.#x(h)]=this.#r(i)}await ds(this.emitter.getProgram(),{path:Nt(this.emitter.getOptions().emitterOutputDir,s),content:this.#R(t)})}else{for(let t of n){let i=await this.emitter.emitSourceFile(t);if(t.meta.shouldEmit)e.push(i)}for(let t of e)await ds(this.emitter.getProgram(),{path:t.path,content:t.contents})}}sourceFile(n){let e=this.#r(n);return{contents:this.#R(e),path:n.path}}#r(n){let e=n.globalScope.declarations;U(e.length===1,"Multiple decls in single schema per file mode");let s={...e[0].value},t=new Set;if(n.meta.bundledRefs.length>0){s.$defs={};let i=[...n.meta.bundledRefs];while(i.length>0){let h=i.shift();if(t.has(h))continue;t.add(h),s.$defs[this.#x(h)]=h.value;let d=h.scope.sourceFile;i.push(...d.meta.bundledRefs)}}return s}#R(n){if(this.emitter.getOptions()["file-type"]==="json")return JSON.stringify(n,null,4);else return Jf(n,{aliasDuplicateObjects:!1,lineWidth:0})}#J(){let n=this.emitter.getContext().scope;U(n,"Scope should exists");while(n&&n.kind!=="sourceFile")n=n.parentScope;return U(n,"Top level scope should be a source file"),n.sourceFile}#Q(n,e){let s=vs(this.emitter.getProgram(),n),t=Ye(this.emitter.getProgram(),n);if(t)return this.#o(f(t,s),n);let i=this.emitter.getOptions().emitterOutputDir,h=this.#J().path,d=$h(i,h,!1);if(s)return this.#o(new URL(d,s).href,n);else return this.#o(d,n);function f(l,u){if(u)return new URL(l,u).href;else return l}}#o(n,e){return this.#n.track(n,e),n}#x(n){return this.#i.get(n)??n.name}modelDeclarationContext(n,e){if(this.#f(n)&&n.name==="object")return{};return this.#h(n)}modelInstantiationContext(n,e){if(e===void 0)return{};else return this.#h(n)}arrayDeclarationContext(n){return this.#h(n)}enumDeclarationContext(n){return this.#h(n)}unionDeclarationContext(n){return this.#h(n)}scalarDeclarationContext(n){if(this.#f(n))return{};else return this.#h(n)}#h(n){let e=this.emitter.createSourceFile(`${jt(this.declarationName(n))}.${this.#O()}`);return e.meta.shouldEmit=!0,e.meta.bundledRefs=[],this.#e.set(e,n),{scope:e.globalScope}}#O(){return this.emitter.getOptions()["file-type"]==="json"?"json":"yaml"}}async function Sf(n){let e=Zh(n.program,as,n);if(e.getOptions().emitAllModels)e.emitProgram({emitTypeSpecNamespace:!1});else for(let s of Ts(n.program))e.emitType(s);await e.writeOutput()}var Bo="TypeSpec.JsonSchema";var Cf={"/compiler/lib/intrinsics.tsp":`import "../dist/src/lib/intrinsic/tsp-index.js";
import "./prototypes.tsp";

// This file contains all the intrinsic types of typespec. Everything here will always be loaded
namespace TypeSpec;

/**
 * Represent a byte array
 */
scalar bytes;

/**
 * A numeric type
 */
scalar numeric;

/**
 * A whole number. This represent any \`integer\` value possible.
 * It is commonly represented as \`BigInteger\` in some languages.
 */
scalar integer extends numeric;

/**
 * A number with decimal value
 */
scalar float extends numeric;

/**
 * A 64-bit integer. (\`-9,223,372,036,854,775,808\` to \`9,223,372,036,854,775,807\`)
 */
scalar int64 extends integer;

/**
 * A 32-bit integer. (\`-2,147,483,648\` to \`2,147,483,647\`)
 */
scalar int32 extends int64;

/**
 * A 16-bit integer. (\`-32,768\` to \`32,767\`)
 */
scalar int16 extends int32;

/**
 * A 8-bit integer. (\`-128\` to \`127\`)
 */
scalar int8 extends int16;

/**
 * A 64-bit unsigned integer (\`0\` to \`18,446,744,073,709,551,615\`)
 */
scalar uint64 extends integer;

/**
 * A 32-bit unsigned integer (\`0\` to \`4,294,967,295\`)
 */
scalar uint32 extends uint64;

/**
 * A 16-bit unsigned integer (\`0\` to \`65,535\`)
 */
scalar uint16 extends uint32;

/**
 * A 8-bit unsigned integer (\`0\` to \`255\`)
 */
scalar uint8 extends uint16;

/**
 * An integer that can be serialized to JSON (\`−9007199254740991 (−(2^53 − 1))\` to \`9007199254740991 (2^53 − 1)\` )
 */
scalar safeint extends int64;

/**
 * A 64 bit floating point number. (\`±5.0 × 10^−324\` to \`±1.7 × 10^308\`)
 */
scalar float64 extends float;

/**
 * A 32 bit floating point number. (\`±1.5 x 10^−45\` to \`±3.4 x 10^38\`)
 */
scalar float32 extends float64;

/**
 * A decimal number with any length and precision. This represent any \`decimal\` value possible.
 * It is commonly represented as \`BigDecimal\` in some languages.
 */
scalar decimal extends numeric;

/**
 * A 128-bit decimal number.
 */
scalar decimal128 extends decimal;

/**
 * A sequence of textual characters.
 */
scalar string;

/**
 * A date on a calendar without a time zone, e.g. "April 10th"
 */
scalar plainDate {
  /**
   * Create a plain date from an ISO 8601 string.
   * @example
   *
   * \`\`\`tsp
   * const date = plainDate.fromISO("2024-05-06");
   * \`\`\`
   */
  init fromISO(value: string);

  /**
   * Create a plain date representing the current date.
   * @example
   *
   * \`\`\`tsp
   * const date = plainDate.now();
   * \`\`\`
   */
  init now();
}

/**
 * A time on a clock without a time zone, e.g. "3:00 am"
 */
scalar plainTime {
  /**
   * Create a plain time from an ISO 8601 string.
   * @example
   *
   * \`\`\`tsp
   * const time = plainTime.fromISO("12:34");
   * \`\`\`
   */
  init fromISO(value: string);

  /**
   * Create a plain time representing the current time.
   * @example
   *
   * \`\`\`tsp
   * const time = plainTime.now();
   * \`\`\`
   */
  init now();
}

/**
 * An instant in coordinated universal time (UTC)"
 */
scalar utcDateTime {
  /**
   * Create a date from an ISO 8601 string.
   * @example
   *
   * \`\`\`tsp
   * const time = utcDateTime.fromISO("2024-05-06T12:20-12Z");
   * \`\`\`
   */
  init fromISO(value: string);

  /**
   * Create a date representing the current date and time in UTC.
   * @example
   *
   * \`\`\`tsp
   * const time = utcDateTime.now();
   * \`\`\`
   */
  init now();
}

/**
 * A date and time in a particular time zone, e.g. "April 10th at 3:00am in PST"
 */
scalar offsetDateTime {
  /**
   * Create a date from an ISO 8601 string.
   * @example
   *
   * \`\`\`tsp
   * const time = offsetDateTime.fromISO("2024-05-06T12:20-12-0700");
   * \`\`\`
   */
  init fromISO(value: string);

  /**
   * Create a date representing the current date and time with offset.
   * @example
   *
   * \`\`\`tsp
   * const time = offsetDateTime.now();
   * \`\`\`
   */
  init now();
}

/**
 * A duration/time period. e.g 5s, 10h
 */
scalar duration {
  /**
   * Create a duration from an ISO 8601 string.
   * @example
   *
   * \`\`\`tsp
   * const time = duration.fromISO("P1Y1D");
   * \`\`\`
   */
  init fromISO(value: string);
}

/**
 * Boolean with \`true\` and \`false\` values.
 */
scalar boolean;

/**
 * @dev Array model type, equivalent to \`Element[]\`
 * @template Element The type of the array elements
 */
@indexer(integer, Element)
model Array<Element> {}

/**
 * @dev Model with string properties where all the properties have type \`Property\`
 * @template Element The type of the properties
 */
@indexer(string, Element)
model Record<Element> {}
`,"/compiler/lib/prototypes.tsp":`namespace TypeSpec.Prototypes;

internal extern dec getter(target: unknown);

namespace Types {
  interface ModelProperty {
    @getter type(): unknown;
  }

  interface Operation {
    @getter returnType(): unknown;
    @getter parameters(): unknown;
  }

  interface Array<TElementType> {
    @getter elementType(): TElementType;
  }
}
`,"/compiler/lib/std/decorators.tsp":`import "../../dist/src/lib/tsp-index.js";

using TypeSpec.Reflection;

namespace TypeSpec;

/**
 * Typically a short, single-line description.
 * @param summary Summary string.
 *
 * @example
 * \`\`\`typespec
 * @summary("This is a pet")
 * model Pet {}
 * \`\`\`
 */
extern dec summary(target: unknown, summary: valueof string);

/**
 * Attach a documentation string. Content support CommonMark markdown formatting.
 * @param doc Documentation string
 * @param formatArgs Record with key value pair that can be interpolated in the doc.
 *
 * @example
 * \`\`\`typespec
 * @doc("Represent a Pet available in the PetStore")
 * model Pet {}
 * \`\`\`
 */
extern dec doc(target: unknown, doc: valueof string, formatArgs?: {});

/**
 * Attach a documentation string to describe the successful return types of an operation.
 * If an operation returns a union of success and errors it only describes the success. See \`@errorsDoc\` for error documentation.
 * @param doc Documentation string
 *
 * @example
 * \`\`\`typespec
 * @returnsDoc("Returns doc")
 * op get(): Pet | NotFound;
 * \`\`\`
 */
extern dec returnsDoc(target: Operation, doc: valueof string);

/**
 * Attach a documentation string to describe the error return types of an operation.
 * If an operation returns a union of success and errors it only describes the errors. See \`@returnsDoc\` for success documentation.
 * @param doc Documentation string
 *
 * @example
 * \`\`\`typespec
 * @errorsDoc("Errors doc")
 * op get(): Pet | NotFound;
 * \`\`\`
 */
extern dec errorsDoc(target: Operation, doc: valueof string);

/**
 * Service options.
 */
model ServiceOptions {
  /**
   * Title of the service.
   */
  title?: string;
}

/**
 * Mark this namespace as describing a service and configure service properties.
 * @param options Optional configuration for the service.
 *
 * @example
 * \`\`\`typespec
 * @service
 * namespace PetStore;
 * \`\`\`
 *
 * @example Setting service title
 * \`\`\`typespec
 * @service(#{title: "Pet store"})
 * namespace PetStore;
 * \`\`\`
 */
extern dec service(target: Namespace, options?: valueof ServiceOptions);

/**
 * Specify that this model is an error type. Operations return error types when the operation has failed.
 *
 * @example
 * \`\`\`typespec
 * @error
 * model PetStoreError {
 *   code: string;
 *   message: string;
 * }
 * \`\`\`
 */
extern dec error(target: Model);

/**
 * Applies a media type hint to a TypeSpec type. Emitters and libraries may choose to use this hint to determine how a
 * type should be serialized. For example, the \`@typespec/http\` library will use the media type hint of the response
 * body type as a default \`Content-Type\` if one is not explicitly specified in the operation.
 *
 * Media types (also known as MIME types) are defined by RFC 6838. The media type hint should be a valid media type
 * string as defined by the RFC, but the decorator does not enforce or validate this constraint.
 *
 * Notes: the applied media type is _only_ a hint. It may be overridden or not used at all. Media type hints are
 * inherited by subtypes. If a media type hint is applied to a model, it will be inherited by all other models that
 * \`extend\` it unless they delcare their own media type hint.
 *
 * @param mediaType The media type hint to apply to the target type.
 *
 * @example create a model that serializes as XML by default
 *
 * \`\`\`tsp
 * @mediaTypeHint("application/xml")
 * model Example {
 *   @visibility(Lifecycle.Read)
 *   id: string;
 *
 *   name: string;
 * }
 * \`\`\`
 */
extern dec mediaTypeHint(target: Model | Scalar | Enum | Union, mediaType: valueof string);

// Cannot apply this to the scalar itself. Needs to be applied here so that we don't crash nostdlib scenarios
@@mediaTypeHint(TypeSpec.bytes, "application/octet-stream");

// @@mediaTypeHint(TypeSpec.string "text/plain") -- This is hardcoded in the compiler to avoid circularity
// between the initialization of the string scalar and the \`valueof string\` required to call the
// \`mediaTypeHint\` decorator.

/**
 * Specify a known data format hint for this string type. For example \`uuid\`, \`uri\`, etc.
 * This differs from the \`@pattern\` decorator which is meant to specify a regular expression while \`@format\` accepts a known format name.
 * The format names are open ended and are left to emitter to interpret.
 *
 * @param format format name.
 *
 * @example
 * \`\`\`typespec
 * @format("uuid")
 * scalar uuid extends string;
 * \`\`\`
 */
extern dec format(target: string | ModelProperty, format: valueof string);

/**
 * Specify the the pattern this string should respect using simple regular expression syntax.
 * The following syntax is allowed: alternations (\`|\`), quantifiers (\`?\`, \`*\`, \`+\`, and \`{ }\`), wildcard (\`.\`), and grouping parentheses.
 * Advanced features like look-around, capture groups, and references are not supported.
 *
 * This decorator may optionally provide a custom validation _message_. Emitters may choose to use the message to provide
 * context when pattern validation fails. For the sake of consistency, the message should be a phrase that describes in
 * plain language what sort of content the pattern attempts to validate. For example, a complex regular expression that
 * validates a GUID string might have a message like "Must be a valid GUID."
 *
 * @param pattern Regular expression.
 * @param validationMessage Optional validation message that may provide context when validation fails.
 *
 * @example
 * \`\`\`typespec
 * @pattern("[a-z]+", "Must be a string consisting of only lower case letters and of at least one character.")
 * scalar LowerAlpha extends string;
 * \`\`\`
 */
extern dec pattern(
  target: string | bytes | ModelProperty,
  pattern: valueof string,
  validationMessage?: valueof string
);

/**
 * Specify the minimum length this string type should be.
 * @param value Minimum length
 *
 * @example
 * \`\`\`typespec
 * @minLength(2)
 * scalar Username extends string;
 * \`\`\`
 */
extern dec minLength(target: string | ModelProperty, value: valueof integer);

/**
 * Specify the maximum length this string type should be.
 * @param value Maximum length
 *
 * @example
 * \`\`\`typespec
 * @maxLength(20)
 * scalar Username extends string;
 * \`\`\`
 */
extern dec maxLength(target: string | ModelProperty, value: valueof integer);

/** Types that can have range limits */
alias RangeLimitableTypes =
  | numeric
  | utcDateTime
  | offsetDateTime
  | plainDate
  | plainTime
  | duration;

/**
 * Specify the minimum number of items this array should have.
 * @param value Minimum number
 *
 * @example
 * \`\`\`typespec
 * @minItems(1)
 * model Endpoints is string[];
 * \`\`\`
 */
extern dec minItems(target: unknown[] | ModelProperty, value: valueof integer);

/**
 * Specify the maximum number of items this array should have.
 * @param value Maximum number
 *
 * @example
 * \`\`\`typespec
 * @maxItems(5)
 * model Endpoints is string[];
 * \`\`\`
 */
extern dec maxItems(target: unknown[] | ModelProperty, value: valueof integer);

/**
 * Specify the minimum value this numeric type should be.
 * @param value Minimum value
 *
 * @example
 * \`\`\`typespec
 * @minValue(18)
 * scalar Age is int32;
 * \`\`\`
 */
extern dec minValue(
  target: RangeLimitableTypes | ModelProperty,
  value: valueof RangeLimitableTypes
);

/**
 * Specify the maximum value this numeric type should be.
 * @param value Maximum value
 *
 * @example
 * \`\`\`typespec
 * @maxValue(200)
 * scalar Age is int32;
 * \`\`\`
 */
extern dec maxValue(
  target: RangeLimitableTypes | ModelProperty,
  value: valueof RangeLimitableTypes
);

/**
 * Specify the minimum value this numeric type should be, exclusive of the given
 * value.
 * @param value Minimum value
 *
 * @example
 * \`\`\`typespec
 * @minValueExclusive(0)
 * scalar distance is float64;
 * \`\`\`
 */
extern dec minValueExclusive(
  target: RangeLimitableTypes | ModelProperty,
  value: valueof RangeLimitableTypes
);

/**
 * Specify the maximum value this numeric type should be, exclusive of the given
 * value.
 * @param value Maximum value
 *
 * @example
 * \`\`\`typespec
 * @maxValueExclusive(50)
 * scalar distance is float64;
 * \`\`\`
 */
extern dec maxValueExclusive(
  target: RangeLimitableTypes | ModelProperty,
  value: valueof RangeLimitableTypes
);

/**
 * Mark this value as a secret value that should be treated carefully to avoid exposure
 *
 * @example
 * \`\`\`typespec
 * @secret
 * scalar Password is string;
 * \`\`\`
 */
extern dec secret(target: Scalar | ModelProperty | Model | Union | Enum);

/**
 * Attaches a tag to an operation, interface, or namespace. Multiple \`@tag\` decorators can be specified to attach multiple tags to a TypeSpec element.
 * @param tag Tag value
 */
extern dec tag(target: Namespace | Interface | Operation, tag: valueof string);

/**
 * Specifies how a templated type should name their instances.
 * @param name name the template instance should take
 * @param formatArgs Model with key value used to interpolate the name
 *
 * @example
 * \`\`\`typespec
 * @friendlyName("{name}List", T)
 * model List<Item> {
 *   value: Item[];
 *   nextLink: string;
 * }
 * \`\`\`
 */
extern dec friendlyName(target: unknown, name: valueof string, formatArgs?: unknown);

/**
 * Mark a model property as the key to identify instances of that type
 * @param altName Name of the property. If not specified, the decorated property name is used.
 *
 * @example
 * \`\`\`typespec
 * model Pet {
 *   @key id: string;
 * }
 * \`\`\`
 */
extern dec key(target: ModelProperty, altName?: valueof string);

/**
 * Specify this operation is an overload of the given operation.
 * @param overloadbase Base operation that should be a union of all overloads
 *
 * @example
 * \`\`\`typespec
 * op upload(data: string | bytes, @header contentType: "text/plain" | "application/octet-stream"): void;
 * @overload(upload)
 * op uploadString(data: string, @header contentType: "text/plain" ): void;
 * @overload(upload)
 * op uploadBytes(data: bytes, @header contentType: "application/octet-stream"): void;
 * \`\`\`
 */
extern dec overload(target: Operation, overloadbase: Operation);

/**
 * Provide an alternative name for this type when serialized to the given mime type.
 * @param mimeType Mime type this should apply to. The mime type should be a known mime type as described here https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types without any suffix (e.g. \`+json\`)
 * @param name Alternative name
 *
 * @example
 *
 * \`\`\`typespec
 * model Certificate {
 *   @encodedName("application/json", "exp")
 *   @encodedName("application/xml", "expiry")
 *   expireAt: int32;
 * }
 * \`\`\`
 *
 * @example Invalid values
 *
 * \`\`\`typespec
 * @encodedName("application/merge-patch+json", "exp")
 *              ^ error cannot use subtype
 * \`\`\`
 */
extern dec encodedName(target: unknown, mimeType: valueof string, name: valueof string);

/**
 * Options for \`@discriminated\` decorator.
 */
model DiscriminatedOptions {
  /**
   * How is the discriminated union serialized.
   * @default object
   */
  envelope?: "object" | "none";

  /** Name of the discriminator property */
  discriminatorPropertyName?: string;

  /** Name of the property envelopping the data */
  envelopePropertyName?: string;
}

/**
 * Specify that this union is discriminated.
 * @param options Options to configure the serialization of the discriminated union.
 *
 * @example
 *
 * \`\`\`typespec
 * @discriminated
 * union Pet{ cat: Cat, dog: Dog }
 *
 * model Cat { name: string, meow: boolean }
 * model Dog { name: string, bark: boolean }
 * \`\`\`
 * Serialized as:
 * \`\`\`json
 * {
 *   "kind": "cat",
 *   "value": {
 *     "name": "Whiskers",
 *     "meow": true
 *   }
 * },
 * {
 *   "kind": "dog",
 *   "value": {
 *     "name": "Rex",
 *     "bark": false
 *   }
 * }
 * \`\`\`
 *
 * @example Custom property names
 *
 * \`\`\`typespec
 * @discriminated(#{discriminatorPropertyName: "dataKind", envelopePropertyName: "data"})
 * union Pet{ cat: Cat, dog: Dog }
 *
 * model Cat { name: string, meow: boolean }
 * model Dog { name: string, bark: boolean }
 * \`\`\`
 * Serialized as:
 * \`\`\`json
 * {
 *   "dataKind": "cat",
 *   "data": {
 *     "name": "Whiskers",
 *     "meow": true
 *   }
 * },
 * {
 *   "dataKind": "dog",
 *   "data": {
 *     "name": "Rex",
 *     "bark": false
 *   }
 * }
 * \`\`\`
 */
extern dec discriminated(target: Union, options?: valueof DiscriminatedOptions);

/**
 * Specify the property to be used to discriminate this type.
 * @param propertyName The property name to use for discrimination
 *
 * @example
 *
 * \`\`\`typespec
 * @discriminator("kind")
 * model Pet{ kind: string }
 *
 * model Cat extends Pet {kind: "cat", meow: boolean}
 * model Dog extends Pet  {kind: "dog", bark: boolean}
 * \`\`\`
 */
extern dec discriminator(target: Model, propertyName: valueof string);

/**
 * Known encoding to use on utcDateTime or offsetDateTime
 */
enum DateTimeKnownEncoding {
  /**
   * RFC 3339 standard. https://www.ietf.org/rfc/rfc3339.txt
   * Encode to string.
   */
  rfc3339: "rfc3339",

  /**
   * RFC 7231 standard. https://www.ietf.org/rfc/rfc7231.txt
   * Encode to string.
   */
  rfc7231: "rfc7231",

  /**
   * Encode a datetime to a unix timestamp.
   * Unix timestamps are represented as an integer number of seconds since the Unix epoch and usually encoded as an int32.
   */
  unixTimestamp: "unixTimestamp",
}

/**
 * Known encoding to use on duration
 */
enum DurationKnownEncoding {
  /**
   * ISO8601 duration
   */
  ISO8601: "ISO8601",

  /**
   * Encode to integer or float as seconds
   */
  seconds: "seconds",

  /**
   * Encode to integer or float as milliseconds
   */
  milliseconds: "milliseconds",
}

/**
 * Known encoding to use on bytes
 */
enum BytesKnownEncoding {
  /**
   * Encode to Base64
   */
  base64: "base64",

  /**
   * Encode to Base64 Url
   */
  base64url: "base64url",
}

/**
 * Encoding for serializing arrays
 */
enum ArrayEncoding {
  /**
   * Each value of the array is separated by a pipe character (|).
   * Values can only contain | if the underlying protocol supports encoding them.
   * - json -> error
   * - http -> %7C
   */
  pipeDelimited,

  /**
   * Each value of the array is separated by a space character.
   * Values can only contain spaces if the underlying protocol supports encoding them.
   * - json -> error
   * - http -> %20
   */
  spaceDelimited,

  /**
   * Each value of the array is separated by a comma (,).
   * Values can only contain commas if the underlying protocol supports encoding them.
   * - json -> error
   * - http -> %2C
   */
  commaDelimited,

  /**
   * Each value of the array is separated by a newline character (\\n).
   * Values can only contain newlines if the underlying protocol supports encoding them.
   * - json -> error
   * - http -> %0A
   */
  newlineDelimited,
}

/**
 * Specify how to encode the target type.
 * @param encodingOrEncodeAs Known name of an encoding or a scalar type to encode as(Only for numeric and boolean types to encode as string).
 * @param encodedAs What target type is this being encoded as. Default to string.
 *
 * @example offsetDateTime encoded with rfc7231
 *
 * \`\`\`tsp
 * @encode("rfc7231")
 * scalar myDateTime extends offsetDateTime;
 * \`\`\`
 *
 * @example utcDateTime encoded with unixTimestamp
 *
 * \`\`\`tsp
 * @encode("unixTimestamp", int32)
 * scalar myDateTime extends unixTimestamp;
 * \`\`\`
 *
 * @example encode numeric type to string
 *
 * \`\`\`tsp
 * model Pet {
 *   @encode(string) id: int64;
 * }
 * \`\`\`
 *
 * @example encode boolean type to string
 *
 * \`@encode(string)\` on boolean uses case-insensitive \`true\` / \`false\` values.
 *
 * \`\`\`tsp
 * model FeatureFlags {
 *   @encode(string) enabled: boolean;
 * }
 * \`\`\`
 */
extern dec encode(
  target: Scalar | ModelProperty,
  encodingOrEncodeAs: (valueof string | EnumMember) | Scalar,
  encodedAs?: Scalar
);

/** Options for example decorators */
model ExampleOptions {
  /** The title of the example */
  title?: string;

  /** Description of the example */
  description?: string;
}

/**
 * Provide an example value for a data type.
 *
 * @param example Example value.
 * @param options Optional metadata for the example.
 *
 * @example
 *
 * \`\`\`tsp
 * @example(#{name: "Fluffy", age: 2})
 * model Pet {
 *  name: string;
 *  age: int32;
 * }
 * \`\`\`
 */
extern dec example(
  target: Model | Enum | Scalar | Union | ModelProperty | UnionVariant,
  example: valueof unknown,
  options?: valueof ExampleOptions
);

/**
 * Operation example configuration.
 */
model OperationExample {
  /** Example request body. */
  parameters?: unknown;

  /** Example response body. */
  returnType?: unknown;
}

/**
 * Provide example values for an operation's parameters and corresponding return type.
 *
 * @param example Example value.
 * @param options Optional metadata for the example.
 *
 * @example
 *
 * \`\`\`tsp
 * @opExample(#{parameters: #{name: "Fluffy", age: 2}, returnType: #{name: "Fluffy", age: 2, id: "abc"})
 * op createPet(pet: Pet): Pet;
 * \`\`\`
 */
extern dec opExample(
  target: Operation,
  example: valueof OperationExample,
  options?: valueof ExampleOptions
);

/**
 * Returns the model with required properties removed.
 */
extern dec withOptionalProperties(target: Model);

/**
 * Returns the model with any default values removed.
 */
extern dec withoutDefaultValues(target: Model);

/**
 * Returns the model with the given properties omitted.
 * @param omit List of properties to omit
 */
extern dec withoutOmittedProperties(target: Model, omit: string | Union);

/**
 * Returns the model with only the given properties included.
 * @param pick List of properties to include
 */
extern dec withPickedProperties(target: Model, pick: string | Union);

//---------------------------------------------------------------------------
// Paging
//---------------------------------------------------------------------------

/**
 * Mark this operation as a \`list\` operation that returns a paginated list of items.
 */
extern dec list(target: Operation);

/**
 * Pagination property defining the number of items to skip.
 * @example
 * \`\`\`tsp
 * model Page<T> {
 *   @pageItems items: T[];
 * }
 * @list op listPets(@offset skip: int32, @pageSize pageSize: int8): Page<Pet>;
 * \`\`\`
 */
extern dec offset(target: ModelProperty);

/**
 * Pagination property defining the page index.
 *
 * @example
 * \`\`\`tsp
 * model Page<T> {
 *   @pageItems items: T[];
 * }
 * @list op listPets(@pageIndex page: int32, @pageSize pageSize: int8): Page<Pet>;
 * \`\`\`
 */
extern dec pageIndex(target: ModelProperty);

/**
 * Specify the pagination parameter that controls the maximum number of items to include in a page.
 *
 * @example
 * \`\`\`tsp
 * model Page<T> {
 *   @pageItems items: T[];
 * }
 * @list op listPets(@pageIndex page: int32, @pageSize pageSize: int8): Page<Pet>;
 * \`\`\`
 */
extern dec pageSize(target: ModelProperty);

/**
 * Specify the the property that contains the array of page items.
 *
 * @example
 * \`\`\`tsp
 * model Page<T> {
 *   @pageItems items: T[];
 * }
 * @list op listPets(@pageIndex page: int32, @pageSize pageSize: int8): Page<Pet>;
 * \`\`\`
 */
extern dec pageItems(target: ModelProperty);

/**
 * Pagination property defining the token to get to the next page.
 * It MUST be specified both on the request parameter and the response.
 *
 * @example
 * \`\`\`tsp
 * model Page<T> {
 *   @pageItems items: T[];
 *   @continuationToken continuationToken: string;
 * }
 * @list op listPets(@continuationToken continuationToken: string): Page<Pet>;
 * \`\`\`
 */
extern dec continuationToken(target: ModelProperty);

/**
 * Pagination property defining a link to the next page.
 *
 * It is expected that navigating to the link will return the same set of responses as the operation that returned the current page.
 *
 * @example
 * \`\`\`tsp
 * model Page<T> {
 *   @pageItems items: T[];
 *   @nextLink next: url;
 *   @prevLink prev: url;
 *   @firstLink first: url;
 *   @lastLink last: url;
 * }
 * @list op listPets(): Page<Pet>;
 * \`\`\`
 */
extern dec nextLink(target: ModelProperty);

/**
 * Pagination property defining a link to the previous page.
 *
 * It is expected that navigating to the link will return the same set of responses as the operation that returned the current page.
 *
 * @example
 * \`\`\`tsp
 * model Page<T> {
 *   @pageItems items: T[];
 *   @nextLink next: url;
 *   @prevLink prev: url;
 *   @firstLink first: url;
 *   @lastLink last: url;
 * }
 * @list op listPets(): Page<Pet>;
 * \`\`\`
 */
extern dec prevLink(target: ModelProperty);

/**
 * Pagination property defining a link to the first page.
 *
 * It is expected that navigating to the link will return the same set of responses as the operation that returned the current page.
 *
 * @example
 * \`\`\`tsp
 * model Page<T> {
 *   @pageItems items: T[];
 *   @nextLink next: url;
 *   @prevLink prev: url;
 *   @firstLink first: url;
 *   @lastLink last: url;
 * }
 * @list op listPets(): Page<Pet>;
 * \`\`\`
 */
extern dec firstLink(target: ModelProperty);

/**
 * Pagination property defining a link to the last page.
 *
 * It is expected that navigating to the link will return the same set of responses as the operation that returned the current page.
 *
 * @example
 * \`\`\`tsp
 * model Page<T> {
 *   @pageItems items: T[];
 *   @nextLink next: url;
 *   @prevLink prev: url;
 *   @firstLink first: url;
 *   @lastLink last: url;
 * }
 * @list op listPets(): Page<Pet>;
 * \`\`\`
 */
extern dec lastLink(target: ModelProperty);

//---------------------------------------------------------------------------
// Debugging
//---------------------------------------------------------------------------

/**
 * A debugging decorator used to inspect a type.
 * @param text Custom text to log
 */
extern dec inspectType(target: unknown, text: valueof string);

/**
 * A debugging decorator used to inspect a type name.
 * @param text Custom text to log
 */
extern dec inspectTypeName(target: unknown, text: valueof string);
`,"/compiler/lib/std/main.tsp":`// TypeSpec standard library. Everything in here can be omitted by using \`--nostdlib\` cli flag or \`nostdlib\` in the config.
import "./types.tsp";
import "./decorators.tsp";
import "./reflection.tsp";
import "./visibility.tsp";
`,"/compiler/lib/std/reflection.tsp":`namespace TypeSpec.Reflection;

model Enum {}
model EnumMember {}
model Interface {}
model Model {}
model ModelProperty {}
model Namespace {}
model Operation {}
model Scalar {}
model Union {}
model UnionVariant {}
model StringTemplate {}
`,"/compiler/lib/std/types.tsp":`namespace TypeSpec;

/**
 * Represent a 32-bit unix timestamp datetime with 1s of granularity.
 * It measures time by the number of seconds that have elapsed since 00:00:00 UTC on 1 January 1970.
 */
@encode("unixTimestamp", int32)
scalar unixTimestamp32 extends utcDateTime;

/**
 * Represent a URL string as described by https://url.spec.whatwg.org/
 */
scalar url extends string;

/**
 * Represents a collection of optional properties.
 *
 * @template Source An object whose spread properties are all optional.
 */
@doc("The template for adding optional properties.")
@withOptionalProperties
model OptionalProperties<Source> {
  ...Source;
}

/**
 * Represents a collection of updateable properties.
 *
 * @template Source An object whose spread properties are all updateable.
 */
@doc("The template for adding updateable properties.")
@withUpdateableProperties
model UpdateableProperties<Source> {
  ...Source;
}

/**
 * Represents a collection of omitted properties.
 *
 * @template Source An object whose properties are spread.
 * @template Keys The property keys to omit.
 */
@doc("The template for omitting properties.")
@withoutOmittedProperties(Keys)
model OmitProperties<Source, Keys extends string> {
  ...Source;
}

/**
 * Represents a collection of properties with only the specified keys included.
 *
 * @template Source An object whose properties are spread.
 * @template Keys The property keys to include.
 */
@doc("The template for picking properties.")
@withPickedProperties(Keys)
model PickProperties<Source, Keys extends string> {
  ...Source;
}

/**
 * Represents a collection of properties with default values omitted.
 *
 * @template Source An object whose spread property defaults are all omitted.
 */
@withoutDefaultValues
model OmitDefaults<Source> {
  ...Source;
}

/**
 * Applies a visibility setting to a collection of properties.
 *
 * @template Source An object whose properties are spread.
 * @template Visibility The visibility to apply to all properties.
 */
@doc("The template for setting the default visibility of key properties.")
@withDefaultKeyVisibility(Visibility)
model DefaultKeyVisibility<Source, Visibility extends valueof Reflection.EnumMember> {
  ...Source;
}
`,"/compiler/lib/std/visibility.tsp":`// Copyright (c) Microsoft Corporation
// Licensed under the MIT license.

import "../../dist/src/lib/tsp-index.js";

using TypeSpec.Reflection;

namespace TypeSpec;

/**
 * Sets the visibility modifiers that are active on a property, indicating that it is only considered to be present
 * (or "visible") in contexts that select for the given modifiers.
 *
 * A property without any visibility settings applied for any visibility class (e.g. \`Lifecycle\`) is considered to have
 * the default visibility settings for that class.
 *
 * If visibility for the property has already been set for a visibility class (for example, using \`@invisible\` or
 * \`@removeVisibility\`), this decorator will **add** the specified visibility modifiers to the property.
 *
 * See: [Visibility](https://typespec.io/docs/language-basics/visibility)
 *
 * The \`@typespec/http\` library uses \`Lifecycle\` visibility to determine which properties are included in the request or
 * response bodies of HTTP operations. By default, it uses the following visibility settings:
 *
 * - For the return type of operations, properties are included if they have \`Lifecycle.Read\` visibility.
 * - For POST operation parameters, properties are included if they have \`Lifecycle.Create\` visibility.
 * - For PUT operation parameters, properties are included if they have \`Lifecycle.Create\` or \`Lifecycle.Update\` visibility.
 * - For PATCH operation parameters, properties are included if they have \`Lifecycle.Update\` visibility.
 * - For DELETE operation parameters, properties are included if they have \`Lifecycle.Delete\` visibility.
 * - For GET or HEAD operation parameters, properties are included if they have \`Lifecycle.Query\` visibility.
 *
 * By default, properties have all five Lifecycle visibility modifiers enabled, so a property is visible in all contexts
 * by default.
 *
 * The default settings may be overridden using the \`@returnTypeVisibility\` and \`@parameterVisibility\` decorators.
 *
 * See also: [Automatic visibility](https://typespec.io/docs/libraries/http/operations#automatic-visibility)
 *
 * @param visibilities List of visibilities which apply to this property.
 *
 * @example
 *
 * \`\`\`typespec
 * model Dog {
 *   // The service will generate an ID, so you don't need to send it.
 *   @visibility(Lifecycle.Read)
 *   id: int32;
 *
 *   // The service will store this secret name, but won't ever return it.
 *   @visibility(Lifecycle.Create, Lifecycle.Update)
 *   secretName: string;
 *
 *   // The regular name has all vi
 *   name: string;
 * }
 * \`\`\`
 */
extern dec visibility(target: ModelProperty, ...visibilities: valueof EnumMember[]);

/**
 * Indicates that a property is not visible in the given visibility class.
 *
 * This decorator removes all active visibility modifiers from the property within
 * the given visibility class, making it invisible to any context that selects for
 * visibility modifiers within that class.
 *
 * @param visibilityClass The visibility class to make the property invisible within.
 *
 * @example
 * \`\`\`typespec
 * model Example {
 *   @invisible(Lifecycle)
 *   hidden_property: string;
 * }
 * \`\`\`
 */
extern dec invisible(target: ModelProperty, visibilityClass: Enum);

/**
 * Removes visibility modifiers from a property.
 *
 * If the visibility modifiers for a visibility class have not been initialized,
 * this decorator will use the default visibility modifiers for the visibility
 * class as the default modifier set.
 *
 * @param target The property to remove visibility from.
 * @param visibilities The visibility modifiers to remove from the target property.
 *
 * @example
 * \`\`\`typespec
 * model Example {
 *   // This property will have all Lifecycle visibilities except the Read
 *   // visibility, since it is removed.
 *   @removeVisibility(Lifecycle.Read)
 *   secret_property: string;
 * }
 * \`\`\`
 */
extern dec removeVisibility(target: ModelProperty, ...visibilities: valueof EnumMember[]);

/**
 * Removes properties that do not have at least one of the given visibility modifiers
 * active.
 *
 * If no visibility modifiers are supplied, this decorator has no effect.
 *
 * See also: [Automatic visibility](https://typespec.io/docs/libraries/http/operations#automatic-visibility)
 *
 * When using an emitter that applies visibility automatically, it is generally
 * not necessary to use this decorator.
 *
 * @param visibilities List of visibilities that apply to this property.
 *
 * @example
 * \`\`\`typespec
 * model Dog {
 *   @visibility(Lifecycle.Read)
 *   id: int32;
 *
 *   @visibility(Lifecycle.Create, Lifecycle.Update)
 *   secretName: string;
 *
 *   name: string;
 * }
 *
 * // The spread operator will copy all the properties of Dog into DogRead,
 * // and @withVisibility will then remove those that are not visible with
 * // create or update visibility.
 * //
 * // In this case, the id property is removed, and the name and secretName
 * // properties are kept.
 * @withVisibility(Lifecycle.Create, Lifecycle.Update)
 * model DogCreateOrUpdate {
 *   ...Dog;
 * }
 *
 * // In this case the id and name properties are kept and the secretName property
 * // is removed.
 * @withVisibility(Lifecycle.Read)
 * model DogRead {
 *   ...Dog;
 * }
 * \`\`\`
 */
extern dec withVisibility(target: Model, ...visibilities: valueof EnumMember[]);

/**
 * Set the visibility of key properties in a model if not already set.
 *
 * This will set the visibility modifiers of all key properties in the model if the visibility is not already _explicitly_ set,
 * but will not change the visibility of any properties that have visibility set _explicitly_, even if the visibility
 * is the same as the default visibility.
 *
 * Visibility may be set explicitly using any of the following decorators:
 *
 * - \`@visibility\`
 * - \`@removeVisibility\`
 * - \`@invisible\`
 *
 * @param visibility The desired default visibility value. If a key property already has visibility set, it will not be changed.
 */
extern dec withDefaultKeyVisibility(target: Model, visibility: valueof EnumMember);

/**
 * Declares the visibility constraint of the parameters of a given operation.
 *
 * A parameter or property nested within a parameter will be visible if it has _any_ of the visibilities
 * in the list.
 *
 * It is invalid to call this decorator with no visibility modifiers.
 *
 * @param visibilities List of visibility modifiers that apply to the parameters of this operation.
 */
extern dec parameterVisibility(target: Operation, ...visibilities: valueof EnumMember[]);

/**
 * Declares the visibility constraint of the return type of a given operation.
 *
 * A property within the return type of the operation will be visible if it has _any_ of the visibilities
 * in the list.
 *
 * It is invalid to call this decorator with no visibility modifiers.
 *
 * @param visibilities List of visibility modifiers that apply to the return type of this operation.
 */
extern dec returnTypeVisibility(target: Operation, ...visibilities: valueof EnumMember[]);

/**
 * Returns the model with non-updateable properties removed.
 */
extern dec withUpdateableProperties(target: Model);

/**
 * Declares the default visibility modifiers for a visibility class.
 *
 * The default modifiers are used when a property does not have any visibility decorators
 * applied to it.
 *
 * The modifiers passed to this decorator _MUST_ be members of the target Enum.
 *
 * @param visibilities the list of modifiers to use as the default visibility modifiers.
 */
extern dec defaultVisibility(target: Enum, ...visibilities: valueof EnumMember[]);

/**
 * A visibility class for resource lifecycle phases.
 *
 * These visibilities control whether a property is visible during the various phases of a resource's lifecycle.
 *
 * @example
 * \`\`\`typespec
 * model Dog {
 *  @visibility(Lifecycle.Read)
 *  id: int32;
 *
 *  @visibility(Lifecycle.Create, Lifecycle.Update)
 *  secretName: string;
 *
 *  name: string;
 * }
 * \`\`\`
 *
 * In this example, the \`id\` property is only visible during the read phase, and the \`secretName\` property is only visible
 * during the create and update phases. This means that the server will return the \`id\` property when returning a \`Dog\`,
 * but the client will not be able to set or update it. In contrast, the \`secretName\` property can be set when creating
 * or updating a \`Dog\`, but the server will never return it. The \`name\` property has no visibility modifiers and is
 * therefore visible in all phases.
 */
enum Lifecycle {
  /**
   * The property is visible when a resource is being created.
   */
  Create,

  /**
   * The property is visible when a resource is being read.
   */
  Read,

  /**
   * The property is visible when a resource is being updated.
   */
  Update,

  /**
   * The property is visible when a resource is being deleted.
   */
  Delete,

  /**
   * The property is visible when a resource is being queried.
   *
   * In HTTP APIs, this visibility applies to parameters of GET or HEAD operations.
   */
  Query,
}

/**
 * A visibility filter, used to specify which properties should be included when
 * using the \`withVisibilityFilter\` decorator.
 *
 * The filter matches any property with ALL of the following:
 * - If the \`any\` key is present, the property must have at least one of the specified visibilities.
 * - If the \`all\` key is present, the property must have all of the specified visibilities.
 * - If the \`none\` key is present, the property must have none of the specified visibilities.
 */
model VisibilityFilter {
  any?: EnumMember[];
  all?: EnumMember[];
  none?: EnumMember[];
}

/**
 * Applies the given visibility filter to the properties of the target model.
 *
 * This transformation is recursive, so it will also apply the filter to any nested
 * or referenced models that are the types of any properties in the \`target\`.
 *
 * If a \`nameTemplate\` is provided, newly-created type instances will be named according
 * to the template. See the \`@friendlyName\` decorator for more information on the template
 * syntax. The transformed type is provided as the argument to the template.
 *
 * @param target The model to apply the visibility filter to.
 * @param filter The visibility filter to apply to the properties of the target model.
 * @param nameTemplate The name template to use when renaming new model instances.
 *
 * @example
 * \`\`\`typespec
 * model Dog {
 *   @visibility(Lifecycle.Read)
 *   id: int32;
 *
 *   name: string;
 * }
 *
 * @withVisibilityFilter(#{ all: #[Lifecycle.Read] })
 * model DogRead {
 *  ...Dog
 * }
 * \`\`\`
 */
#deprecated "withVisibilityFilter is deprecated and will be removed in a future release. Use the \`FilterVisibility\` template or Lifecycle specific templates (e.g. \`Read\`, \`Create\`, \`Update\`, etc.) instead."
extern dec withVisibilityFilter(
  target: Model,
  filter: valueof VisibilityFilter,
  nameTemplate?: valueof string
);

/**
 * A copy of the input model \`M\` with only the properties that match the given visibility filter.
 *
 * This transformation is recursive, so it will also apply the filter to any nested
 * or referenced models that are the types of any properties in the \`target\`.
 *
 * If a \`nameTemplate\` is provided, newly-created type instances will be named according
 * to the template. See the \`@friendlyName\` decorator for more information on the template
 * syntax. The transformed type is provided as the argument to the template.
 *
 * @template M the model to apply the visibility filter to.
 * @template Filter the visibility filter to apply to the properties of the target model.
 * @template NameTemplate the name template to use when renaming new type instances.
 *
 * @example
 * \`\`\`typespec
 * model Dog {
 *   @visibility(CustomVisibility.A)
 *   id: int32;
 *   @removeVisibility(CustomVisibility.A)
 *   name: string;
 * }
 *
 * enum CustomVisibility {
 *   A,
 *   B,
 * }
 *
 * const customFilter: VisibilityFilter = #{ all: #[CustomVisibility.A] };
 *
 * // This model will have the \`id\` property but not the \`name\` property, since \`id\` has the CustomVisibility.A visibility and \`name\` does not.
 * model DogRead is FilterVisibility<Dog, customFilter, "Read{name}">;
 * \`\`\`
 */
alias FilterVisibility<
  M extends Model,
  Filter extends valueof VisibilityFilter,
  NameTemplate extends valueof string
> = applyVisibilityFilter(M, Filter, NameTemplate);

#suppress "experimental-feature"
internal extern fn applyVisibilityFilter(
  input: Model,
  filter: valueof VisibilityFilter,
  nameTemplate?: valueof string
): Model;

/**
 * Transforms the \`target\` model to include only properties that are visible during the
 * "Update" lifecycle phase.
 *
 * Any nested models of optional properties will be transformed into the "CreateOrUpdate"
 * lifecycle phase instead of the "Update" lifecycle phase, so that nested models may be
 * fully updated.
 *
 * If a \`nameTemplate\` is provided, newly-created type instances will be named according
 * to the template. See the \`@friendlyName\` decorator for more information on the template
 * syntax. The transformed type is provided as the argument to the template.
 *
 * @param target The model to apply the transformation to.
 * @param nameTemplate The name template to use when renaming new model instances.
 *
 * @example
 * \`\`\`typespec
 * model Dog {
 *   @visibility(Lifecycle.Read)
 *   id: int32;
 *
 *   @visibility(Lifecycle.Create, Lifecycle.Update)
 *   secretName: string;
 *
 *   name: string;
 * }
 *
 * @withLifecycleUpdate
 * model DogUpdate {
 *   ...Dog
 * }
 * \`\`\`
 */
#deprecated "withLifecycleUpdate is deprecated and will be removed in a future release. Use the \`Update\` template instead."
extern dec withLifecycleUpdate(target: Model, nameTemplate?: valueof string);

#suppress "experimental-feature"
internal extern fn applyLifecycleUpdate(input: Model, nameTemplate?: valueof string): Model;

/**
 * A copy of the input model \`T\` with only the properties that are visible during the
 * "Create" resource lifecycle phase.
 *
 * This transformation is recursive, and will include only properties that have the
 * \`Lifecycle.Create\` visibility modifier.
 *
 * If a \`NameTemplate\` is provided, the new model will be named according to the template.
 * The template uses the same syntax as the \`@friendlyName\` decorator.
 *
 * @template T The model to transform.
 * @template NameTemplate The name template to use for the new model.
 *
 *  * @example
 * \`\`\`typespec
 * model Dog {
 *   @visibility(Lifecycle.Read)
 *   id: int32;
 *
 *   name: string;
 * }
 *
 * // This model has only the \`name\` field.
 * model CreateDog is Create<Dog>;
 * \`\`\`
 */
alias Create<
  T extends Model,
  NameTemplate extends valueof string = "Create{name}"
> = applyVisibilityFilter(T, #{ all: #[Lifecycle.Create] }, NameTemplate);

/**
 * A copy of the input model \`T\` with only the properties that are visible during the
 * "Read" resource lifecycle phase.
 *
 * The "Read" lifecycle phase is used for properties returned by operations that read data, like
 * HTTP GET operations.
 *
 * This transformation is recursive, and will include only properties that have the
 * \`Lifecycle.Read\` visibility modifier.
 *
 * If a \`NameTemplate\` is provided, the new model will be named according to the template.
 * The template uses the same syntax as the \`@friendlyName\` decorator.
 *
 * @template T The model to transform.
 * @template NameTemplate The name template to use for the new model.
 *
 *  * @example
 * \`\`\`typespec
 * model Dog {
 *   @visibility(Lifecycle.Read)
 *   id: int32;
 *
 *   @visibility(Lifecycle.Create, Lifecycle.Update)
 *   secretName: string;
 *
 *   name: string;
 * }
 *
 * // This model has the \`id\` and \`name\` fields, but not \`secretName\`.
 * model ReadDog is Read<Dog>;
 * \`\`\`
 */
alias Read<
  T extends Model,
  NameTemplate extends valueof string = "Read{name}"
> = applyVisibilityFilter(T, #{ all: #[Lifecycle.Read] }, NameTemplate);

/**
 * A copy of the input model \`T\` with only the properties that are visible during the
 * "Update" resource lifecycle phase.
 *
 * The "Update" lifecycle phase is used for properties passed as parameters to operations
 * that update data, like HTTP PATCH operations.
 *
 * This transformation will include only the properties that have the \`Lifecycle.Update\`
 * visibility modifier, and the types of all properties will be replaced with the
 * equivalent \`CreateOrUpdate\` transformation.
 *
 * If a \`NameTemplate\` is provided, the new model will be named according to the template.
 * The template uses the same syntax as the \`@friendlyName\` decorator.
 *
 * @template T The model to transform.
 * @template NameTemplate The name template to use for the new model.
 *
 *  * @example
 * \`\`\`typespec
 * model Dog {
 *   @visibility(Lifecycle.Read)
 *   id: int32;
 *
 *   @visibility(Lifecycle.Create, Lifecycle.Update)
 *   secretName: string;
 *
 *   name: string;
 * }
 *
 * // This model will have the \`secretName\` and \`name\` fields, but not the \`id\` field.
 * model UpdateDog is Update<Dog>;
 * \`\`\`
 */
alias Update<
  T extends Model,
  NameTemplate extends valueof string = "Update{name}"
> = applyLifecycleUpdate(T, NameTemplate);

/**
 * A copy of the input model \`T\` with only the properties that are visible during the
 * "Create" or "Update" resource lifecycle phases.
 *
 * The "CreateOrUpdate" lifecycle phase is used by default for properties passed as parameters to operations
 * that can create _or_ update data, like HTTP PUT operations.
 *
 * This transformation is recursive, and will include only properties that have the
 * \`Lifecycle.Create\` or \`Lifecycle.Update\` visibility modifier.
 *
 * If a \`NameTemplate\` is provided, the new model will be named according to the template.
 * The template uses the same syntax as the \`@friendlyName\` decorator.
 *
 * @template T The model to transform.
 * @template NameTemplate The name template to use for the new model.
 *
 *  * @example
 * \`\`\`typespec
 * model Dog {
 *   @visibility(Lifecycle.Read)
 *   id: int32;
 *
 *   @visibility(Lifecycle.Create)
 *   immutableSecret: string;
 *
 *   @visibility(Lifecycle.Create, Lifecycle.Update)
 *   secretName: string;
 *
 *   name: string;
 * }
 *
 * // This model will have the \`immutableSecret\`, \`secretName\`, and \`name\` fields, but not the \`id\` field.
 * model CreateOrUpdateDog is CreateOrUpdate<Dog>;
 * \`\`\`
 */
alias CreateOrUpdate<
  T extends Model,
  NameTemplate extends valueof string = "CreateOrUpdate{name}"
> = applyVisibilityFilter(T, #{ any: #[Lifecycle.Create, Lifecycle.Update] }, NameTemplate);

/**
 * A copy of the input model \`T\` with only the properties that are visible during the
 * "Delete" resource lifecycle phase.
 *
 * The "Delete" lifecycle phase is used for properties passed as parameters to operations
 * that delete data, like HTTP DELETE operations.
 *
 * This transformation is recursive, and will include only properties that have the
 * \`Lifecycle.Delete\` visibility modifier.
 *
 * If a \`NameTemplate\` is provided, the new model will be named according to the template.
 * The template uses the same syntax as the \`@friendlyName\` decorator.
 *
 * @template T The model to transform.
 * @template NameTemplate The name template to use for the new model.
 *
 *  * @example
 * \`\`\`typespec
 * model Dog {
 *   @visibility(Lifecycle.Read)
 *   id: int32;
 *
 *   // Set when the Dog is removed from our data store. This happens when the
 *   // Dog is re-homed to a new owner.
 *   @visibility(Lifecycle.Delete)
 *   nextOwner: string;
 *
 *   name: string;
 * }
 *
 * // This model will have the \`nextOwner\` and \`name\` fields, but not the \`id\` field.
 * model DeleteDog is Delete<Dog>;
 * \`\`\`
 */
alias Delete<
  T extends Model,
  NameTemplate extends valueof string = "Delete{name}"
> = applyVisibilityFilter(T, #{ all: #[Lifecycle.Delete] }, NameTemplate);

/**
 * A copy of the input model \`T\` with only the properties that are visible during the
 * "Query" resource lifecycle phase.
 *
 * The "Query" lifecycle phase is used for properties passed as parameters to operations
 * that read data, like HTTP GET or HEAD operations. This should not be confused for
 * the \`@query\` decorator, which specifies that the property is transmitted in the
 * query string of an HTTP request.
 *
 * This transformation is recursive, and will include only properties that have the
 * \`Lifecycle.Query\` visibility modifier.
 *
 * If a \`NameTemplate\` is provided, the new model will be named according to the template.
 * The template uses the same syntax as the \`@friendlyName\` decorator.
 *
 * @template T The model to transform.
 * @template NameTemplate The name template to use for the new model.
 *
 *  * @example
 * \`\`\`typespec
 * model Dog {
 *   @visibility(Lifecycle.Read)
 *   id: int32;
 *
 *   // When getting information for a Dog, you can set this field to true to include
 *   // some extra information about the Dog's pedigree that is normally not returned.
 *   // Alternatively, you could just use a separate option parameter to get this
 *   // information.
 *   @visibility(Lifecycle.Query)
 *   includePedigree?: boolean;
 *
 *   name: string;
 *
 *   // Only included if \`includePedigree\` is set to true in the request.
 *   @visibility(Lifecycle.Read)
 *   pedigree?: string;
 * }
 *
 * // This model will have the \`includePedigree\` and \`name\` fields, but not \`id\` or \`pedigree\`.
 * model QueryDog is Query<Dog>;
 * \`\`\`
 */
alias Query<
  T extends Model,
  NameTemplate extends valueof string = "Query{name}"
> = applyVisibilityFilter(T, #{ all: #[Lifecycle.Query] }, NameTemplate);
`,"/compiler/package.json":`{
  "name": "@typespec/compiler",
  "version": "1.16.0",
  "description": "TypeSpec compiler and standard library",
  "author": "Microsoft Corporation",
  "license": "MIT",
  "homepage": "https://typespec.io",
  "readme": "https://github.com/microsoft/typespec/blob/main/README.md",
  "repository": {
    "type": "git",
    "url": "git+https://github.com/microsoft/typespec.git"
  },
  "bugs": {
    "url": "https://github.com/microsoft/typespec/issues"
  },
  "keywords": [
    "typespec",
    "cli"
  ],
  "type": "module",
  "main": "dist/src/index.js",
  "tspMain": "lib/std/main.tsp",
  "exports": {
    ".": {
      "typespec": "./lib/std/main.tsp",
      "types": "./dist/src/index.d.ts",
      "default": "./dist/src/index.js"
    },
    "./utils": {
      "types": "./dist/src/utils/index.d.ts",
      "default": "./dist/src/utils/index.js"
    },
    "./testing": {
      "types": "./dist/src/testing/index.d.ts",
      "default": "./dist/src/testing/index.js"
    },
    "./module-resolver": {
      "types": "./dist/module-resolver/index.d.ts",
      "default": "./dist/src/module-resolver/index.js"
    },
    "./ast": {
      "import": "./dist/src/ast/index.js"
    },
    "./typekit": {
      "types": "./dist/src/typekit/index.d.ts",
      "default": "./dist/src/typekit/index.js"
    },
    "./experimental": {
      "types": "./dist/src/experimental/index.d.ts",
      "default": "./dist/src/experimental/index.js"
    },
    "./experimental/typekit": {
      "types": "./dist/src/experimental/typekit/index.d.ts",
      "default": "./dist/src/experimental/typekit/index.js"
    },
    "./internals": {
      "types": "./dist/src/internals/index.d.ts",
      "import": "./dist/src/internals/index.js"
    },
    "./internals/prettier-formatter": {
      "import": "./dist/src/internals/prettier-formatter.js"
    },
    "./internals/standalone": {
      "import": "./dist/src/internals/standalone.js"
    },
    "./casing": {
      "import": "./dist/src/casing/index.js"
    },
    "./package.json": "./package.json"
  },
  "browser": {
    "./dist/src/core/node-host.js": "./dist/src/core/node-host.browser.js",
    "./dist/src/core/logger/console-sink.js": "./dist/src/core/logger/console-sink.browser.js"
  },
  "engines": {
    "node": ">=22.0.0"
  },
  "bin": {
    "tsp": "cmd/tsp.js",
    "tsp-server": "cmd/tsp-server.js"
  },
  "files": [
    "lib/**/*.tsp",
    "dist/**",
    "templates/**",
    "entrypoints",
    "!dist/test/**"
  ],
  "dependencies": {
    "@babel/code-frame": "^7.29.0",
    "@inquirer/prompts": "^8.6.0",
    "ajv": "^8.20.0",
    "change-case": "^5.4.4",
    "env-paths": "^4.0.0",
    "is-unicode-supported": "^2.1.0",
    "mustache": "^4.2.0",
    "picocolors": "^1.1.1",
    "prettier": "^3.9.6",
    "semver": "^7.8.5",
    "tar": "^7.5.22",
    "temporal-polyfill": "^1.0.4",
    "vscode-languageserver": "^10.1.0",
    "vscode-languageserver-textdocument": "^1.0.13",
    "yaml": "^2.9.0",
    "yargs": "^18.1.0"
  },
  "devDependencies": {
    "@types/babel__code-frame": "^7.27.0",
    "@types/mustache": "^4.2.6",
    "@types/node": "^26.3.0",
    "@types/semver": "^7.8.0",
    "@types/yargs": "^17.0.35",
    "@vitest/coverage-v8": "^4.1.11",
    "@vitest/ui": "^4.1.11",
    "pathe": "^2.0.3",
    "rimraf": "^6.1.3",
    "typescript": "~6.0.2",
    "vitest": "^4.1.11",
    "vscode-oniguruma": "^2.0.1",
    "vscode-textmate": "^9.3.2",
    "@typespec/internal-build-utils": "^0.86.0",
    "tmlanguage-generator": "^0.6.8"
  },
  "scripts": {
    "clean": "rimraf ./dist ./temp",
    "build:init-templates-index": "node ./.scripts/build-init-templates.ts",
    "build": "pnpm gen-manifest && pnpm build:init-templates-index && pnpm compile && pnpm generate-tmlanguage",
    "api-extractor": "api-extractor run --local --verbose",
    "compile": "tsc -p tsconfig.build.json",
    "watch": "tsc -p tsconfig.build.json --watch",
    "watch-tmlanguage": "node scripts/watch-tmlanguage.ts",
    "generate-tmlanguage": "node scripts/generate-tmlanguage.ts",
    "gen-extern-signature": "node ./.scripts/gen-extern-signature.ts",
    "dogfood": "node scripts/dogfood.ts",
    "test": "vitest run",
    "test:ui": "vitest --ui",
    "test:watch": "vitest -w",
    "test:ci": "vitest run --coverage --reporter=junit --reporter=default",
    "test:e2e": "vitest run --config ./vitest.config.e2e.ts",
    "gen-manifest": "node scripts/generate-manifest.ts",
    "regen-nonascii": "node scripts/regen-nonascii.ts",
    "fuzz": "node dist/test/manual/fuzz.js run",
    "lint": "oxlint . --deny-warnings",
    "lint:fix": "oxlint . --fix"
  }
}`};var Vo={"/compiler/dist/src/lib/tsp-index.js":Vf,"/compiler/dist/src/lib/intrinsic/tsp-index.js":Kf};async function yo(n,e,s={},t){let i=new Map(Object.entries(Cf)),h={...Vo};if(t)h["/project/node_modules/@typespec/json-schema/dist/src/index.js"]=Yh;for(let[u,a]of Object.entries(s)){let c=Object.hasOwn(We,u)?We[u]:void 0;if(!c||c.version!==a)throw new ln("TYPESPEC_LIBRARY_UNAVAILABLE","No registered library "+u+"@"+a);for(let[b,r]of Object.entries(c.files))i.set("/project/node_modules/"+u+"/"+b,r);for(let[b,r]of Object.entries(If))if(b.startsWith(u+"/"))h["/project/node_modules/"+b]=r}for(let[u,a]of Object.entries(n)){if(i.has("/project/"+u))throw new ln("TYPESPEC_LIBRARY_COLLISION","Supplied source conflicts with registered library: "+u);i.set("/project/"+u,a)}let d=(u)=>Object.assign(Error("No supplied file: "+u),{code:"ENOENT"});return await yf({async readFile(u){let a=i.get(u);if(a===void 0)throw d(u);return Mf(a,u)},async readUrl(u){throw d(u)},async writeFile(u,a){if(!t)throw Error("Emission is not enabled");if(!u.startsWith("/output/")||u.split("/").includes(".."))throw new ln("TYPESPEC_OUTPUT_PATH","Emitter output escaped its memory root");let c=u.slice(8);if(Object.hasOwn(t.outputs,c))throw new ln("TYPESPEC_OUTPUT_COLLISION","Duplicate emitter output: "+c);if(a.length+Object.values(t.outputs).reduce((b,r)=>b+r.length,0)>ze.maxTextLength)throw new ln("LIMIT","Emitter output exceeds text limit");t.outputs[c]=a},async readDir(u){let a=u.replace(/\/$/,"")+"/";return[...new Set([...i.keys()].filter((c)=>c.startsWith(a)).map((c)=>c.slice(a.length).split("/")[0]))]},async rm(){throw Error("Removal is not enabled")},async mkdirp(){return},async stat(u){let a=i.has(u),c=[...i.keys()].some((b)=>b.startsWith(u.replace(/\/$/,"")+"/"));if(!a&&!c&&!Object.hasOwn(h,u))throw d(u);return{isFile:()=>a||Object.hasOwn(h,u),isDirectory:()=>c}},async realpath(u){return u},getExecutionRoot:()=>"/compiler",getLibDirs:()=>["/compiler/lib/std"],async getJsImport(u){if(Object.hasOwn(h,u))return h[u];throw d(u)},getSourceFileKind:Bf,fileURLToPath:(u)=>decodeURIComponent(new URL(u).pathname),pathToFileURL:(u)=>"file://"+u,logSink:{log(){}}},"/project/"+e,t?{noEmit:!1,outputDir:"/output",emit:["@typespec/json-schema"],options:{"@typespec/json-schema":t.options}}:{noEmit:!0})}function po(n,e={}){let s=n.diagnostics.map((t)=>{let i=ge(t.target);return{code:t.code,severity:t.severity,message:t.message,...i?{file:i.file.path,start:i.pos,end:i.end}:{}}});return{compiler:"@typespec/compiler@1.16.0",libraries:{...e},valid:!n.hasError(),complete:!1,diagnostics:s,limitations:["Compiler checks supplied TypeSpec and the pinned standard library; only explicitly selected registered libraries are available; arbitrary JavaScript and emitters are not loaded","Compiler validity does not establish cross-system projection equivalence"]}}async function pf(n,e,s={}){return po(await yo(n,e,s),s)}var cs="umf.typespec",go=Xh,nw=gf().compile(Xh.schema);function ew(n){let e=n,s=[];for(let i of Object.keys(e))if(!["profile","entrypoint","files","libraries"].includes(i))s.push({code:"TYPESPEC_REPRESENTATION",path:"/"+Je(i),severity:"warning",message:"Unknown source representation field cannot be emitted natively"});for(let[i,h]of Object.entries(e.libraries??{}))if(!Object.hasOwn(We,i)||We[i].version!==h)s.push({code:"TYPESPEC_LIBRARY_UNAVAILABLE",path:"/libraries/"+Je(i),severity:"warning",message:"Unregistered library selection retained: "+i+"@"+h});if(!Object.hasOwn(e.files,e.entrypoint))s.push({code:"TYPESPEC_ENTRYPOINT",path:"/entrypoint",severity:"error",message:"Entrypoint must name a supplied file"});let t=0;for(let[i,h]of Object.entries(e.files)){if(!/^(?:[A-Za-z0-9_.-]+\/)*[A-Za-z0-9_.-]+\.tsp$/.test(i)||i.split("/").some((d)=>d==="."||d==="..")){s.push({code:"TYPESPEC_PATH",path:"/files/"+Je(i),severity:"error",message:"Source paths must be relative .tsp paths without traversal"});continue}if(t+=h.length,t>ze.maxTextLength){s.push({code:"LIMIT",path:"/files",severity:"error",message:"Source bundle exceeds text limit"});break}try{for(let d of bs(h,{comments:!0,docs:!0}).parseDiagnostics)s.push({code:"TYPESPEC_"+d.code,path:"/files/"+Je(i),severity:d.severity,message:d.message})}catch(d){s.push({code:"TYPESPEC_PARSE",path:"/files/"+Je(i),severity:"error",message:String(d)})}}return s.push({code:"TYPESPEC_COMPILATION_REQUIRED",path:"",severity:"warning",message:"Syntax preservation does not establish compiler validity; call compileTypeSpecDocument with supplied imports"}),s}function sw(){return new nu().register(go,ew)}function _h(n){return eu(n,sw())}function os(n){let e=_h(n);if(!e.valid)throw new ln("TYPESPEC_DOCUMENT",JSON.stringify(e.diagnostics));let s=n.modules.find((t)=>t.id==="schema")?.elements.find((t)=>t.id==="schema")?.extensions[cs];if(n.vocabularies[cs]?.version!=="0.1.0"||!nw(s))throw new ln("TYPESPEC_PAYLOAD","Missing TypeSpec source bundle");return Dt(s)}function l3(n,e){let s=Dt(n);if(Object.keys(s).some((h)=>!["entrypoint","files","libraries"].includes(h)))throw new ln("TYPESPEC_INPUT","Unknown source-bundle option");let t={profile:"typespec-1.16.0-sources",...s},i={umf:"0.1.0",id:e.id,vocabularies:{[cs]:{version:"0.1.0"}},modules:[{id:"schema",namespace:"",elements:[{id:"schema",extensions:{[cs]:t}}]}]};return os(i),i}function tw(n){let e=os(n);if(_h(n).diagnostics.some((s)=>s.code==="TYPESPEC_REPRESENTATION"))throw new ln("TYPESPEC_REPRESENTATION","Native export would discard unknown content");return{entrypoint:e.entrypoint,files:e.files,...e.libraries?{libraries:e.libraries}:{}}}function f3(n){let e=tw(n);return pf(e.files,e.entrypoint,e.libraries)}function u3(n,e,s){let t=os(n);if(!Object.hasOwn(t.files,e)||typeof s!=="string")throw new ln("TYPESPEC_EDIT","Edit must replace a supplied source file");t.files[e]=s;let i=Dt(n);return i.modules.find((h)=>h.id==="schema").elements.find((h)=>h.id==="schema").extensions[cs]=t,os(i),{document:i,validation:_h(i)}}function a3(n,e){let s=os(n);if(!Object.hasOwn(s.files,e))throw new ln("TYPESPEC_FILE","No supplied source file");let t=[];function i(h,d){if(d>ze.maxDepth||t.length>=ze.maxValues)throw new ln("LIMIT","Syntax traversal exceeds structural limit");t.push({kind:K[h.kind],start:h.pos,end:h.end}),Vt(h,(f)=>{i(f,d+1);return})}return i(bs(s.files[e],{comments:!0,docs:!0}),0),t}export{sw as typespecRegistry,go as typespecPackage,u3 as proposeTypeSpecSourceEdit,_h as inspectTypeSpec,l3 as importTypeSpecSources,a3 as getTypeSpecSyntax,tw as exportTypeSpecSources,f3 as compileTypeSpecDocument,cs as TYPESPEC_EXTENSION};
