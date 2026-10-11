import{$,$invisible,$removeVisibility,$visibility,DuplicateTracker,MutatorFlow,NoTarget,SyntaxKind,compile,compilerAssert,createDiagnosticCollector,createLinterRule,createSourceFile,createTypeSpecLibrary,defineLinter,definePackageFlags,defineTypeInfoProvider,emitFile,explainStringTemplateNotSerializable,exports_tsp_index,exports_tsp_index1 as exports_tsp_index2,exports_tsp_index2 as exports_tsp_index3,fileRef,filterModelProperties,getDeprecated,getDirectoryPath,getDiscriminatedUnion,getDiscriminator,getDoc,getEncode,getErrorsDoc,getExamples,getFormat,getKey,getLifecycleVisibilityEnum,getMaxItems,getMaxLength,getMaxValue,getMaxValueExclusive,getMediaTypeHint,getMinItems,getMinLength,getMinValue,getMinValueExclusive,getNamespaceFullName,getOverloadedOperation,getOverloads,getParameterVisibilityFilter,getPattern,getProperty,getRelativePathFromDirectory,getReturnsDoc,getSourceFileKindFromExt,getSourceLocation,getStreamOf,getSummary,getTypeName,isArrayModelType,isErrorModel,isErrorType,isKey,isNullType,isStringType,isTemplateDeclaration,isTemplateInstance,isType,isVisible,isVoidType,joinPaths,listOperationsIn,listServices,mutateSubgraph,navigateProgram,navigateType,navigateTypesInNamespace,paramMessage,parse,resetVisibilityModifiersForClass,sanitizePathSegment,serializeValueAsJson,setMediaTypeHint,setTypeSpecNamespace,typespecTypeToJson,useStateMap,useStateSet,validateDecoratorUniqueOnNode,visitChildren,walkPropertiesInherited}from"./chunk-zefgejd5.js";import"./chunk-4cp6pdvp.js";import{stringify}from"./chunk-wws8tn2b.js";import{Registry,validateDocument}from"./chunk-1b9zv8y7.js";import{createValidator}from"./chunk-ye2s8p8w.js";import"./chunk-q1g6p5sq.js";import{LIMITS,UmfError,copyJson,pointer}from"./chunk-fde5egca.js";import{__export,__require}from"./chunk-3cxgdp6v.js";var exports_tsp_index4={};__export(exports_tsp_index4,{$onValidate:()=>$onValidate,$lib:()=>$lib,$decorators:()=>$decorators});var EmitterOptionsSchema={type:"object",additionalProperties:!1,properties:{"output-file":{type:"string",nullable:!0,description:["Name of the output file."," Output file will interpolate the following values:"," - schema-name: Name of the schema if multiple",""," Default: `{schema-name}.graphql`",""," Example Single schema"," - `schema.graphql`",""," Example Multiple schemas"," - `Org1.Schema1.graphql`"," - `Org1.Schema2.graphql`"].join(`
`)},"new-line":{type:"string",enum:["crlf","lf"],default:"lf",nullable:!0,description:"Set the newLine character for emitting files."},"omit-unreachable-types":{type:"boolean",nullable:!0,description:["Omit unreachable types.","By default all types declared under the schema namespace will be included.","With this flag on only types references in an operation will be emitted."].join(`
`)}},required:[]},libDef={name:"@typespec/graphql",diagnostics:{"graphql-operation-kind-duplicate":{severity:"error",messages:{default:paramMessage`GraphQL Operation Kind already applied to \`${"entityName"}\`.`}},"operation-field-conflict":{severity:"error",messages:{default:paramMessage`Operation \`${"operation"}\` conflicts with an existing ${"conflictType"} on model \`${"model"}\`.`}},"operation-field-duplicate":{severity:"warning",messages:{default:paramMessage`Operation \`${"operation"}\` is defined multiple times on \`${"model"}\`.`}},"invalid-interface":{severity:"error",messages:{default:paramMessage`All models used with \`@compose\` must be marked with \`@graphqlInterface\`, but ${"interface"} is not.`}},"circular-interface":{severity:"error",messages:{default:"An interface cannot implement itself."}},"missing-interface-property":{severity:"error",messages:{default:paramMessage`Model must contain property \`${"property"}\` from \`${"interface"}\` in order to implement it in GraphQL.`}},"incompatible-interface-property":{severity:"error",messages:{default:paramMessage`Property \`${"property"}\` is incompatible with \`${"interface"}\`.`}},"unrecognized-union":{severity:"error",messages:{default:"Unrecognized union construction. Union must be named, a return type, a model property, or an alias."}},"duplicate-union-variant":{severity:"warning",messages:{default:paramMessage`Union variant type "${"type"}" appears multiple times after flattening nested unions. Duplicate removed.`}},"empty-union":{severity:"error",messages:{default:"Union has no non-null variants. A GraphQL union must contain at least one member type."}},"graphql-builtin-scalar-collision":{severity:"warning",messages:{default:paramMessage`Scalar "${"name"}" collides with GraphQL built-in type "${"builtinName"}". This may cause unexpected behavior. Consider renaming the scalar.`}},"type-name-collision":{severity:"error",messages:{default:paramMessage`Type "${"name"}" collides with another type of the same name in the GraphQL schema. Consider renaming one of the types.`}},"operation-fields-ignored-on-input":{severity:"warning",messages:{default:paramMessage`@operationFields on \`${"model"}\` is ignored in input context — GraphQL input types cannot have operation fields.`}},"empty-schema":{severity:"warning",messages:{default:"GraphQL schema has no operations. At minimum a Query root type is required."}},"empty-enum":{severity:"error",messages:{default:paramMessage`Enum "${"name"}" must define at least one value. GraphQL enums cannot be empty.`}},"reserved-name":{severity:"error",messages:{default:paramMessage`Name "${"name"}" must not begin with "__" (two underscores), which is reserved by GraphQL for introspection.`}},"unsupported-type":{severity:"warning",messages:{default:paramMessage`Type "${"type"}" has no GraphQL equivalent. Using String as fallback.`}}},emitter:{options:EmitterOptionsSchema,capabilities:{dryRun:!0}},state:{operationKind:{description:"State for the graphql operation kind decorators (@query, @mutation, @subscription)"},operationFields:{description:"State for the @operationFields decorator."},compose:{description:"State for the @compose decorator."},interface:{description:"State for the @interface decorator."},interfaceOnly:{description:"State for @interface(#{interfaceOnly: true})."},schema:{description:"State for the @schema decorator."},specifiedBy:{description:"State for the @specifiedBy decorator."}}},$lib=createTypeSpecLibrary(libDef),{reportDiagnostic,createDiagnostic,stateKeys:GraphQLKeys}=$lib;function typesEqual(a,b){return a===b||getTypeName(a)===getTypeName(b)}function propertiesEqual(prop1,prop2,ignoreNames=!1){if(!ignoreNames&&prop1.name!==prop2.name)return!1;return typesEqual(prop1.type,prop2.type)&&prop1.optional===prop2.optional}function modelsEqual(model1,model2,ignoreNames=!1){if(!ignoreNames&&model1.name!==model2.name)return!1;let model1Properties=new Set(walkPropertiesInherited(model1)),model2Properties=new Set(walkPropertiesInherited(model2));if(model1Properties.size!==model2Properties.size)return!1;if([...model1Properties].some((prop)=>![...model2Properties].some((p)=>propertiesEqual(prop,p,!1))))return!1;return!0}function operationsEqual(op1,op2,ignoreNames=!1){if(!ignoreNames&&op1.name!==op2.name)return!1;return typesEqual(op1.returnType,op2.returnType)&&modelsEqual(op1.parameters,op2.parameters,!0)}var[getInterface,setInterface]=useStateSet(GraphQLKeys.interface),[getInterfaceOnly,setInterfaceOnly]=useStateSet(GraphQLKeys.interfaceOnly),[getComposition,setComposition,_getCompositionMap]=useStateMap(GraphQLKeys.compose);function isInterface(program,model){return!!getInterface(program,model)}function validateImplementedsAreInterfaces(context,interfaces){let valid=!0;for(let iface of interfaces)if(!isInterface(context.program,iface))valid=!1,reportDiagnostic(context.program,{code:"invalid-interface",format:{interface:iface.name},target:context.decoratorTarget});return valid}function validateNoCircularImplementation(context,target,interfaces){let valid=!isInterface(context.program,target)||!interfaces.includes(target);if(!valid)reportDiagnostic(context.program,{code:"circular-interface",target:context.decoratorTarget});return valid}function validateImplementsInterfaceProperties(context,modelProperties,iface){let valid=!0;for(let prop of walkPropertiesInherited(iface))if(!modelProperties.has(prop.name))valid=!1,reportDiagnostic(context.program,{code:"missing-interface-property",format:{interface:iface.name,property:prop.name},target:context.decoratorTarget});else if(!propertiesEqual(modelProperties.get(prop.name),prop))valid=!1,reportDiagnostic(context.program,{code:"incompatible-interface-property",format:{interface:iface.name,property:prop.name},target:context.decoratorTarget});return valid}function validateImplementsInterfacesProperties(context,target,interfaces){let valid=!0,allModelProperties=new Map([...walkPropertiesInherited(target)].map((prop)=>[prop.name,prop]));for(let iface of interfaces)if(!validateImplementsInterfaceProperties(context,allModelProperties,iface))valid=!1;return valid}var $graphqlInterface=(context,target,options)=>{if(validateDecoratorUniqueOnNode(context,target,$graphqlInterface),setInterface(context.program,target),options?.interfaceOnly)setInterfaceOnly(context.program,target)},$compose=(context,target,...interfaces)=>{validateImplementedsAreInterfaces(context,interfaces),validateNoCircularImplementation(context,target,interfaces),validateImplementsInterfacesProperties(context,target,interfaces);let existingCompose=getComposition(context.program,target),composed=interfaces;if(existingCompose)composed=[...existingCompose,...composed];setComposition(context.program,target,composed)};var[getOperationFieldsInternal,setOperationFields,_getOperationFieldsMap]=useStateMap(GraphQLKeys.operationFields);function getOperationFields(program,model){return getOperationFieldsInternal(program,model)||new Set}function validateDuplicateProperties(context,model,operation){if(getOperationFields(context.program,model).has(operation))return reportDiagnostic(context.program,{code:"operation-field-duplicate",format:{operation:operation.name,model:model.name},target:context.getArgumentTarget(0)}),!1;return!0}function validateNoConflictWithProperties(context,model,operation){let conflictTypes=[];if([...walkPropertiesInherited(model)].some((prop)=>prop.name===operation.name))conflictTypes.push("property");let existingOperation=[...getOperationFields(context.program,model)].find((op)=>op.name===operation.name);if(existingOperation&&!operationsEqual(existingOperation,operation))conflictTypes.push("operation");for(let conflictType of conflictTypes)reportDiagnostic(context.program,{code:"operation-field-conflict",format:{operation:operation.name,model:model.name,conflictType},target:context.getArgumentTarget(0)});return conflictTypes.length===0}function addOperationField(context,model,operation){let operationFields=getOperationFields(context.program,model);if(!validateDuplicateProperties(context,model,operation))return;if(!validateNoConflictWithProperties(context,model,operation))return;operationFields.add(operation),setOperationFields(context.program,model,operationFields)}var $operationFields=(context,target,...operationOrInterfaces)=>{for(let operationOrInterface of operationOrInterfaces)if(operationOrInterface.kind==="Operation")addOperationField(context,target,operationOrInterface);else for(let[_,operation]of operationOrInterface.operations)addOperationField(context,target,operation)};var[getOperationKind,setOperationKindInternal,_getOperationKindMap]=useStateMap(GraphQLKeys.operationKind);function validateOperationKindUniqueOnNode(context,operation){if(operation.decorators.filter((x)=>OPERATION_KIND_DECORATORS.includes(x.decorator)&&x.node?.kind===SyntaxKind.DecoratorExpression&&x.node?.parent===operation.node).length>1)return reportDiagnostic(context.program,{code:"graphql-operation-kind-duplicate",format:{entityName:operation.name},target:context.decoratorTarget}),!1;return!0}function setOperationKind(context,entity,operationKind){if(validateOperationKindUniqueOnNode(context,entity))setOperationKindInternal(context.program,entity,operationKind)}function createOperationKindDecorator(operationKind){return(context,entity)=>{setOperationKind(context,entity,operationKind)}}var $mutation=createOperationKindDecorator("Mutation"),$query=createOperationKindDecorator("Query"),$subscription=createOperationKindDecorator("Subscription"),OPERATION_KIND_DECORATORS=[$mutation,$query,$subscription];var[getSchema,setSchema,getSchemaMap]=useStateMap(GraphQLKeys.schema);function listSchemas(program){return[...getSchemaMap(program).values()]}function addSchema(program,namespace,details={}){let existing=getSchemaMap(program).get(namespace)??{};setSchema(program,namespace,{...existing,...details,type:namespace})}var $schema=(context,target,options)=>{validateDecoratorUniqueOnNode(context,target,$schema),addSchema(context.program,target,options)};var[getSpecifiedByUrl,setSpecifiedByUrl]=useStateMap(GraphQLKeys.specifiedBy);var $specifiedBy=(context,target,url)=>{validateDecoratorUniqueOnNode(context,target,$specifiedBy),setSpecifiedByUrl(context.program,target,url)};function $onValidate(program){let schemas=listSchemas(program);if(schemas.length===0)return;for(let schema of schemas)validateSchema(program,schema.type)}function validateSchema(program,ns){let hasGraphQLOps=!1;if(navigateTypesInNamespace(ns,{operation(op){if(getOperationKind(program,op)!==void 0)hasGraphQLOps=!0;validateOperation(program,op)},model(model){validateModel(program,model)},enum(enumType){validateEnum(program,enumType)},union(unionType){validateUnion(program,unionType)}}),!hasGraphQLOps)reportDiagnostic(program,{code:"empty-schema",target:ns})}function validateReservedName(program,name,target){if(name.startsWith("__"))reportDiagnostic(program,{code:"reserved-name",format:{name},target})}function validateModel(program,model){if(model.name)validateReservedName(program,model.name,model);for(let prop of model.properties.values())validateReservedName(program,prop.name,prop)}function validateOperation(program,op){validateReservedName(program,op.name,op);for(let param of op.parameters.properties.values())validateReservedName(program,param.name,param)}function validateUnion(program,unionType){if(!unionType.name)return;if(validateReservedName(program,unionType.name,unionType),[...unionType.variants.values()].filter((v)=>!isNullType(v.type)).length===0)reportDiagnostic(program,{code:"empty-union",target:unionType})}function validateEnum(program,enumType){if(enumType.name)validateReservedName(program,enumType.name,enumType);for(let member of enumType.members.values())validateReservedName(program,member.name,member);if(enumType.members.size===0)reportDiagnostic(program,{code:"empty-enum",format:{name:enumType.name},target:enumType})}var $decorators={"TypeSpec.GraphQL":{compose:$compose,graphqlInterface:$graphqlInterface,mutation:$mutation,operationFields:$operationFields,query:$query,schema:$schema,specifiedBy:$specifiedBy,subscription:$subscription}};var exports_tsp_index5={};__export(exports_tsp_index5,{$lib:()=>$lib2,$flags:()=>$flags,$decorators:()=>$decorators2});var EmitterOptionsSchema2={type:"object",additionalProperties:!1,properties:{"file-type":{type:"string",enum:["yaml","json"],nullable:!0,description:"Serialize the schema as either yaml or json."},"int64-strategy":{type:"string",enum:["string","number"],nullable:!0,description:`How to handle 64 bit integers on the wire. Options are:

* string: serialize as a string (widely interoperable)
* number: serialize as a number (not widely interoperable)`},bundleId:{type:"string",nullable:!0,description:"When provided, bundle all the schemas into a single json schema document with schemas under $defs. The provided id is the id of the root document and is also used for the file name."},emitAllModels:{type:"boolean",nullable:!0,description:"When true, emit all model declarations to JSON Schema without requiring the @jsonSchema decorator."},emitAllRefs:{type:"boolean",nullable:!0,description:"When true, emit all references as json schema files, even if the referenced type does not have the `@jsonSchema` decorator or is not within a namespace with the `@jsonSchema` decorator."},"seal-object-schemas":{type:"boolean",nullable:!0,default:!1,description:["If true, then for models emitted as object schemas we default `unevaluatedProperties` to `{ not: {} }`,","if not explicitly specified elsewhere.","Default: `false`"].join(`
`)},"polymorphic-models-strategy":{type:"string",enum:["ignore","oneOf","anyOf"],nullable:!0,default:"ignore",description:["Strategy for emitting models with the @discriminator decorator:","- ignore: Emit as regular object schema (default). Derived models use allOf to reference their base model.","- oneOf: Emit a oneOf schema with references to all derived models (closed union)","- anyOf: Emit an anyOf schema with references to all derived models (open union)","","When using oneOf or anyOf, derived models will inline all properties from their base model","instead of using allOf references. This avoids circular references in the generated schemas,","since the base model references derived models via oneOf/anyOf."].join(`
`)}},required:[]},$lib2=createTypeSpecLibrary({name:"@typespec/json-schema",diagnostics:{"invalid-default":{severity:"error",messages:{default:paramMessage`Invalid type '${"type"}' for a default value`}},"duplicate-id":{severity:"error",messages:{default:paramMessage`There are multiple types with the same id "${"id"}".`}},"unknown-scalar":{severity:"warning",messages:{default:paramMessage`Scalar '${"name"}' is not a known scalar type and doesn't extend a known scalar type.`}}},emitter:{options:EmitterOptionsSchema2},state:{JsonSchema:{description:"State indexing types marked with @jsonSchema"},"JsonSchema.baseURI":{description:"Contains data configured with @baseUri decorator"},"JsonSchema.multipleOf":{description:"Contains data configured with @multipleOf decorator"},"JsonSchema.id":{description:"Contains data configured with @id decorator"},"JsonSchema.oneOf":{description:"Contains data configured with @oneOf decorator"},"JsonSchema.contains":{description:"Contains data configured with @contains decorator"},"JsonSchema.minContains":{description:"Contains data configured with @minContains decorator"},"JsonSchema.maxContains":{description:"Contains data configured with @maxContains decorator"},"JsonSchema.uniqueItems":{description:"Contains data configured with @uniqueItems decorator"},"JsonSchema.minProperties":{description:"Contains data configured with @minProperties decorator"},"JsonSchema.maxProperties":{description:"Contains data configured with @maxProperties decorator"},"JsonSchema.contentEncoding":{description:"Contains data configured with @contentEncoding decorator"},"JsonSchema.contentSchema":{description:"Contains data configured with @contentSchema decorator"},"JsonSchema.contentMediaType":{description:"Contains data configured with @contentMediaType decorator"},"JsonSchema.prefixItems":{description:"Contains data configured with @prefixItems decorator"},"JsonSchema.extension":{description:"Contains data configured with @extension decorator"}}}),$flags=definePackageFlags({}),{reportDiagnostic:reportDiagnostic2,createStateSymbol,stateKeys:JsonSchemaStateKeys}=$lib2;function createDataDecorator(key,validate){let[getData,setData]=useStateMap(key);return[getData,setData,(...args)=>{if(validate&&!validate(...args))return;let[context,target,value]=args;setData(context.program,target,value)}]}function includeDerivedModel(model){return!isTemplateDeclaration(model)&&(model.templateMapper?.args===void 0||model.templateMapper.args?.length===0||model.derivedModels.length>0)}var[getJsonSchema,markJsonSchema]=useStateSet(JsonSchemaStateKeys.JsonSchema),$jsonSchema=(context,target,baseUriOrId)=>{if(markJsonSchema(context.program,target),baseUriOrId)if(target.kind==="Namespace")context.call($baseUri,target,baseUriOrId);else context.call($id,target,baseUriOrId)},[getBaseUri,setBaseUri,$baseUri]=createDataDecorator(JsonSchemaStateKeys["JsonSchema.baseURI"]);function findBaseUri(program,target){let baseUrl,current=target;do baseUrl=getBaseUri(program,current),current=current.namespace;while(!baseUrl&&current);return baseUrl}function isJsonSchemaDeclaration(program,target){let current=target;do{if(getJsonSchema(program,current))return!0;current=current.namespace}while(current);return!1}function getJsonSchemaTypes(program){let types=[];function visitNamespace(ns){if(getJsonSchema(program,ns))types.push(ns);visitMembers(ns.models.values()),visitMembers(ns.enums.values()),visitMembers(ns.unions.values()),visitMembers(ns.scalars.values());for(let member of ns.namespaces.values())visitNamespace(member)}function visitMembers(members){for(let member of members)visitDeclaration(member)}function visitDeclaration(type){if(!(type.kind!=="Enum"&&isTemplateDeclaration(type))&&isJsonSchemaDeclaration(program,type))types.push(type)}return visitNamespace(program.getGlobalNamespaceType()),types}var[getMultipleOfAsNumeric,setMultipleOf,$multipleOf]=createDataDecorator(JsonSchemaStateKeys["JsonSchema.multipleOf"]);function getMultipleOf(program,target){return getMultipleOfAsNumeric(program,target)?.asNumber()??void 0}var[getId,setId,$id]=createDataDecorator(JsonSchemaStateKeys["JsonSchema.id"]),[isOneOf,markOneOf]=useStateSet(JsonSchemaStateKeys["JsonSchema.oneOf"]),$oneOf=(context,target)=>{markOneOf(context.program,target)},[getContains,setContains,$contains]=createDataDecorator(JsonSchemaStateKeys["JsonSchema.contains"]),[getMinContains,setMinContains,$minContains]=createDataDecorator(JsonSchemaStateKeys["JsonSchema.minContains"]),[getMaxContains,setMaxContains,$maxContains]=createDataDecorator(JsonSchemaStateKeys["JsonSchema.maxContains"]),[getUniqueItems,setUniqueItems]=useStateMap(JsonSchemaStateKeys["JsonSchema.uniqueItems"]),$uniqueItems=(context,target)=>setUniqueItems(context.program,target,!0),[getMinProperties,setMinProperties,$minProperties]=createDataDecorator(JsonSchemaStateKeys["JsonSchema.minProperties"]),[getMaxProperties,setMaxProperties,$maxProperties]=createDataDecorator(JsonSchemaStateKeys["JsonSchema.maxProperties"]),[getContentEncoding,setContentEncoding,$contentEncoding]=createDataDecorator(JsonSchemaStateKeys["JsonSchema.contentEncoding"]),[getContentMediaType,setContentMediaType,$contentMediaType]=createDataDecorator(JsonSchemaStateKeys["JsonSchema.contentMediaType"]),[getContentSchema,setContentSchema,$contentSchema]=createDataDecorator(JsonSchemaStateKeys["JsonSchema.contentSchema"]),[getPrefixItems,setPrefixItems]=useStateMap(JsonSchemaStateKeys["JsonSchema.prefixItems"]),$prefixItems=(context,target,value)=>{setPrefixItems(context.program,target,value)},[getExtensionsInternal,_,getExtensionsStateMap]=useStateMap(JsonSchemaStateKeys["JsonSchema.extension"]),$extension=(context,target,key,value)=>{if(!isTypeLike(value))value=convertRemainingValuesToExtensions(context.program,value);setExtension(context.program,target,key,value)};function convertRemainingValuesToExtensions(program,value){switch(typeof value){case"string":case"number":case"boolean":return value;case"object":if(value===null)return null;if(Array.isArray(value))return value.map((x)=>convertRemainingValuesToExtensions(program,x));if(isTypeSpecValue(value))return serializeValueAsJson(program,value,value.type);else{let result={};for(let[key,val]of Object.entries(value)){if(val===void 0)continue;result[key]=convertRemainingValuesToExtensions(program,val)}return result}default:return value}}function isTypeLike(value){return typeof value==="object"&&value!==null&&isType(value)}function isTypeSpecValue(value){return"entityKind"in value&&value.entityKind==="Value"}function getExtensions(program,target){return getExtensionsInternal(program,target)??[]}function setExtension(program,target,key,value){let stateMap=getExtensionsStateMap(program),extensions=stateMap.has(target)?stateMap.get(target):stateMap.set(target,[]).get(target);if(isJsonTemplateType(value))extensions.push({key,value:typespecTypeToJson(value.properties.get("value").type,target)[0]});else extensions.push({key,value})}function isJsonTemplateType(value){return typeof value==="object"&&value!==null&&isType(value)&&value.kind==="Model"&&value.name==="Json"&&value.namespace?.name==="JsonSchema"}var $validatesRawJson=(context,target,value)=>{let[_2,diagnostics]=typespecTypeToJson(value,target);if(diagnostics.length>0)context.program.reportDiagnostics(diagnostics)};setTypeSpecNamespace("Private",$validatesRawJson);var $decorators2={"TypeSpec.JsonSchema":{jsonSchema:$jsonSchema,baseUri:$baseUri,id:$id,oneOf:$oneOf,multipleOf:$multipleOf,contains:$contains,minContains:$minContains,maxContains:$maxContains,uniqueItems:$uniqueItems,minProperties:$minProperties,maxProperties:$maxProperties,contentEncoding:$contentEncoding,prefixItems:$prefixItems,contentMediaType:$contentMediaType,contentSchema:$contentSchema,extension:$extension},"TypeSpec.JsonSchema.Private":{validatesRawJson:$validatesRawJson}};var exports_tsp_index6={};__export(exports_tsp_index6,{$lib:()=>TypeSpecProtobufLibrary,$decorators:()=>$decorators3});var $scalar=Symbol("$scalar"),$ref=Symbol("$ref"),$map=Symbol("$map");var StreamingMode;(function(StreamingMode2){StreamingMode2[StreamingMode2.Duplex=3]="Duplex",StreamingMode2[StreamingMode2.In=2]="In",StreamingMode2[StreamingMode2.Out=1]="Out",StreamingMode2[StreamingMode2.None=0]="None"})(StreamingMode||(StreamingMode={}));var EmitterOptionsSchema3={type:"object",additionalProperties:!1,properties:{noEmit:{type:"boolean",nullable:!0,description:"If set to `true`, this emitter will not write any files. It will still validate the TypeSpec sources to ensure they are compatible with Protobuf, but the files will simply not be written to the output directory."},"omit-unreachable-types":{type:"boolean",nullable:!0,description:"By default, the emitter will create `message` declarations for any models in a namespace decorated with `@package` that have an `@field` decorator on every property. If this option is set to true, this behavior will be disabled, and only messages that are explicitly decorated with `@message` or that are reachable from a service operation will be emitted."}},required:[]},PACKAGE_NAME="@typespec/protobuf",TypeSpecProtobufLibrary=createTypeSpecLibrary({name:PACKAGE_NAME,capabilities:{dryRun:!0},requireImports:[PACKAGE_NAME],diagnostics:{"field-index":{severity:"error",messages:{missing:paramMessage`field ${"name"} does not have a field index, but one is required (try using the '@field' decorator)`,invalid:paramMessage`field index ${"index"} is invalid (must be an integer greater than zero)`,"out-of-bounds":paramMessage`field index ${"index"} is out of bounds (must be less than ${"max"})`,reserved:paramMessage`field index ${"index"} falls within the implementation-reserved range of 19000-19999 inclusive`,"user-reserved":paramMessage`field index ${"index"} was reserved by a call to @reserve on this model`,"user-reserved-range":paramMessage`field index ${"index"} falls within a range reserved by a call to @reserve on this model`}},"field-name":{severity:"error",messages:{"user-reserved":paramMessage`field name '${"name"}' was reserved by a call to @reserve on this model`}},"root-operation":{severity:"error",messages:{default:"operations in the root namespace are not supported (no associated Protobuf service)"}},"unsupported-intrinsic":{severity:"error",messages:{default:paramMessage`intrinsic type ${"name"} is not supported in Protobuf`}},"unsupported-return-type":{severity:"error",messages:{default:"Protobuf methods must return a named Model"}},"unsupported-input-type":{severity:"error",messages:{"wrong-number":"Protobuf methods must accept exactly one Model input (an empty model will do)","wrong-type":"Protobuf methods may only accept a named Model as an input",unconvertible:"input parameters cannot be converted to a Protobuf message"}},"unsupported-field-type":{severity:"error",messages:{unconvertible:paramMessage`cannot convert a ${"type"} to a protobuf type (only intrinsic types and models are supported)`,"unknown-intrinsic":paramMessage`no known protobuf scalar for intrinsic type ${"name"}`,"unknown-scalar":paramMessage`no known protobuf scalar for TypeSpec scalar type ${"name"}`,"recursive-map":"a protobuf map's 'value' type may not refer to another map",union:"a message field's type may not be a union"}},"optional-array-field":{severity:"warning",messages:{default:"optional array fields cannot preserve unset versus empty in protobuf; emitting a repeated field without the 'optional' label"}},"optional-map-field":{severity:"warning",messages:{default:"optional map fields cannot preserve unset versus empty in protobuf; emitting a map field without the 'optional' label"}},"namespace-collision":{severity:"error",messages:{default:paramMessage`the package name ${"name"} has already been used`}},"unconvertible-enum":{severity:"error",messages:{default:"enums must explicitly assign exactly one integer to each member to be used in a Protobuf message","no-zero-first":"the first variant of an enum must be set to zero to be used in a Protobuf message"}},"nested-array":{severity:"error",messages:{default:"nested arrays are not supported by the Protobuf emitter"}},"invalid-package-name":{severity:"error",messages:{default:paramMessage`${"name"} is not a valid package name (must consist of letters and numbers separated by ".")`}},"illegal-reservation":{severity:"error",messages:{default:"reservation value must be a string literal, uint32 literal, or a tuple of two uint32 literals denoting a range"}},"model-not-in-package":{severity:"error",messages:{default:paramMessage`model ${"name"} is not in a namespace that uses the '@Protobuf.package' decorator`}},"anonymous-model":{severity:"error",messages:{default:"anonymous models cannot be used in Protobuf messages"}},"unspeakable-template-argument":{severity:"error",messages:{default:paramMessage`template ${"name"} cannot be converted to a Protobuf message because it has an unspeakable argument (try using the '@friendlyName' decorator on the template)`}},package:{severity:"error",messages:{"disallowed-option-type":paramMessage`option '${"name"}' with type '${"type"}' is not allowed in a package declaration (only string, boolean, and numeric types are allowed)`}}},emitter:{options:EmitterOptionsSchema3}}),__DIAGNOSTIC_CACHE=new WeakMap;function getDiagnosticCache(program){let cache=__DIAGNOSTIC_CACHE.get(program);if(!cache)cache=new Map,__DIAGNOSTIC_CACHE.set(program,cache);return cache}function getAppliedCodesForTarget(program,target){let cache=getDiagnosticCache(program),codes=cache.get(target);if(!codes)codes=new Set,cache.set(target,codes);return codes}var reportDiagnostic3=Object.assign(TypeSpecProtobufLibrary.reportDiagnostic,{once:function(program,diagnostic){let codes=getAppliedCodesForTarget(program,diagnostic.target);if(codes.has(diagnostic.code))return;codes.add(diagnostic.code),TypeSpecProtobufLibrary.reportDiagnostic(program,diagnostic)}}),keys=["fieldIndex","package","service","externRef","stream","reserve","message","_map"],state=Object.fromEntries(keys.map((k)=>[k,TypeSpecProtobufLibrary.createStateSymbol(k)]));var MAX_FIELD_INDEX=536870911,IMPLEMENTATION_RESERVED_RANGE=[19000,19999];function $service(ctx,target){ctx.program.stateSet(state.service).add(target)}var $package=(ctx,target,details)=>{ctx.program.stateMap(state.package).set(target,details)};function $_map(ctx,target){ctx.program.stateSet(state._map).add(target)}var $externRef=(ctx,target,path,name)=>{ctx.program.stateMap(state.externRef).set(target,[path.value,name.value])},$stream=(ctx,target,mode)=>{let emitStreamingMode={Duplex:StreamingMode.Duplex,In:StreamingMode.In,Out:StreamingMode.Out,None:StreamingMode.None}[mode.name];ctx.program.stateMap(state.stream).set(target,emitStreamingMode)},$reserve=(ctx,target,...reservations)=>{let finalReservations=reservations.filter((v)=>v!=null);ctx.program.stateMap(state.reserve).set(target,finalReservations)},$message=(ctx,target)=>{ctx.program.stateSet(state.message).add(target)},$field=(ctx,target,fieldIndex)=>{if(!Number.isInteger(fieldIndex)||fieldIndex<=0){reportDiagnostic3(ctx.program,{code:"field-index",messageId:"invalid",format:{index:String(fieldIndex)},target});return}else if(fieldIndex>MAX_FIELD_INDEX){reportDiagnostic3(ctx.program,{code:"field-index",messageId:"out-of-bounds",format:{index:String(fieldIndex),max:String(MAX_FIELD_INDEX+1)},target});return}else if(fieldIndex>=IMPLEMENTATION_RESERVED_RANGE[0]&&fieldIndex<=IMPLEMENTATION_RESERVED_RANGE[1])reportDiagnostic3(ctx.program,{code:"field-index",messageId:"reserved",format:{index:String(fieldIndex)},target});ctx.program.stateMap(state.fieldIndex).set(target,fieldIndex)};var $decorators3={"TypeSpec.Protobuf":{message:$message,field:$field,reserve:$reserve,service:$service,package:$package,stream:$stream},"TypeSpec.Protobuf.Private":{externRef:$externRef,_map:$_map}};var exports_tsp_index7={};__export(exports_tsp_index7,{$lib:()=>$lib3,$decorators:()=>$decorators4});var operationIdStrategySchema={type:"string",enum:["parent-container","fqn","explicit-only"],default:"parent-container",description:["Determines how to generate operation IDs when `@operationId` is not used.","Avaliable options are:"," - `parent-container`: Uses the parent namespace and operation name to generate the ID."," - `fqn`: Uses the fully qualified name of the operation to generate the ID."," - `explicit-only`: Only use explicitly defined operation IDs."].join(`
`)},EmitterOptionsSchema4={type:"object",additionalProperties:!1,properties:{"file-type":{type:["string","array"],nullable:!0,oneOf:[{type:"string",enum:["yaml","json"]},{type:"array",items:{type:"string",enum:["yaml","json"]},uniqueItems:!0,minItems:1}],description:"If the content should be serialized as YAML or JSON. Can be a single value or an array to emit multiple formats. Default 'yaml', if not specified infer from the `output-file` extension"},"output-file":{type:"string",nullable:!0,description:["Name of the output file."," Output file will interpolate the following values:","  - service-name: Name of the service","  - service-name-if-multiple: Name of the service if multiple","  - version: Version of the service if multiple","  - file-type: The file type being emitted (json or yaml). Useful when `file-type` is an array.","",' Default: `{service-name-if-multiple}.{version}.openapi.yaml` or `.json` if `file-type` is `"json"`'," When `file-type` is an array: `{service-name-if-multiple}.{version}.openapi.{file-type}`",""," Example Single service no versioning","  - `openapi.yaml`",""," Example Multiple services no versioning","  - `openapi.Org1.Service1.yaml`","  - `openapi.Org1.Service2.yaml`",""," Example Single service with versioning","  - `openapi.v1.yaml`","  - `openapi.v2.yaml`",""," Example Multiple service with versioning","  - `openapi.Org1.Service1.v1.yaml`","  - `openapi.Org1.Service1.v2.yaml`","  - `openapi.Org1.Service2.v1.0.yaml`","  - `openapi.Org1.Service2.v1.1.yaml`    "].join(`
`)},"openapi-versions":{title:"OpenAPI Versions",type:"array",items:{type:"string",enum:["3.0.0","3.1.0","3.2.0"],nullable:!0,description:"The versions of OpenAPI to emit. Defaults to `[3.0.0]`"},nullable:!0,uniqueItems:!0,minItems:1,default:["3.0.0"]},"new-line":{type:"string",enum:["crlf","lf"],default:"lf",nullable:!0,description:"Set the newline character for emitting files."},"omit-unreachable-types":{type:"boolean",nullable:!0,description:`Omit unreachable types.
By default all types declared under the service namespace will be included. With this flag on only types references in an operation will be emitted.`},"include-x-typespec-name":{type:"string",enum:["inline-only","never"],nullable:!0,default:"never",description:"If the generated openapi types should have the `x-typespec-name` extension set with the name of the TypeSpec type that created it.\nThis extension is meant for debugging and should not be depended on."},"safeint-strategy":{type:"string",enum:["double-int","int64"],nullable:!0,default:"int64",description:["How to handle safeint type. Options are:"," - `double-int`: Will produce `type: integer, format: double-int`"," - `int64`: Will produce `type: integer, format: int64`","","Default: `int64`"].join(`
`)},"seal-object-schemas":{type:"boolean",nullable:!0,default:!1,description:["If true, then for models emitted as object schemas we default `additionalProperties` to false for","OpenAPI 3.0, and `unevaluatedProperties` to false for OpenAPI 3.1, if not explicitly specified elsewhere.","Default: `false`"].join(`
`)},"experimental-parameter-examples":{type:"string",enum:["data","serialized"],nullable:!0,description:["Determines how to emit examples on parameters.","Note: This is an experimental feature and may change in future versions.","See https://spec.openapis.org/oas/v3.0.4.html#style-examples for parameter example serialization rules","See https://github.com/OAI/OpenAPI-Specification/discussions/4622 for discussion on handling parameter examples."].join(`
`)},"operation-id-strategy":{oneOf:[operationIdStrategySchema,{type:"object",properties:{kind:operationIdStrategySchema,separator:{type:"string",nullable:!0,description:"Separator used to join segment in the operation name."}},required:["kind"]}]},"enum-strategy":{type:"string",enum:["default","annotated"],nullable:!0,default:"default",description:["How to emit TypeSpec enums and unions of literals. Options are:"," - `default`: Emit as a single schema using the `enum` keyword."," - `annotated`: Emit as a `oneOf` of `const` subschemas annotated with `title` and `description`","   from each member's/variant's `@summary` and `@doc`. Follows the OpenAPI 3.1.1 annotated enumerations pattern.","   Only supported by OpenAPI 3.1.0 and above; on 3.0.0 the `default` style is used and a warning is reported."].join(`
`)}},required:[]},$lib3=createTypeSpecLibrary({name:"@typespec/openapi3",capabilities:{dryRun:!0},diagnostics:{"oneof-union":{severity:"error",messages:{default:"@oneOf decorator can only be used on a union or a model property which type is a union."}},"inconsistent-shared-route-request-visibility":{severity:"error",messages:{default:"All operations with `@sharedRoutes` must have the same `@requestVisibility`."}},"invalid-server-variable":{severity:"error",docs:fileRef.fromPackageRoot("src/diagnostics/invalid-server-variable.md"),messages:{default:paramMessage`Server variable '${"propName"}' must be assignable to 'string'. It must either be a string, enum of string or union of strings.`}},"invalid-format":{severity:"warning",messages:{default:paramMessage`Collection format '${"value"}' is not supported in OpenAPI3 ${"paramType"} parameters. Defaulting to type 'string'.`}},"invalid-style":{severity:"warning",messages:{default:paramMessage`Style '${"style"}' is not supported in OpenAPI3 ${"paramType"} parameters. Defaulting to style 'simple'.`,optionalPath:paramMessage`Style '${"style"}' is not supported in OpenAPI3 ${"paramType"} parameters. The style ${"style"} could be introduced by an optional parameter. Defaulting to style 'simple'.`}},"path-reserved-expansion":{severity:"warning",messages:{default:"Reserved expansion of path parameter with '+' operator #{allowReserved: true} is not supported in OpenAPI3."}},"resource-namespace":{severity:"error",messages:{default:"Resource goes on namespace"}},"path-query":{severity:"error",docs:fileRef.fromPackageRoot("src/diagnostics/path-query.md"),messages:{default:"OpenAPI does not allow paths containing a query string."}},"duplicate-header":{severity:"error",docs:fileRef.fromPackageRoot("src/diagnostics/duplicate-header.md"),messages:{default:paramMessage`The header ${"header"} is defined across multiple content types`}},"status-code-in-default-response":{severity:"error",messages:{default:"a default response should not have an explicit status code"}},"invalid-schema":{severity:"error",docs:fileRef.fromPackageRoot("src/diagnostics/invalid-schema.md"),messages:{default:paramMessage`Couldn't get schema for type ${"type"}`}},"union-null":{severity:"error",docs:fileRef.fromPackageRoot("src/diagnostics/union-null.md"),messages:{default:"Cannot have a union containing only null types."}},"empty-union":{severity:"error",messages:{default:"Empty unions are not supported for OpenAPI v3 - enums must have at least one value."}},"empty-enum":{severity:"error",messages:{default:"Empty enums are not supported for OpenAPI v3 - enums must have at least one value."}},"enum-unique-type":{severity:"error",messages:{default:"Enums are not supported unless all options are literals of the same type."}},"inline-cycle":{severity:"error",docs:fileRef.fromPackageRoot("src/diagnostics/inline-cycle.md"),messages:{default:paramMessage`Cycle detected in '${"type"}'. Use @friendlyName decorator to assign an OpenAPI definition name and make it non-inline.`}},"unsupported-status-code-range":{severity:"error",messages:{default:paramMessage`Status code range '${"start"} to '${"end"}' is not supported. OpenAPI 3.0 can only represent range 1XX, 2XX, 3XX, 4XX and 5XX. Example: \`@minValue(400) @maxValue(499)\` for 4XX.`}},"invalid-model-property":{severity:"error",messages:{default:paramMessage`'${"type"}' cannot be specified as a model property.`}},"unsupported-auth":{severity:"warning",messages:{default:paramMessage`Authentication "${"authType"}" is not a known authentication by the openapi3 emitter, it will be ignored.`}},"xml-attribute-invalid-property-type":{severity:"warning",messages:{default:paramMessage`XML \`@attribute\` can only be primitive types in the OpenAPI 3 emitter, Property '${"name"}' type will be changed to type: string.`}},"xml-unwrapped-invalid-property-type":{severity:"warning",messages:{default:paramMessage`XML \`@unwrapped\` can only used on array properties or primitive ones in the OpenAPI 3 emitter, Property '${"name"}' will be ignored.`}},"invalid-component-fixed-field-key":{severity:"warning",messages:{default:paramMessage`Invalid key '${"value"}' used in a fixed field of the Component object. Only alphanumerics, dot (.), hyphen (-), and underscore (_) characters are allowed in keys.`}},"streams-not-supported":{severity:"warning",messages:{default:"Streams with itemSchema are only fully supported in OpenAPI 3.2.0 or above. The response will be emitted without itemSchema. Consider using OpenAPI 3.2.0 for full stream support."}},"default-not-supported":{severity:"warning",messages:{default:paramMessage`Default value is not supported in OpenAPI 3.0 ${"message"}`}},"enum-strategy-not-supported":{severity:"warning",messages:{default:"`enum-strategy: annotated` is only supported for OpenAPI 3.1.0 and above. The default enum strategy will be used for OpenAPI 3.0.0."}}},emitter:{options:EmitterOptionsSchema4}}),{createDiagnostic:createDiagnostic2,reportDiagnostic:reportDiagnostic4,createStateSymbol:createStateSymbol2}=$lib3;var refTargetsKey=createStateSymbol2("refs"),$useRef=(context,entity,refUrl)=>{context.program.stateMap(refTargetsKey).set(entity,refUrl)};var oneOfKey=createStateSymbol2("oneOf"),$oneOf2=(context,entity)=>{if(entity.kind==="ModelProperty"&&entity.type.kind!=="Union")reportDiagnostic4(context.program,{code:"oneof-union",target:context.decoratorTarget});context.program.stateMap(oneOfKey).set(entity,!0)};var $decorators4={"TypeSpec.OpenAPI":{useRef:$useRef,oneOf:$oneOf2}};var exports_tsp_index10={};__export(exports_tsp_index10,{$onValidate:()=>$onValidate4,$lib:()=>$lib4,$decorators:()=>$decorators7});var $lib4=createTypeSpecLibrary({name:"@typespec/sse",diagnostics:{"terminal-event-not-in-events":{severity:"error",messages:{default:"A field marked as '@terminalEvent' must be a member of a type decorated with '@TypeSpec.Events.events'."}},"sse-stream-union-not-events":{severity:"error",messages:{default:"SSEStream type parameter must be a union decorated with '@TypeSpec.Events.events'."}}},state:{terminalEvent:{description:"State for the @terminalEvent decorator."}}}),{reportDiagnostic:reportDiagnostic5,createDiagnostic:createDiagnostic3,stateKeys:SSEStateKeys}=$lib4;var[isTerminalEvent,setTerminalEvent]=useStateSet(SSEStateKeys.terminalEvent),$terminalEventDecorator=(context,target)=>{setTerminalEvent(context.program,target)};var $lib5=createTypeSpecLibrary({name:"@typespec/events",diagnostics:{"invalid-content-type-target":{severity:"error",messages:{default:"@contentType can only be specified on the top-level event envelope, or the event payload marked with @data"}},"multiple-event-payloads":{severity:"error",messages:{default:paramMessage`Event payload already applied to ${"dataPath"} but also exists under ${"currentPath"}`,payloadInIndexedModel:paramMessage`Event payload applied from inside a Record or Array at ${"dataPath"}`}}},state:{events:{description:"State for the @events decorator."},contentType:{description:"State for the @contentType decorator."},data:{description:"State for the @data decorator."}}}),{reportDiagnostic:reportDiagnostic6,createDiagnostic:createDiagnostic4,stateKeys:EventsStateKeys}=$lib5;var exports_tsp_index8={};__export(exports_tsp_index8,{$onValidate:()=>$onValidate2,$lib:()=>$lib5,$decorators:()=>$decorators5});var[isEvents,setEvents]=useStateSet(EventsStateKeys.events),$eventsDecorator=(context,target)=>{setEvents(context.program,target)};var[getContentType,setContentType]=useStateMap(EventsStateKeys.contentType),$contentTypeDecorator=(context,target,contentType)=>{setContentType(context.program,target,contentType)};var[isEventData,setEventData]=useStateSet(EventsStateKeys.data),$dataDecorator=(context,target)=>{setEventData(context.program,target)};function validateContentType(program,target){if(!getContentType(program,target)||isEventData(program,target))return;return createDiagnostic4({code:"invalid-content-type-target",target})}function stringifyPropertyPath(path){return path.map((p)=>{switch(p.kind){case"ModelProperty":return p.name;case"Tuple":return"[]";case"Model":return}}).filter(Boolean).join(".")}function getEventDefinition(program,target){let diagnostics=createDiagnosticCollector(),eventType=typeof target.name==="string"?target.name:void 0,envelopeContentType=getContentType(program,target),payloadType,payloadContentType,pathToPayload="",indexerDepth=0,currentPropertyPath=[],typesThatChainToPayload=new Set;navigateType(target.type,{modelProperty(prop){currentPropertyPath.push(prop);let contentTypeDiagnostic=validateContentType(program,prop);if(contentTypeDiagnostic)diagnostics.add(contentTypeDiagnostic);if(typesThatChainToPayload.has(prop.type)){diagnostics.add(createDiagnostic4({code:"multiple-event-payloads",format:{dataPath:pathToPayload,currentPath:stringifyPropertyPath(currentPropertyPath)},target}));return}if(!isEventData(program,prop))return;if(currentPropertyPath.forEach((p)=>{if(p.kind==="ModelProperty")typesThatChainToPayload.add(p.type);else if(p.kind==="Model"||p.kind==="Tuple")typesThatChainToPayload.add(p)}),indexerDepth>0){diagnostics.add(createDiagnostic4({code:"multiple-event-payloads",messageId:"payloadInIndexedModel",format:{dataPath:stringifyPropertyPath(currentPropertyPath.slice(0,-1))},target}));return}if(payloadType){diagnostics.add(createDiagnostic4({code:"multiple-event-payloads",format:{dataPath:pathToPayload,currentPath:stringifyPropertyPath(currentPropertyPath)},target}));return}payloadType=prop.type,payloadContentType=getContentType(program,prop),pathToPayload=stringifyPropertyPath(currentPropertyPath)},exitModelProperty(){currentPropertyPath.pop()},tuple(tuple){currentPropertyPath.push(tuple),tuple.values.forEach((value)=>{if(typesThatChainToPayload.has(value)){diagnostics.add(createDiagnostic4({code:"multiple-event-payloads",format:{dataPath:pathToPayload,currentPath:stringifyPropertyPath(currentPropertyPath)},target}));return}})},exitTuple(){currentPropertyPath.pop()},model(model){if(model.indexer)indexerDepth++;currentPropertyPath.push(model)},exitModel(model){if(model.indexer)indexerDepth--;currentPropertyPath.pop()}},{});let eventDefinition={eventType,root:target,isEventEnvelope:!!payloadType,type:target.type,contentType:envelopeContentType,payloadType:payloadType??target.type,payloadContentType:payloadType?payloadContentType:envelopeContentType};return diagnostics.wrap(eventDefinition)}function getEventDefinitions(program,target){let diagnostics=createDiagnosticCollector(),events=[];return target.variants.forEach((variant)=>{events.push(diagnostics.pipe(getEventDefinition(program,variant)))}),diagnostics.wrap(events)}function $onValidate2(program){checkForInvalidEvents(program)}function checkForInvalidEvents(program){program.stateSet(EventsStateKeys.events).forEach((events)=>{let[,diagnostics]=getEventDefinitions(program,events);program.reportDiagnostics(diagnostics)})}var $decorators5={"TypeSpec.Events":{contentType:$contentTypeDecorator,data:$dataDecorator,events:$eventsDecorator}};var $lib6=createTypeSpecLibrary({name:"@typespec/http",diagnostics:{"http-verb-duplicate":{severity:"error",messages:{default:paramMessage`HTTP verb already applied to ${"entityName"}`}},"missing-uri-param":{severity:"error",messages:{default:paramMessage`Route reference parameter '${"param"}' but wasn't found in operation parameters`}},"incompatible-uri-param":{severity:"error",messages:{default:paramMessage`Parameter '${"param"}' is defined in the uri as a ${"uriKind"} but is annotated as a ${"annotationKind"}.`}},"use-uri-template":{severity:"error",messages:{default:paramMessage`Parameter '${"param"}' is already defined in the uri template. Explode, style and allowReserved property must be defined in the uri template as described by RFC 6570.`}},"double-slash":{severity:"warning",messages:{default:paramMessage`Route will result in duplicate slashes as parameter '${"paramName"}' use path expansion and is prefixed with a /`,optionalUnset:paramMessage`Route will result in duplicate slashes when optional parameter '${"paramName"}' is not set.`,optionalSet:paramMessage`Route will result in duplicate slashes when optional parameter '${"paramName"}' is set.`}},"missing-server-param":{severity:"error",messages:{default:paramMessage`Server url contains parameter '${"param"}' but wasn't found in given parameters`}},"duplicate-body":{severity:"error",messages:{default:"Operation has multiple @body parameters declared",duplicateUnannotated:"Operation has multiple unannotated parameters. There can only be one representing the body",bodyAndUnannotated:"Operation has a @body and an unannotated parameter. There can only be one representing the body"}},"duplicate-route-decorator":{severity:"error",messages:{namespace:"@route was defined twice on this namespace and has different values."}},"operation-param-duplicate-type":{severity:"error",messages:{default:paramMessage`Param ${"paramName"} has multiple types: [${"types"}]`}},"duplicate-operation":{severity:"error",messages:{default:paramMessage`Duplicate operation "${"operationName"}" routed at "${"verb"} ${"path"}".`}},"multiple-status-codes":{severity:"error",messages:{default:"Multiple `@statusCode` decorators defined for this operation response."}},"status-code-invalid":{severity:"error",messages:{default:"statusCode value must be a numeric or string literal or union of numeric or string literals",value:"statusCode value must be a three digit code between 100 and 599"}},"content-type-string":{severity:"error",messages:{default:"contentType parameter must be a string literal or union of string literals"}},"content-type-ignored":{severity:"warning",messages:{default:"`Content-Type` header ignored because there is no body."}},"metadata-ignored":{severity:"warning",messages:{default:paramMessage`${"kind"} property will be ignored as it is inside of a @body property. Use @bodyRoot instead if wanting to mix.`}},"response-cookie-not-supported":{severity:"warning",messages:{default:paramMessage`@cookie on response is not supported. Property '${"propName"}' will be ignored in the body. If you need 'Set-Cookie', use @header instead.`}},"no-service-found":{severity:"warning",messages:{default:paramMessage`No namespace with '@service' was found, but Namespace '${"namespace"}' contains routes. Did you mean to annotate this with '@service'?`}},"invalid-type-for-auth":{severity:"error",messages:{default:paramMessage`@useAuth ${"kind"} only accept Auth model, Tuple of auth model or union of auth model.`}},"shared-inconsistency":{severity:"error",messages:{default:paramMessage`Each operation routed at "${"verb"} ${"path"}" needs to have the @sharedRoute decorator.`}},"multipart-invalid-content-type":{severity:"error",messages:{default:paramMessage`Content type '${"contentType"}' is not a multipart content type. Supported content types are: ${"supportedContentTypes"}.`}},"multipart-model":{severity:"error",messages:{default:"Multipart request body must be a model or a tuple of http parts."}},"no-implicit-multipart":{severity:"error",messages:{default:"Using multipart payloads requires the use of @multipartBody and HttpPart<T> models."}},"multipart-part":{severity:"error",messages:{default:"Expect item to be an HttpPart model."}},"multipart-nested":{severity:"error",messages:{default:"Cannot use @multipartBody inside of an HttpPart"}},"http-file-extra-property":{severity:"error",messages:{default:paramMessage`File model cannot define extra properties. Found '${"propName"}'.`}},"http-file-disallowed-metadata":{severity:"error",messages:{default:paramMessage`File model cannot define HTTP metadata type '${"metadataType"}' on property '${"propName"}'.`}},"formdata-no-part-name":{severity:"error",messages:{default:"Part used in multipart/form-data must have a name."}},"http-file-structured":{severity:"warning",messages:{default:paramMessage`HTTP File body is serialized as a structured model in '${"contentTypes"}' instead of being treated as the contents of a file because an explicit Content-Type header is defined. Override the \`contentType\` property of the file model to declare the internal media type of the file's contents, or suppress this warning if you intend to serialize the File as a model.`,union:"An HTTP File in a union is serialized as a structured model instead of being treated as the contents of a file. Declare a separate operation using `@sharedRoute` that has only the File model as the body type to treat it as a file, or suppress this warning if you intend to serialize the File as a model."}},"http-file-content-type-not-string":{severity:"error",messages:{default:paramMessage`The 'contentType' property of the file model must be 'TypeSpec.string', a string literal, or a union of string literals. Found '${"type"}'.`}},"http-file-contents-not-scalar":{severity:"error",messages:{default:paramMessage`The 'contents' property of the file model must be a scalar type that extends 'string' or 'bytes'. Found '${"type"}'.`}},"deprecated-implicit-optionality":{severity:"warning",messages:{default:"The implicitOptionality option is deprecated. To preserve previous behavior, use an explicit patch model with optional properties. For actual merge-patch semantics, use MergePatchUpdate<T> for the @body type."}},"merge-patch-contains-null":{severity:"error",messages:{default:"Cannot convert model to a merge-patch compatible shape because it contains the 'null' intrinsic type."}},"merge-patch-content-type":{severity:"warning",messages:{default:paramMessage`The content-type of a request using a merge-patch template should be 'application/merge-patch+json' detected a header with content-type '${"contentType"}'.`}},"merge-patch-contains-metadata":{severity:"error",messages:{default:paramMessage`The MergePatch transform does not operate on http envelope metadata.  Remove any http metadata decorators ('@query', '@header', '@path', '@cookie', '@statusCode') from the model passed to the MergePatch template. Found '${"metadataType"}' decorating property '${"propertyName"}'`}}},state:{authentication:{description:"State for the @auth decorator"},header:{description:"State for the @header decorator"},cookie:{description:"State for the @cookie decorator"},query:{description:"State for the @query decorator"},path:{description:"State for the @path decorator"},body:{description:"State for the @body decorator"},bodyRoot:{description:"State for the @bodyRoot decorator"},bodyIgnore:{description:"State for the @bodyIgnore decorator"},multipartBody:{description:"State for the @bodyIgnore decorator"},statusCode:{description:"State for the @statusCode decorator"},verbs:{description:"State for the verb decorators (@get, @post, @put, etc.)"},patchOptions:{description:"State for the options of the @patch decorator"},servers:{description:"State for the @server decorator"},includeInapplicableMetadataInPayload:{description:"State for the @includeInapplicableMetadataInPayload decorator"},externalInterfaces:{},routeProducer:{},routes:{},sharedRoutes:{description:"State for the @sharedRoute decorator"},routeOptions:{},file:{description:"State for the @Private.file decorator"},httpPart:{description:"State for the @Private.httpPart decorator"},mergePatchModel:{description:"State marking mergePatch models "},mergePatchProperty:{description:"State marking merge path model property source"},mergePatchPropertyOptions:{description:"Override options for a property in a merge patch transform"}}}),{reportDiagnostic:reportDiagnostic7,createDiagnostic:createDiagnostic5,stateKeys:HttpStateKeys}=$lib6;function error(target){return[[],[createDiagnostic5({code:"status-code-invalid",target,messageId:"value"})]]}function validateStatusCode(code,diagnosticTarget){let codeAsNumber=typeof code==="string"?parseInt(code,10):code;if(isNaN(codeAsNumber))return error(diagnosticTarget);if(!Number.isInteger(codeAsNumber))return error(diagnosticTarget);if(codeAsNumber<100||codeAsNumber>599)return error(diagnosticTarget);return[[codeAsNumber],[]]}function getStatusCodesFromType(program,type,diagnosticTarget){switch(type.kind){case"String":case"Number":return validateStatusCode(type.value,diagnosticTarget);case"Union":let diagnostics=createDiagnosticCollector(),statusCodes=[...type.variants.values()].flatMap((variant)=>{return diagnostics.pipe(getStatusCodesFromType(program,variant.type,diagnosticTarget))});return diagnostics.wrap(statusCodes);case"Scalar":return validateStatusCodeRange(program,type,type,diagnosticTarget);case"ModelProperty":if(type.type.kind==="Scalar")return validateStatusCodeRange(program,type,type.type,diagnosticTarget);else return getStatusCodesFromType(program,type.type,diagnosticTarget);default:return error(diagnosticTarget)}}function validateStatusCodeRange(program,type,scalar2,diagnosticTarget){if(!isInt32(program,scalar2))return error(diagnosticTarget);let range=getStatusCodesRange(program,type,diagnosticTarget);if(isRangeComplete(range))return[[range],[]];else return error(diagnosticTarget)}function isRangeComplete(range){return range.start!==void 0&&range.end!==void 0}function getStatusCodesRange(program,type,diagnosticTarget){let start=getMinValue(program,type),end=getMaxValue(program,type),baseRange={};if(type.kind==="ModelProperty"&&(type.type.kind==="Scalar"||type.type.kind==="ModelProperty"))baseRange=getStatusCodesRange(program,type.type,diagnosticTarget);else if(type.kind==="Scalar"&&type.baseScalar)baseRange=getStatusCodesRange(program,type.baseScalar,diagnosticTarget);return{...baseRange,start,end}}function isInt32(program,type){let tk=$(program);return tk.type.isAssignableTo(type,tk.builtin.int32,type)}function extractParamsFromPath(path){return path.match(/\{[^}]+\}/g)?.map((s)=>s.slice(1,-1))??[]}var $header=(context,entity,headerNameOrOptions)=>{let options={type:"header",name:entity.name.replace(/([a-z])([A-Z])/g,"$1-$2").toLowerCase()};if(headerNameOrOptions)if(typeof headerNameOrOptions==="string")options.name=headerNameOrOptions;else{let name=headerNameOrOptions.name;if(name)options.name=name;if(headerNameOrOptions.explode)options.explode=!0}context.program.stateMap(HttpStateKeys.header).set(entity,options)};function getHeaderFieldOptions(program,entity){return program.stateMap(HttpStateKeys.header).get(entity)}function isHeader(program,entity){return program.stateMap(HttpStateKeys.header).has(entity)}var $cookie=(context,entity,cookieNameOrOptions)=>{let options={type:"cookie",name:typeof cookieNameOrOptions==="string"?cookieNameOrOptions:cookieNameOrOptions?.name??entity.name.replace(/([a-z])([A-Z])/g,"$1_$2").toLowerCase()};context.program.stateMap(HttpStateKeys.cookie).set(entity,options)};function getCookieParamOptions(program,entity){return program.stateMap(HttpStateKeys.cookie).get(entity)}function isCookieParam(program,entity){return program.stateMap(HttpStateKeys.cookie).has(entity)}var[getQueryOptions,setQueryOptions]=useStateMap(HttpStateKeys.query),$query2=(context,entity,queryNameOrOptions)=>{let paramName=typeof queryNameOrOptions==="string"?queryNameOrOptions:queryNameOrOptions?.name??entity.name,userOptions=typeof queryNameOrOptions==="object"?queryNameOrOptions:{};setQueryOptions(context.program,entity,{explode:userOptions.explode,name:paramName})};function resolveQueryOptionsWithDefaults(options){return{explode:options.explode??!1,name:options.name}}function isQueryParam(program,entity){return program.stateMap(HttpStateKeys.query).has(entity)}var[getPathOptions,setPathOptions]=useStateMap(HttpStateKeys.path),$path=(context,entity,paramNameOrOptions)=>{let paramName=typeof paramNameOrOptions==="string"?paramNameOrOptions:paramNameOrOptions?.name??entity.name,userOptions=typeof paramNameOrOptions==="object"?paramNameOrOptions:{};setPathOptions(context.program,entity,{explode:userOptions.explode,allowReserved:userOptions.allowReserved,style:userOptions.style,name:paramName})};function resolvePathOptionsWithDefaults(options){return{explode:options.explode??!1,allowReserved:options.allowReserved??!1,style:options.style??"simple",name:options.name}}function isPathParam(program,entity){return program.stateMap(HttpStateKeys.path).has(entity)}var $body=(context,entity)=>{context.program.stateSet(HttpStateKeys.body).add(entity)},$bodyRoot=(context,entity)=>{context.program.stateSet(HttpStateKeys.bodyRoot).add(entity)},$bodyIgnore=(context,entity)=>{context.program.stateSet(HttpStateKeys.bodyIgnore).add(entity)};function isBody(program,entity){return program.stateSet(HttpStateKeys.body).has(entity)}function isBodyRoot(program,entity){return program.stateSet(HttpStateKeys.bodyRoot).has(entity)}function isBodyIgnore(program,entity){return program.stateSet(HttpStateKeys.bodyIgnore).has(entity)}var $multipartBody=(context,entity)=>{context.program.stateSet(HttpStateKeys.multipartBody).add(entity)};function isMultipartBodyProperty(program,entity){return program.stateSet(HttpStateKeys.multipartBody).has(entity)}var $statusCode=(context,entity)=>{context.program.stateSet(HttpStateKeys.statusCode).add(entity)};function setStatusCode(program,entity,codes){program.stateMap(HttpStateKeys.statusCode).set(entity,codes)}function isStatusCode(program,entity){return program.stateSet(HttpStateKeys.statusCode).has(entity)}function getStatusCodesWithDiagnostics(program,type){return getStatusCodesFromType(program,type,type)}function getStatusCodeDescription(statusCode){if(typeof statusCode==="object")return rangeDescription(statusCode.start,statusCode.end);let statusCodeNumber=typeof statusCode==="string"?parseInt(statusCode,10):statusCode;switch(statusCodeNumber){case 200:return"The request has succeeded.";case 201:return"The request has succeeded and a new resource has been created as a result.";case 202:return"The request has been accepted for processing, but processing has not yet completed.";case 204:return"There is no content to send for this request, but the headers may be useful. ";case 301:return"The URL of the requested resource has been changed permanently. The new URL is given in the response.";case 304:return"The client has made a conditional request and the resource has not been modified.";case 400:return"The server could not understand the request due to invalid syntax.";case 401:return"Access is unauthorized.";case 403:return"Access is forbidden.";case 404:return"The server cannot find the requested resource.";case 409:return"The request conflicts with the current state of the server.";case 412:return"Precondition failed.";case 503:return"Service unavailable."}return rangeDescription(statusCodeNumber,statusCodeNumber)}function rangeDescription(start,end){if(start>=100&&end<=199)return"Informational";else if(start>=200&&end<=299)return"Successful";else if(start>=300&&end<=399)return"Redirection";else if(start>=400&&end<=499)return"Client error";else if(start>=500&&end<=599)return"Server error";return}function setOperationVerb(context,entity,verb){validateVerbUniqueOnNode(context,entity),context.program.stateMap(HttpStateKeys.verbs).set(entity,verb)}function validateVerbUniqueOnNode(context,type){if(type.decorators.filter((x)=>VERB_DECORATORS.includes(x.decorator)&&x.node?.kind===SyntaxKind.DecoratorExpression&&x.node?.parent===type.node).length>1)return reportDiagnostic7(context.program,{code:"http-verb-duplicate",format:{entityName:type.name},target:context.decoratorTarget}),!1;return!0}function getOperationVerb(program,entity){return program.stateMap(HttpStateKeys.verbs).get(entity)}function createVerbDecorator(verb){return(context,entity)=>{setOperationVerb(context,entity,verb)}}var $get=createVerbDecorator("get"),$put=createVerbDecorator("put"),$post=createVerbDecorator("post"),$delete=createVerbDecorator("delete"),$head=createVerbDecorator("head"),_patch=createVerbDecorator("patch"),[_getPatchOptions,setPatchOptions]=useStateMap(HttpStateKeys.patchOptions);function isOriginalDecoratorApplication(context,entity){let decoratorNode=context.decoratorTarget;if(entity.interface!==void 0&&entity.node!==void 0&&entity.node.parent!==entity.interface.node)return!1;if(entity.sourceOperation!==void 0)return!1;if(decoratorNode?.kind===SyntaxKind.DecoratorExpression&&decoratorNode.parent){if(decoratorNode.parent!==entity.node)return!1}return!0}var $patch=(context,entity,options)=>{if(_patch(context,entity),options){if(options.implicitOptionality===!0){if(isOriginalDecoratorApplication(context,entity))reportDiagnostic7(context.program,{code:"deprecated-implicit-optionality",target:entity})}setPatchOptions(context.program,entity,options)}};function getPatchOptions(program,operation){return _getPatchOptions(program,operation)}var VERB_DECORATORS=[$get,$head,$post,$put,$patch,$delete],$server=(context,target,url,description,parameters)=>{let params=extractParamsFromPath(url),parameterMap=new Map(parameters?.properties??[]);for(let declaredParam of params)if(!parameterMap.get(declaredParam))reportDiagnostic7(context.program,{code:"missing-server-param",format:{param:declaredParam},target:context.getArgumentTarget(0)}),parameterMap.delete(declaredParam);let servers=context.program.stateMap(HttpStateKeys.servers).get(target);if(servers===void 0)servers=[],context.program.stateMap(HttpStateKeys.servers).set(target,servers);servers.push({url,description,parameters:parameterMap})};function $useAuth(context,entity,authConfig){validateDecoratorUniqueOnNode(context,entity,$useAuth);let[auth,diagnostics]=extractAuthentication(context.program,authConfig);if(diagnostics.length>0)context.program.reportDiagnostics(diagnostics);if(auth!==void 0)setAuthentication(context.program,entity,auth)}function setAuthentication(program,entity,auth){program.stateMap(HttpStateKeys.authentication).set(entity,auth)}function extractAuthentication(program,type){let diagnostics=createDiagnosticCollector();switch(type.kind){case"Model":let auth=diagnostics.pipe(extractHttpAuthentication(program,type,type));if(auth===void 0)return diagnostics.wrap(void 0);return diagnostics.wrap({options:[{schemes:[auth]}]});case"Tuple":let option=diagnostics.pipe(extractHttpAuthenticationOption(program,type,type));return diagnostics.wrap({options:[option]});case"Union":return extractHttpAuthenticationOptions(program,type,type);default:return[void 0,[createDiagnostic5({code:"invalid-type-for-auth",format:{kind:type.kind},target:type})]]}}function extractHttpAuthenticationOptions(program,tuple,diagnosticTarget){let options=[],diagnostics=createDiagnosticCollector();for(let variant of tuple.variants.values()){let value=variant.type;switch(value.kind){case"Model":let result=diagnostics.pipe(extractHttpAuthentication(program,value,diagnosticTarget));if(result!==void 0)options.push({schemes:[result]});break;case"Tuple":let option=diagnostics.pipe(extractHttpAuthenticationOption(program,value,diagnosticTarget));options.push(option);break;default:diagnostics.add(createDiagnostic5({code:"invalid-type-for-auth",format:{kind:value.kind},target:value}))}}return diagnostics.wrap({options})}function extractHttpAuthenticationOption(program,tuple,diagnosticTarget){let schemes=[],diagnostics=createDiagnosticCollector();for(let value of tuple.values)switch(value.kind){case"Model":let result=diagnostics.pipe(extractHttpAuthentication(program,value,diagnosticTarget));if(result!==void 0)schemes.push(result);break;default:diagnostics.add(createDiagnostic5({code:"invalid-type-for-auth",format:{kind:value.kind},target:value}))}return diagnostics.wrap({schemes})}function extractHttpAuthentication(program,modelType,diagnosticTarget){let[result,diagnostics]=typespecTypeToJson(modelType,diagnosticTarget);if(result===void 0)return[result,diagnostics];let description=getDoc(program,modelType);return[{...result.type==="oauth2"?extractOAuth2Auth(modelType,result):{...result,...result.type==="openIdConnect"&&{scopes:Array.isArray(result.scopes)?result.scopes:[]},model:modelType},id:modelType.name||result.type,...description&&{description}},diagnostics]}function extractOAuth2Auth(modelType,data){let flows=Array.isArray(data.flows)&&data.flows.every((x)=>typeof x==="object")?data.flows:[],defaultScopes=Array.isArray(data.defaultScopes)?data.defaultScopes:[];return{id:data.id,type:data.type,model:modelType,flows:flows.map((flow)=>{let scopes=flow.scopes?flow.scopes:defaultScopes;return{...flow,scopes:scopes.map((x)=>({value:x}))}})}}function getAuthentication(program,entity){return program.stateMap(HttpStateKeys.authentication).get(entity)}function getAuthenticationForOperation(program,operation){let operationAuth=getAuthentication(program,operation);if(operationAuth)return operationAuth;if(operation.interface!==void 0){let interfaceAuth=getAuthentication(program,operation.interface);if(interfaceAuth)return interfaceAuth}let namespace=operation.namespace;while(namespace){let namespaceAuth=getAuthentication(program,namespace);if(namespaceAuth)return namespaceAuth;namespace=namespace.namespace}return}function getContentTypes(property){let diagnostics=createDiagnosticCollector();if(property.type.kind==="String")return[[property.type.value],[]];else if(property.type.kind==="Union"){let contentTypes=[];for(let option of property.type.variants.values())if(option.type.kind==="String")contentTypes.push(option.type.value);else{diagnostics.add(createDiagnostic5({code:"content-type-string",target:property}));continue}return diagnostics.wrap(contentTypes)}else if(property.type.kind==="Scalar"&&property.type.name==="string")return[["*/*"],[]];return[[],[createDiagnostic5({code:"content-type-string",target:property})]]}var[getSharedRoute,setSharedRouteFor]=useStateMap(HttpStateKeys.sharedRoutes);function setSharedRoute(program,operation){setSharedRouteFor(program,operation,!0)}function isSharedRoute(program,operation){return getSharedRoute(program,operation)===!0}var $sharedRoute=(context,entity)=>{setSharedRoute(context.program,entity)};var[getRouteState,setRouteState]=useStateMap(HttpStateKeys.routes),$route=(context,entity,path)=>{validateDecoratorUniqueOnNode(context,entity,$route),setRoute(context,entity,{path,shared:!1})};function setRoute(context,entity,details){let existingPath=getRouteState(context.program,entity);if(existingPath&&entity.kind==="Namespace"){if(existingPath!==details.path)reportDiagnostic7(context.program,{code:"duplicate-route-decorator",messageId:"namespace",target:entity})}else if(setRouteState(context.program,entity,details.path),entity.kind==="Operation"&&details.shared)setSharedRoute(context.program,entity)}var $plainData=(context,entity)=>{let{program}=context,decoratorsToRemove=["$header","$body","$query","$path","$statusCode"],[headers,bodies,queries,paths,statusCodes]=[program.stateMap(HttpStateKeys.header),program.stateSet(HttpStateKeys.body),program.stateMap(HttpStateKeys.query),program.stateMap(HttpStateKeys.path),program.stateMap(HttpStateKeys.statusCode)];for(let property of entity.properties.values())property.decorators=property.decorators.filter((d)=>!decoratorsToRemove.includes(d.decorator.name)),headers.delete(property),bodies.delete(property),queries.delete(property),paths.delete(property),statusCodes.delete(property)};function getFileTemplateMetadata(target){if(target.sourceModels.length)return;let templateMapper=target.templateMapper;if(!templateMapper||!templateMapper.args)return;let[contentTypeArg,contentsArg]=templateMapper.args;if(!contentTypeArg||!contentsArg)return;return{contentTypeArg,contentsArg,templateMapper:target.templateMapper}}function getFileDiagFormatName(t){if(t?.entityKind==="Type")return getTypeName(t,{printable:!0});else if(t?.type.kind==="String")return`"${t.type.value}"`;return}var $httpFile=(context,target)=>{let aliasModel=target.sourceModels.length>0?target:void 0,templateMetadata=getFileTemplateMetadata(target),templateMapper=templateMetadata?.templateMapper,contentType=target.properties.get("contentType").type;if(!(contentType.kind==="String"||context.program.checker.isStdType(contentType,"string")||contentType.kind==="Union"&&[...contentType.variants.values()].every((v)=>v.type.kind==="String"))){let contentTypeDiagnosticTarget=getTemplateArgumentTarget(templateMapper?.source,"ContentType",0)??templateMetadata?.contentTypeArg,contentTypeArg=templateMetadata?.contentTypeArg,typeName=getFileDiagFormatName(contentTypeArg)??getFileDiagFormatName(contentType)??"<unknown>";reportDiagnostic7(context.program,{code:"http-file-content-type-not-string",format:{type:typeName},target:contentTypeDiagnosticTarget??aliasModel??NoTarget})}let contents=target.properties.get("contents").type;if(contents.kind!=="Scalar"){let contentsArg=templateMetadata?.contentsArg,contentsDiagnosticTarget=getTemplateArgumentTarget(templateMapper?.source,"Contents",1)??contentsArg,typeName=getFileDiagFormatName(contentsArg)??getFileDiagFormatName(contents)??"<unknown>";reportDiagnostic7(context.program,{code:"http-file-contents-not-scalar",format:{type:typeName},target:contentsDiagnosticTarget??aliasModel??NoTarget})}context.program.stateSet(HttpStateKeys.file).add(target);function getTemplateArgumentTarget(source,name,argumentPosition){if(source?.node.kind===SyntaxKind.TypeReference){let argument=source.node.arguments.find((n)=>n.name?.sv===name);if(argument)return argument;let position=target.templateNode?.templateParameters.map((n,idx)=>[idx,n]).find(([_2,n])=>n.id.sv)?.[0]??argumentPosition;if(position===void 0)return;return source.node.arguments[position]}return}return{onGraphFinish:()=>{return validateHttpFileModel(context.program,target),[]}}};function validateHttpFileModel(program,model){for(let prop of model.properties.values())switch(prop.name){case"contentType":case"contents":{let annotations={header:getHeaderFieldOptions(program,prop),cookie:getCookieParamOptions(program,prop),query:getQueryOptions(program,prop),path:getPathOptions(program,prop),body:isBody(program,prop),bodyRoot:isBodyRoot(program,prop),multipartBody:isMultipartBodyProperty(program,prop),statusCode:isStatusCode(program,prop)};reportDisallowed(prop,annotations);break}case"filename":{let annotations={body:isBody(program,prop),bodyRoot:isBodyRoot(program,prop),multipartBody:isMultipartBodyProperty(program,prop),statusCode:isStatusCode(program,prop),cookie:getCookieParamOptions(program,prop)};reportDisallowed(prop,annotations);break}default:reportDiagnostic7(program,{code:"http-file-extra-property",format:{propName:prop.name},target:prop})}for(let child of model.derivedModels)validateHttpFileModel(program,child);function reportDisallowed(target,annotations){let metadataEntries=Object.entries(annotations).filter((e)=>!!e[1]);for(let[metadataType]of metadataEntries)reportDiagnostic7(program,{code:"http-file-disallowed-metadata",format:{propName:target.name,metadataType},target})}}function isHttpFile(program,type){return program.stateSet(HttpStateKeys.file).has(type)}function isOrExtendsHttpFile(program,type){if(type.kind!=="Model")return!1;let current=type;while(current){if(isHttpFile(program,current))return!0;current=current.baseModel}return!1}function getHttpFileModel(program,type,filter){if(type.kind!=="Model")return;let contentType=getProperty(type,"contentType"),filename=getProperty(type,"filename"),contents=getProperty(type,"contents");if(!contentType||!filename||!contents)return;if(!propertyIsFromHttpFile(program,contentType)||!propertyIsFromHttpFile(program,filename)||!propertyIsFromHttpFile(program,contents))return;let effectiveProperties=new Set(walkPropertiesInherited(type));if(filter){for(let prop of effectiveProperties)if(!filter(prop))effectiveProperties.delete(prop)}if(effectiveProperties.size!==3)return;return{contents,contentType,filename,type}}function propertyIsFromHttpFile(program,property){if(property.model?isOrExtendsHttpFile(program,property.model):!1)return!0;if(property.sourceProperty)return propertyIsFromHttpFile(program,property.sourceProperty);return!1}var $httpPart=(context,target,type,options)=>{context.program.stateMap(HttpStateKeys.httpPart).set(target,{type,options})};function getHttpPart(program,target){return program.stateMap(HttpStateKeys.httpPart).get(target)}function $includeInapplicableMetadataInPayload(context,entity,value){context.program.stateMap(HttpStateKeys.includeInapplicableMetadataInPayload).set(entity,value)}var Visibility;(function(Visibility2){Visibility2[Visibility2.Read=1]="Read",Visibility2[Visibility2.Create=2]="Create",Visibility2[Visibility2.Update=4]="Update",Visibility2[Visibility2.Delete=8]="Delete",Visibility2[Visibility2.Query=16]="Query",Visibility2[Visibility2.None=0]="None",Visibility2[Visibility2.All=31]="All",Visibility2[Visibility2.Item=1048576]="Item",Visibility2[Visibility2.Patch=2097152]="Patch",Visibility2[Visibility2.Synthetic=3145728]="Synthetic"})(Visibility||(Visibility={}));function filterToVisibility(program,filter){let Lifecycle=getLifecycleVisibilityEnum(program);if(compilerAssert(!filter.all,"Unexpected: `all` constraint in visibility filter passed to filterToVisibility"),compilerAssert(!filter.none,"Unexpected: `none` constraint in visibility filter passed to filterToVisibility"),!filter.any)return Visibility.All;else{let visibility=Visibility.None;for(let modifierConstraint of filter.any??[]){if(modifierConstraint.enum!==Lifecycle)continue;switch(modifierConstraint.name){case"Read":visibility|=Visibility.Read;break;case"Create":visibility|=Visibility.Create;break;case"Update":visibility|=Visibility.Update;break;case"Delete":visibility|=Visibility.Delete;break;case"Query":visibility|=Visibility.Query;break;default:compilerAssert(!1,`Unreachable: unrecognized Lifecycle visibility member: '${modifierConstraint.name}'`)}}return visibility}}var VISIBILITY_FILTER_CACHE_MAP=new WeakMap;function getVisibilityFilterCache(program){let cache=VISIBILITY_FILTER_CACHE_MAP.get(program);if(!cache)cache=new Map,VISIBILITY_FILTER_CACHE_MAP.set(program,cache);return cache}function visibilityToFilter(program,visibility){if(visibility&=~Visibility.Synthetic,visibility===Visibility.All)return{};let cache=getVisibilityFilterCache(program),filter=cache.get(visibility);if(!filter){let LifecycleEnum=getLifecycleVisibilityEnum(program),Lifecycle={Create:LifecycleEnum.members.get("Create"),Read:LifecycleEnum.members.get("Read"),Update:LifecycleEnum.members.get("Update"),Delete:LifecycleEnum.members.get("Delete"),Query:LifecycleEnum.members.get("Query")},any=new Set;if(visibility&Visibility.Read)any.add(Lifecycle.Read);if(visibility&Visibility.Create)any.add(Lifecycle.Create);if(visibility&Visibility.Update)any.add(Lifecycle.Update);if(visibility&Visibility.Delete)any.add(Lifecycle.Delete);if(visibility&Visibility.Query)any.add(Lifecycle.Query);compilerAssert(any.size>0||visibility===Visibility.None,"invalid visibility"),filter={any},cache.set(visibility,filter)}return filter}function getDefaultVisibilityForVerb(verb){switch(verb){case"get":case"head":return Visibility.Query;case"post":return Visibility.Create;case"put":return Visibility.Create|Visibility.Update;case"patch":return Visibility.Update;case"delete":return Visibility.Delete;default:compilerAssert(!1,`Unreachable: unrecognized HTTP verb: '${verb}'`)}}function HttpVisibilityProvider(verbOrParameterOptions){let hasVerb=typeof verbOrParameterOptions==="string";return{parameters:(program,operation)=>{let verb=hasVerb?verbOrParameterOptions:verbOrParameterOptions?.verbSelector?.(program,operation)??getOperationVerb(program,operation);if(!verb){let[{parameters}]=resolvePathAndParameters(program,operation,void 0,{});verb=parameters.verb}return visibilityToFilter(program,getDefaultVisibilityForVerb(verb))},returnType:(program,_2)=>{let Read=getLifecycleVisibilityEnum(program).members.get("Read");return{any:new Set([Read])}}}}function resolveRequestVisibility(program,operation,verb){let parameterVisibilityFilter=getParameterVisibilityFilter(program,operation,HttpVisibilityProvider(verb)),visibility=filterToVisibility(program,parameterVisibilityFilter);if(verb==="patch"){if(getPatchOptions(program,operation)?.implicitOptionality)visibility|=Visibility.Patch}return visibility}function isMetadata(program,property){return isHeader(program,property)||isCookieParam(program,property)||isQueryParam(program,property)||isPathParam(program,property)||isStatusCode(program,property)}function isVisible2(program,property,visibility){return isVisible(program,property,visibilityToFilter(program,visibility))}var[getMergePatchSource,setMergePatchSource]=useStateMap(HttpStateKeys.mergePatchModel),[getMergePatchPropertySource,setMergePatchPropertySource]=useStateMap(HttpStateKeys.mergePatchProperty),[getMergePatchPropertyOverrides,setMergePatchPropertyOverrides]=useStateMap(HttpStateKeys.mergePatchPropertyOptions);function isMergePatch(program,model){return getMergePatchSource(program,model)!==void 0}function isMergePatchBody(program,bodyType){let _visitedTypes=new WeakMap;function isMergePatchModel(type){if(_visitedTypes.has(type))return!1;if(_visitedTypes.set(bodyType,!0),!$(program).model.is(type))return!1;if(isMergePatch(program,type))return!0;if($(program).array.is(type)||$(program).record.is(type))return isMergePatchModel(type.indexer.value);return!1}switch(bodyType.kind){case"Model":return isMergePatchModel(bodyType)||bodyType.sourceModels.some((m)=>isMergePatchModel(m.model))||[...bodyType.properties.values()].some((p)=>getMergePatchPropertySource(program,p)!==void 0||isMergePatchModel(p.type));case"ModelProperty":return isMergePatchModel(bodyType.type);case"Union":return[...bodyType.variants.values()].some((v)=>isMergePatchModel(v.type));case"UnionVariant":return isMergePatchModel(bodyType.type);case"Tuple":return bodyType.values.some((v)=>isMergePatchModel(v));default:return!1}}function getHttpProperty(program,property,path,options={}){let diagnostics=[];function createResult(opts){return[{...opts,property,path},diagnostics]}let annotations={header:getHeaderFieldOptions(program,property),cookie:getCookieParamOptions(program,property),query:getQueryOptions(program,property),path:getPathOptions(program,property),body:isBody(program,property),bodyRoot:isBodyRoot(program,property),multipartBody:isMultipartBodyProperty(program,property),statusCode:isStatusCode(program,property)},defined=Object.entries(annotations).filter((x)=>!!x[1]),implicit=options.implicitParameter?.(property);if(implicit&&defined.length>0)if(implicit.type==="path"&&annotations.path){if(annotations.path.explode!==void 0||annotations.path.style!==void 0||annotations.path.allowReserved!==void 0)diagnostics.push(createDiagnostic5({code:"use-uri-template",format:{param:property.name},target:property}))}else if(implicit.type==="query"&&annotations.query){if(annotations.query.explode!==void 0)diagnostics.push(createDiagnostic5({code:"use-uri-template",format:{param:property.name},target:property}))}else diagnostics.push(createDiagnostic5({code:"incompatible-uri-param",format:{param:property.name,uriKind:implicit.type,annotationKind:defined[0][0]},target:property}));if(implicit)return createResult({kind:implicit.type,options:implicit,property});if(defined.length===0)return isBodyIgnore(program,property)?createResult({kind:"bodyIgnore"}):createResult({kind:"bodyProperty"});else if(defined.length>1)diagnostics.push(createDiagnostic5({code:"operation-param-duplicate-type",format:{paramName:property.name,types:defined.map((x)=>x[0]).join(", ")},target:property}));if(annotations.header){if(annotations.header.name.toLowerCase()==="content-type"&&!options.treatContentTypeAsHeader)return createResult({kind:"contentType"});return createResult({kind:"header",options:annotations.header})}else if(annotations.cookie)return createResult({kind:"cookie",options:annotations.cookie});else if(annotations.query)return createResult({kind:"query",options:resolveQueryOptionsWithDefaults(annotations.query)});else if(annotations.path)return createResult({kind:"path",options:resolvePathOptionsWithDefaults(annotations.path)});else if(annotations.statusCode)return createResult({kind:"statusCode"});else if(annotations.body)return createResult({kind:"body"});else if(annotations.bodyRoot)return createResult({kind:"bodyRoot"});else if(annotations.multipartBody)return createResult({kind:"multipartBody"});compilerAssert(!1,"Unexpected http property type")}function resolvePayloadProperties(program,type,visibility,disposition,options={}){let diagnostics=createDiagnosticCollector(),httpProperties=new Map;if(type.kind!=="Model"||type.properties.size===0&&!type.baseModel)return diagnostics.wrap([]);let visited=new Set;function checkModel(model,path){visited.add(model);let foundBody=!1,foundBodyProperty=!1;for(let property of walkPropertiesInherited(model)){let propPath=[...path,property.name];if(!isVisible2(program,property,visibility))continue;let httpProperty=diagnostics.pipe(getHttpProperty(program,property,propPath,options));if(httpProperty.kind!=="bodyIgnore"&&shouldTreatAsBodyProperty(httpProperty,disposition))httpProperty={kind:"bodyProperty",property,path:propPath};if(httpProperty.kind==="cookie"&&disposition===HttpPayloadDisposition.Response){diagnostics.add(createDiagnostic5({code:"response-cookie-not-supported",target:property,format:{propName:property.name}}));continue}if(httpProperty.kind==="body"||httpProperty.kind==="bodyRoot"||httpProperty.kind==="multipartBody")foundBody=!0;if(!(httpProperty.kind==="body"||httpProperty.kind==="multipartBody")&&isModelWithProperties(property.type)&&!visited.has(property.type)){if(checkModel(property.type,propPath)){foundBody=!0;continue}}if(httpProperty.kind==="bodyProperty")foundBodyProperty=!0;if(httpProperty.kind!=="bodyIgnore")httpProperties.set(property,httpProperty)}return foundBody&&!foundBodyProperty}return checkModel(type,[]),diagnostics.wrap([...httpProperties.values()])}function isModelWithProperties(type){return type.kind==="Model"&&!type.indexer&&type.properties.size>0}function shouldTreatAsBodyProperty(property,disposition){switch(disposition){case HttpPayloadDisposition.Request:return property.kind==="statusCode";case HttpPayloadDisposition.Response:return property.kind==="query"||property.kind==="path";case HttpPayloadDisposition.Multipart:return property.kind==="path"||property.kind==="query"||property.kind==="statusCode";default:return!1}}var HttpPayloadDisposition;(function(HttpPayloadDisposition2){HttpPayloadDisposition2[HttpPayloadDisposition2.Request=0]="Request",HttpPayloadDisposition2[HttpPayloadDisposition2.Response=1]="Response",HttpPayloadDisposition2[HttpPayloadDisposition2.Multipart=2]="Multipart"})(HttpPayloadDisposition||(HttpPayloadDisposition={}));function resolveHttpPayload(program,type,visibility,disposition,options={}){let diagnostics=createDiagnosticCollector(),metadata=diagnostics.pipe(resolvePayloadProperties(program,type,visibility,disposition,options)),body=diagnostics.pipe(resolveBody(program,type,metadata,visibility,disposition));if(body){if(body.contentTypes.some((x)=>x.startsWith("multipart/"))&&body.bodyKind!=="multipart")return diagnostics.add(createDiagnostic5({code:"no-implicit-multipart",target:body.property??type})),diagnostics.wrap({body:void 0,metadata})}return diagnostics.wrap({body,metadata})}function resolveBody(program,requestOrResponseType,metadata,visibility,disposition){let diagnostics=createDiagnosticCollector(),contentTypeProperty=metadata.find((x)=>x.kind==="contentType"),file=getHttpFileModel(program,requestOrResponseType,createHttpFileModelFilter(metadata));if(file!==void 0)if(!contentTypeProperty)return diagnostics.join(getFileBody(file));else{let contentTypes=contentTypeProperty&&diagnostics.pipe(getContentTypes(contentTypeProperty.property));reportDiagnostic7(program,{code:"http-file-structured",format:{contentTypes:contentTypes.join(", ")},target:contentTypeProperty.property})}if(requestOrResponseType.kind!=="Model"||isArrayModelType(requestOrResponseType))return diagnostics.wrap({bodyKind:"single",...diagnostics.pipe(resolveContentTypesForBody(program,contentTypeProperty,requestOrResponseType)),type:requestOrResponseType,isExplicit:!1,containsMetadataAnnotations:!1});let resolvedBody=diagnostics.pipe(resolveExplicitBodyProperty(program,metadata,contentTypeProperty,visibility,disposition));if(resolvedBody===void 0){if(requestOrResponseType.baseModel||requestOrResponseType.indexer)return diagnostics.wrap({bodyKind:"single",...diagnostics.pipe(resolveContentTypesForBody(program,contentTypeProperty,requestOrResponseType)),type:requestOrResponseType,isExplicit:!1,containsMetadataAnnotations:!1});if(requestOrResponseType.derivedModels.length>0&&getDiscriminator(program,requestOrResponseType))return diagnostics.wrap({bodyKind:"single",...diagnostics.pipe(resolveContentTypesForBody(program,contentTypeProperty,requestOrResponseType)),type:requestOrResponseType,isExplicit:!1,containsMetadataAnnotations:!1})}let unannotatedProperties=filterModelProperties(program,requestOrResponseType,(p)=>metadata.some((x)=>x.property===p&&x.kind==="bodyProperty"));if(unannotatedProperties.properties.size>0)if(resolvedBody===void 0)return diagnostics.wrap({bodyKind:"single",...diagnostics.pipe(resolveContentTypesForBody(program,contentTypeProperty,requestOrResponseType)),type:unannotatedProperties,isExplicit:!1,containsMetadataAnnotations:!1});else diagnostics.add(createDiagnostic5({code:"duplicate-body",messageId:"bodyAndUnannotated",target:requestOrResponseType}));if(resolvedBody===void 0&&contentTypeProperty)diagnostics.add(createDiagnostic5({code:"content-type-ignored",target:contentTypeProperty.property}));return diagnostics.wrap(resolvedBody)}function resolveExplicitBodyProperty(program,metadata,contentTypeProperty,visibility,disposition){let diagnostics=createDiagnosticCollector(),resolvedBody,duplicateTracker=new DuplicateTracker;for(let item of metadata){if(item.kind==="body"||item.kind==="bodyRoot"||item.kind==="multipartBody")duplicateTracker.track("body",item.property);switch(item.kind){case"body":case"bodyRoot":let containsMetadataAnnotations=!1;if(item.kind==="body")containsMetadataAnnotations=!diagnostics.pipe(validateBodyProperty(program,item.property,disposition));let file=getHttpFileModel(program,item.property.type,createHttpFileModelFilter(metadata)),isFile=file!==void 0&&!contentTypeProperty;if(file&&contentTypeProperty){let contentTypes=diagnostics.pipe(getContentTypes(contentTypeProperty.property));reportDiagnostic7(program,{code:"http-file-structured",format:{contentTypes:contentTypes.join(", ")},target:contentTypeProperty.property})}if(item.property.type.kind==="Union"&&unionContainsFile(program,item.property.type))reportDiagnostic7(program,{code:"http-file-structured",messageId:"union",target:item.property.node?.kind===SyntaxKind.ModelProperty?item.property.node.value:item.property});resolvedBody??=isFile?diagnostics.pipe(getFileBody(file,item.property)):{bodyKind:"single",...diagnostics.pipe(resolveContentTypesForBody(program,contentTypeProperty,item.property.type)),type:item.property.type,isExplicit:item.kind==="body",containsMetadataAnnotations,property:item.property};break;case"multipartBody":resolvedBody=diagnostics.pipe(resolveMultiPartBody(program,item.property,contentTypeProperty,visibility));break}}for(let[_2,items]of duplicateTracker.entries())for(let prop of items)diagnostics.add(createDiagnostic5({code:"duplicate-body",target:prop}));return diagnostics.wrap(resolvedBody)}function unionContainsFile(program,u){return unionContainsFileWorker(u);function unionContainsFileWorker(u2,visited=new Set){if(visited.has(u2))return!1;visited.add(u2);for(let{type}of u2.variants.values())if(!!getHttpFileModel(program,type)||type.kind==="Union"&&unionContainsFileWorker(type,visited))return!0;return!1}}function validateBodyProperty(program,property,disposition){let diagnostics=createDiagnosticCollector();return navigateType(property.type,{modelProperty:(prop)=>{let kind=isHeader(program,prop)?"header":(disposition===HttpPayloadDisposition.Request||disposition===HttpPayloadDisposition.Response)&&isCookieParam(program,prop)?"cookie":(disposition===HttpPayloadDisposition.Request||disposition===HttpPayloadDisposition.Multipart)&&isQueryParam(program,prop)?"query":disposition===HttpPayloadDisposition.Request&&isPathParam(program,prop)?"path":disposition===HttpPayloadDisposition.Response&&isStatusCode(program,prop)?"statusCode":void 0;if(kind)diagnostics.add(createDiagnostic5({code:"metadata-ignored",format:{kind},target:prop}))}},{}),diagnostics.wrap(diagnostics.diagnostics.length===0)}function resolveMultiPartBody(program,property,contentTypeProperty,visibility){let diagnostics=createDiagnosticCollector(),type=property.type,contentTypes=contentTypeProperty&&diagnostics.pipe(getContentTypes(contentTypeProperty.property));for(let contentType of contentTypes??[])if(!multipartContentTypesValues.includes(contentType))diagnostics.add(createDiagnostic5({code:"multipart-invalid-content-type",format:{contentType,supportedContentTypes:multipartContentTypesValues.join(", ")},target:type}));if(type.kind==="Model")return diagnostics.join(resolveMultiPartBodyFromModel(program,property,type,contentTypeProperty,visibility));else if(type.kind==="Tuple")return diagnostics.join(resolveMultiPartBodyFromTuple(program,property,type,contentTypeProperty,visibility));else return diagnostics.add(createDiagnostic5({code:"multipart-model",target:property})),diagnostics.wrap(void 0)}function resolveMultiPartBodyFromModel(program,property,type,contentTypeProperty,visibility){let diagnostics=createDiagnosticCollector(),parts=[];for(let item of type.properties.values()){let part=diagnostics.pipe(resolvePartOrParts(program,item.type,visibility,item));if(part)parts.push({partKind:"model",...part,name:part.name??item.name,optional:item.optional,property:item})}let resolvedContentTypes=contentTypeProperty?{contentTypeProperty:contentTypeProperty.property,contentTypes:diagnostics.pipe(getContentTypes(contentTypeProperty.property))}:{contentTypes:[multipartContentTypes.formData]};return diagnostics.wrap({bodyKind:"multipart",multipartKind:"model",...resolvedContentTypes,parts,property,type})}var multipartContentTypes={formData:"multipart/form-data",mixed:"multipart/mixed"},multipartContentTypesValues=Object.values(multipartContentTypes);function resolveMultiPartBodyFromTuple(program,property,type,contentTypeProperty,visibility){let diagnostics=createDiagnosticCollector(),parts=[],contentTypes=contentTypeProperty&&diagnostics.pipe(getContentTypes(contentTypeProperty?.property));for(let[index,item]of type.values.entries()){let part=diagnostics.pipe(resolvePartOrParts(program,item,visibility));if(part?.name===void 0&&contentTypes?.includes(multipartContentTypes.formData))diagnostics.add(createDiagnostic5({code:"formdata-no-part-name",target:type.node?.values[index]??type.values[index]}));if(part)parts.push({partKind:"tuple",...part,optional:!1})}let resolvedContentTypes=contentTypeProperty?{contentTypeProperty:contentTypeProperty.property,contentTypes:diagnostics.pipe(getContentTypes(contentTypeProperty.property))}:{contentTypes:[multipartContentTypes.formData]};return diagnostics.wrap({bodyKind:"multipart",multipartKind:"tuple",...resolvedContentTypes,parts,property,type})}function resolvePartOrParts(program,type,visibility,property){if(type.kind==="Model"&&isArrayModelType(type)){let[part,diagnostics]=resolvePart(program,type.indexer.value,visibility,property);if(part)return[{...part,multi:!0},diagnostics];return[part,diagnostics]}else return resolvePart(program,type,visibility,property)}function resolvePart(program,type,visibility,property){let diagnostics=createDiagnosticCollector(),part=getHttpPart(program,type);if(part){let{body,metadata}=diagnostics.pipe(resolveHttpPayload(program,part.type,visibility,HttpPayloadDisposition.Multipart)),contentTypeProperty=metadata.find((x)=>x.kind==="contentType");if(body===void 0)return diagnostics.wrap(void 0);else if(body.bodyKind==="multipart")return diagnostics.add(createDiagnostic5({code:"multipart-nested",target:type})),diagnostics.wrap(void 0);if(body.contentTypes.length===0)body={...body,contentTypes:diagnostics.pipe(resolveContentTypesForBody(program,contentTypeProperty,body.type)).contentTypes};return diagnostics.wrap({multi:!1,property,name:part.options.name,body,optional:!1,headers:metadata.filter((x)=>x.kind==="header"),filename:body.bodyKind==="file"?body.filename:void 0})}return diagnostics.add(createDiagnostic5({code:"multipart-part",target:type})),diagnostics.wrap(void 0)}function createHttpFileModelFilter(metadata){let metadataPropToMetadata=new Map(metadata.map((x)=>[x.property,x]));return function(property){let httpProperty=metadataPropToMetadata.get(property);if(["filename"].includes(property.name))return!0;if(!httpProperty)return!0;if(httpProperty.kind!=="bodyProperty")return!1;return!0}}function getFileBody(file,property){let[contentTypes,diagnostics]=getContentTypes(file.contentType),isText=isOrExtendsString(file.contents.type);return[{bodyKind:"file",type:file.type,contents:file.contents,filename:file.filename,isText,contentTypeProperty:file.contentType,contentTypes,property},diagnostics];function isOrExtendsString(type){return isString(type)||!!type.baseScalar&&isOrExtendsString(type.baseScalar);function isString(type2){return type2.name==="string"&&!!type2.namespace&&type2.namespace.name==="TypeSpec"&&!!type2.namespace.namespace&&type2.namespace.namespace.name===""&&!type2.namespace.namespace.namespace}}}function getDefaultContentTypeForKind(type){return type.kind==="Scalar"?"text/plain":"application/json"}function isLiteralType(type){return type.kind==="String"||type.kind==="Number"||type.kind==="Boolean"||type.kind==="StringTemplate"}function resolveContentTypesForBody(program,contentTypeProperty,type,getDefaultContentType=getDefaultContentTypeForKind){let diagnostics=createDiagnosticCollector();return diagnostics.wrap(resolve());function resolve(){let mpContentType;if(isMergePatchBody(program,type))mpContentType="application/merge-patch+json";if(contentTypeProperty){let explicitContentTypes=diagnostics.pipe(getContentTypes(contentTypeProperty.property));if(mpContentType){let badContentTypes=explicitContentTypes.filter((c)=>!c.startsWith(mpContentType));if(badContentTypes.length>0)diagnostics.add(createDiagnostic5({code:"merge-patch-content-type",target:contentTypeProperty.property??type,format:{contentType:badContentTypes[0]}}))}return{contentTypes:explicitContentTypes,contentTypeProperty:contentTypeProperty.property}}if(mpContentType)return{contentTypes:[mpContentType]};if(isLiteralType(type))switch(type.kind){case"StringTemplate":case"String":type=program.checker.getStdType("string");break;case"Boolean":type=program.checker.getStdType("boolean");break;case"Number":type=program.checker.getStdType("numeric");break;default:}let encoded;while((type.kind==="Scalar"||type.kind==="ModelProperty")&&(encoded=getEncode(program,type)))type=encoded.type;if(type.kind==="Union"){let variants=[...type.variants.values()];if(variants.some((v)=>v.type.kind==="Intrinsic"&&v.type.name==="null"))return{contentTypes:["application/json"]};let set=new Set;for(let variant of variants){let resolved=diagnostics.pipe(resolveContentTypesForBody(program,contentTypeProperty,variant.type));for(let contentType of resolved.contentTypes)set.add(contentType)}return{contentTypes:[...set]}}else return{contentTypes:[getMediaTypeHint(program,type)??getDefaultContentType(type)]}}}var operators=["+","#",".","/",";","?","&"],uriTemplateRegex=/\{([^{}]+)\}|([^{}]+)/g,expressionRegex=/([^:*]*)(?::(\d+)|(\*))?/;function parseUriTemplate(template){let parameters=[],segments=[],matches=template.matchAll(uriTemplateRegex);for(let[_2,expression,literal]of matches)if(expression){let operator;if(operators.includes(expression[0]))operator=expression[0],expression=expression.slice(1);let items=expression.split(",");for(let item of items){let match=item.match(expressionRegex),parameter={name:match[1],operator,modifier:match[3]?{type:"explode"}:match[2]?{type:"prefix",value:Number(match[2])}:void 0};parameters.push(parameter),segments.push(parameter)}}else segments.push(literal);return{segments,parameters}}function getOperationParameters(program,operation,partialUriTemplate,overloadBase,options={}){let verb=(options?.verbSelector&&options.verbSelector(program,operation))??getOperationVerb(program,operation)??overloadBase?.verb;if(verb)return getOperationParametersForVerb(program,operation,verb,partialUriTemplate);let post=getOperationParametersForVerb(program,operation,"post",partialUriTemplate);return post[0].body?post:getOperationParametersForVerb(program,operation,"get",partialUriTemplate)}var operatorToStyle={";":"matrix","#":"fragment",".":"label","/":"path"};function getOperationParametersForVerb(program,operation,verb,partialUriTemplate){let diagnostics=createDiagnosticCollector(),visibility=resolveRequestVisibility(program,operation,verb),parsedUriTemplate=parseUriTemplate(partialUriTemplate),parameters=[],{body:resolvedBody,metadata}=diagnostics.pipe(resolveHttpPayload(program,operation.parameters,visibility,HttpPayloadDisposition.Request,{implicitParameter:(param)=>{let isTopLevel=param.model===operation.parameters,pathOptions=getPathOptions(program,param),queryOptions=getQueryOptions(program,param),name=pathOptions?.name??queryOptions?.name??param.name,uriParam=isTopLevel&&parsedUriTemplate.parameters.find((x)=>x.name===name);if(!uriParam){let pathOptions2=getPathOptions(program,param);if(pathOptions2&&param.optional)return{type:"path",name:pathOptions2.name,explode:!1,allowReserved:!1,style:operatorToStyle["/"]};return}let explode=uriParam.modifier?.type==="explode";if(uriParam.operator==="?"||uriParam.operator==="&")return{type:"query",name:uriParam.name,explode};else if(uriParam.operator==="+")return{type:"path",name:uriParam.name,explode,allowReserved:!0,style:"simple"};else return{type:"path",name:uriParam.name,explode,allowReserved:!1,style:(uriParam.operator&&operatorToStyle[uriParam.operator])??"simple"}}}));for(let item of metadata)switch(item.kind){case"contentType":parameters.push({name:"Content-Type",type:"header",param:item.property});break;case"path":case"query":case"cookie":case"header":parameters.push({type:item.kind,...item.options,param:item.property});break}let body=resolvedBody;return diagnostics.wrap({properties:metadata,parameters,verb,body,get bodyType(){return body?.type},get bodyParameter(){return body?.property}})}var AllowedSegmentSeparators=["/",":","?"];function needsSlashPrefix(fragment){return!(fragment.length===0||AllowedSegmentSeparators.indexOf(fragment[0])!==-1||fragment[0]==="{"&&fragment[1]==="/")}function normalizeFragment(fragment,trimLast=!1){if(needsSlashPrefix(fragment))fragment=`/${fragment}`;if(trimLast&&fragment[fragment.length-1]==="/")return fragment.slice(0,-1);return fragment}function joinPathSegments(rest){let current="";for(let[index,segment]of rest.entries())current+=normalizeFragment(segment,index<rest.length-1);return current}function buildPath(pathFragments){let path=pathFragments.length===0?"/":joinPathSegments(pathFragments);return AllowedSegmentSeparators.includes(path[0])||path[0]==="{"&&path[1]==="/"?path:`/${path}`}function resolvePathAndParameters(program,operation,overloadBase,options){let diagnostics=createDiagnosticCollector(),{uriTemplate,parameters}=diagnostics.pipe(getUriTemplateAndParameters(program,operation,overloadBase,options)),parsedUriTemplate=parseUriTemplate(uriTemplate),paramByName=new Set(parameters.parameters.filter(({type})=>type==="path"||type==="query").map((x)=>x.name));validateDoubleSlash(parsedUriTemplate,operation,parameters).forEach((d)=>diagnostics.add(d));for(let routeParam of parsedUriTemplate.parameters){let decoded=decodeURIComponent(routeParam.name);if(!paramByName.has(routeParam.name)&&!paramByName.has(decoded))diagnostics.add(createDiagnostic5({code:"missing-uri-param",format:{param:routeParam.name},target:operation}))}let path=produceLegacyPathFromUriTemplate(parsedUriTemplate);return diagnostics.wrap({uriTemplate,path,parameters})}function validateDoubleSlash(parsedUriTemplate,operation,parameters){let diagnostics=createDiagnosticCollector();if(parsedUriTemplate.segments){let[firstSeg,...rest]=parsedUriTemplate.segments,lastSeg=firstSeg;for(let seg of rest)if(typeof seg!=="string"){let parameter=parameters.parameters.find((x)=>x.name===seg.name);if(seg.operator==="/"){if(typeof lastSeg==="string"&&lastSeg.endsWith("/"))diagnostics.add(createDiagnostic5({code:"double-slash",messageId:parameter?.param.optional?"optionalUnset":"default",format:{paramName:seg.name},target:operation}))}lastSeg=seg}}return diagnostics.diagnostics}function produceLegacyPathFromUriTemplate(uriTemplate){let result="";for(let segment of uriTemplate.segments??[])if(typeof segment==="string")result+=segment;else if(segment.operator!=="?"&&segment.operator!=="&")result+=`{${segment.name}}`;return result}function collectSegmentsAndOptions(program,source){if(source===void 0)return[[],{}];let[parentSegments,parentOptions]=collectSegmentsAndOptions(program,source.namespace),route=getRoutePath(program,source)?.path,options=source.kind==="Namespace"?getRouteOptionsForNamespace(program,source)??{}:{};return[[...parentSegments,...route?[route]:[]],{...parentOptions,...options}]}function getUriTemplateAndParameters(program,operation,overloadBase,options){let[parentSegments,parentOptions]=collectSegmentsAndOptions(program,operation.interface??operation.namespace),routeProducer=getRouteProducer(program,operation)??DefaultRouteProducer,[result,diagnostics]=routeProducer(program,operation,parentSegments,overloadBase,{...parentOptions,...options});return[{uriTemplate:buildPath([result.uriTemplate]),parameters:result.parameters},diagnostics]}function DefaultRouteProducer(program,operation,parentSegments,overloadBase,options){let diagnostics=createDiagnosticCollector(),routePath=getRoutePath(program,operation)?.path,uriTemplate=!routePath&&overloadBase?overloadBase.uriTemplate:joinPathSegments([...parentSegments,...routePath?[routePath]:[]]),parsedUriTemplate=parseUriTemplate(uriTemplate),parameters=diagnostics.pipe(getOperationParameters(program,operation,uriTemplate,overloadBase,options.paramOptions)),unreferencedPathParamNames=new Map(parameters.parameters.filter(({type})=>type==="path"||type==="query").map((x)=>[x.name,x]));for(let uriParam of parsedUriTemplate.parameters)unreferencedPathParamNames.delete(uriParam.name);let resolvedUriTemplate=addOperationTemplateToUriTemplate(uriTemplate,[...unreferencedPathParamNames.values()]);return diagnostics.wrap({uriTemplate:resolvedUriTemplate,parameters})}var styleToOperator={matrix:";",label:".",simple:"",path:"/",fragment:"#"};function getUriTemplatePathParam(param){return`{${param.param.optional?"/":param.allowReserved?"+":styleToOperator[param.style]}${param.name}${param.explode?"*":""}}`}function getUriTemplateQueryParamPart(param){return`${escapeUriTemplateParamName(param.name)}${param.explode?"*":""}`}function addQueryParamsToUriTemplate(uriTemplate,params){let queryParams=params.filter((x)=>x.type==="query");return uriTemplate+(queryParams.length>0?`{?${queryParams.map((x)=>getUriTemplateQueryParamPart(x)).join(",")}}`:"")}function addOperationTemplateToUriTemplate(uriTemplate,params){let pathParams=params.filter((x)=>x.type==="path").map(getUriTemplatePathParam),queryParams=params.filter((x)=>x.type==="query"),pathPart=joinPathSegments([uriTemplate,...pathParams]);return addQueryParamsToUriTemplate(pathPart,queryParams)}function escapeUriTemplateParamName(name){return encodeURIComponent(name).replace(/[:-]/g,function(c){return"%"+c.charCodeAt(0).toString(16).toUpperCase()})}function setRouteProducer(program,operation,routeProducer){program.stateMap(HttpStateKeys.routeProducer).set(operation,routeProducer)}function getRouteProducer(program,operation){return program.stateMap(HttpStateKeys.routeProducer).get(operation)}function getRouteOptionsForNamespace(program,namespace){return program.stateMap(HttpStateKeys.routeOptions).get(namespace)}function getRoutePath(program,entity){let path=program.stateMap(HttpStateKeys.routes).get(entity);return path?{path,shared:entity.kind==="Operation"&&isSharedRoute(program,entity)}:void 0}var opReferenceContainerRouteRule=createLinterRule({name:"op-reference-container-route",severity:"warning",description:"Check for referenced (`op is`) operations which have a @route on one of their containers.",url:"https://typespec.io/docs/libraries/http/rules/op-reference-container-route",docs:fileRef.fromPackageRoot("src/rules/op-reference-container-route.md"),messages:{default:paramMessage`Operation ${"opName"} references an operation which has a @route prefix on its namespace or interface: "${"routePrefix"}".  This operation will not carry forward the route prefix so the final route may be different than the referenced operation.`},create(context){let checkedOps=new Map;function getContainerRoutePrefix(container){if(container===void 0)return;if(container.kind==="Operation")return getContainerRoutePrefix(container.interface)??getContainerRoutePrefix(container.namespace);let route=getRoutePath(context.program,container);return route?route.path:getContainerRoutePrefix(container.namespace)}function checkOperationReferences(op,originalOp){if(op!==void 0){let container=op.interface??op.namespace,originalContainer=originalOp.interface??originalOp.namespace;if(container!==originalContainer){let route=checkedOps.get(op);if(route===void 0)route=getContainerRoutePrefix(op),checkedOps.set(op,route);if(route){context.reportDiagnostic({target:originalOp,format:{opName:originalOp.name,routePrefix:route}});return}}checkOperationReferences(op.sourceOperation,originalOp)}}return{operation:(op)=>{checkOperationReferences(op.sourceOperation,op)}}}});var $linter=defineLinter({rules:[opReferenceContainerRouteRule]});function useCache(program,key,type,compute){let stage=program.currentStage;if(stage!=="validating"&&stage!=="linting"&&stage!=="emitting")return compute();if(!type.isFinished)return compute();let map2=program.stateMap(key),existing=map2.get(type);if(existing!==void 0)return existing;let value=compute();return map2.set(type,value),value}function getResponsesForOperation(program,operation){let diagnostics=createDiagnosticCollector(),responses=new ResponseIndex,variants=resolveResponseVariants(program,operation.returnType);for(let{type,description}of variants)processResponseType(program,diagnostics,operation,responses,type,description);return diagnostics.wrap(responses.values())}function resolveResponseVariants(program,responseType,parentDescription){let tk=$(program);if(!tk.union.is(responseType)||tk.union.getDiscriminatedUnion(responseType))return[{type:responseType,description:parentDescription}];let unionDescription=getDoc(program,responseType)??parentDescription,plainVariants=[],responseEnvelopes=[];for(let option of responseType.variants.values()){if(isNullType(option.type))continue;let resolved=resolveResponseVariants(program,option.type,getDoc(program,option)??unionDescription);for(let variant of resolved)if(isPlainResponseBody(program,variant.type))plainVariants.push(variant.type);else responseEnvelopes.push(variant)}let results=[];if(plainVariants.length===1)results.push({type:plainVariants[0],description:unionDescription});else if(plainVariants.length>1){let unionType=responseEnvelopes.length===0?responseType:tk.union.create(plainVariants);results.push({type:unionType,description:unionDescription})}return results.push(...responseEnvelopes),results}class ResponseIndex{#index=new Map;get(statusCode){return this.#index.get(this.#indexKey(statusCode))}set(statusCode,response){this.#index.set(this.#indexKey(statusCode),response)}values(){return[...this.#index.values()]}#indexKey(statusCode){if(typeof statusCode==="number"||statusCode==="*")return String(statusCode);else return`${statusCode.start}-${statusCode.end}`}}function processResponseType(program,diagnostics,operation,responses,responseType,parentDescription){let verb=getOperationVerb(program,operation),{body:resolvedBody,metadata}=diagnostics.pipe(resolveHttpPayload(program,responseType,Visibility.Read,HttpPayloadDisposition.Response,{treatContentTypeAsHeader:verb==="head"})),statusCodes=diagnostics.pipe(getResponseStatusCodes(program,responseType,metadata)),headers=getResponseHeaders(program,metadata);if(statusCodes.length===0)if(isErrorModel(program,responseType))statusCodes.push("*");else if(isVoidType(responseType))resolvedBody=void 0,statusCodes.push(204);else if(resolvedBody===void 0||isVoidType(resolvedBody.type))resolvedBody=void 0,statusCodes.push(200);else statusCodes.push(200);for(let statusCode of statusCodes){let response=responses.get(statusCode)??{statusCodes:statusCode,type:responseType,description:getResponseDescription(program,operation,responseType,statusCode,metadata,parentDescription),responses:[]};if(resolvedBody!==void 0)response.responses.push({body:resolvedBody,headers,properties:metadata});else response.responses.push({headers,properties:metadata});responses.set(statusCode,response)}}function getResponseStatusCodes(program,responseType,metadata){let codes=[],diagnostics=createDiagnosticCollector(),statusFound=!1;for(let prop of metadata)if(prop.kind==="statusCode"){if(statusFound)reportDiagnostic7(program,{code:"multiple-status-codes",target:responseType});statusFound=!0,codes.push(...diagnostics.pipe(getStatusCodesWithDiagnostics(program,prop.property)))}if(responseType.kind==="Model")for(let t=responseType;t;t=t.baseModel)codes.push(...getExplicitSetStatusCode(program,t));return diagnostics.wrap(codes)}function getExplicitSetStatusCode(program,entity){return program.stateMap(HttpStateKeys.statusCode).get(entity)??[]}function getResponseHeaders(program,metadata){let responseHeaders={};for(let prop of metadata)if(prop.kind==="header")responseHeaders[prop.options.name]=prop.property;return responseHeaders}function isResponseEnvelope(metadata){return metadata.some((prop)=>prop.kind==="body"||prop.kind==="bodyRoot"||prop.kind==="multipartBody"||prop.kind==="statusCode")}function isPlainResponseBody(program,type){if(isVoidType(type)||isErrorModel(program,type))return!1;if(type.kind==="Model"&&getExplicitSetStatusCode(program,type).length>0)return!1;let[result]=resolveHttpPayload(program,type,Visibility.Read,HttpPayloadDisposition.Response);return!result||!result.metadata.some((p)=>p.kind!=="bodyProperty")}function getResponseDescription(program,operation,responseType,statusCode,metadata,parentDescription){if(parentDescription)return parentDescription;if(isResponseEnvelope(metadata)){let desc2=getDoc(program,responseType);if(desc2)return desc2}let desc=isErrorModel(program,responseType)?getErrorsDoc(program,operation):getReturnsDoc(program,operation);if(desc)return desc;return getStatusCodeDescription(statusCode)}var httpOperationCacheKey=Symbol.for("@typespec/http.httpOperationCache");function getHttpOperation(program,operation,options){if(!options)return useCache(program,httpOperationCacheKey,operation,()=>getHttpOperationInternal(program,operation,options,new Map));return getHttpOperationInternal(program,operation,options,new Map)}function listHttpOperationsIn(program,container,options){let diagnostics=createDiagnosticCollector(),operations=listOperationsIn(container,options?.listOptions),cache=new Map,httpOperations=operations.map((x)=>diagnostics.pipe(getHttpOperationInternal(program,x,options,cache)));return diagnostics.wrap(httpOperations)}function getAllHttpServices(program,options){let diagnostics=createDiagnosticCollector(),serviceNamespaces=listServices(program),services=serviceNamespaces.map((x)=>diagnostics.pipe(getHttpService(program,x.type,options)));if(serviceNamespaces.length===0)services.push(diagnostics.pipe(getHttpService(program,program.getGlobalNamespaceType(),options)));return diagnostics.wrap(services)}function getHttpService(program,serviceNamespace,options){let diagnostics=createDiagnosticCollector(),httpOperations=diagnostics.pipe(listHttpOperationsIn(program,serviceNamespace,{...options,listOptions:{recursive:serviceNamespace!==program.getGlobalNamespaceType()}})),authentication=getAuthentication(program,serviceNamespace);validateRouteUnique(program,diagnostics,httpOperations);let service={namespace:serviceNamespace,operations:httpOperations,authentication};return diagnostics.wrap(service)}function validateRouteUnique(program,diagnostics,operations){let grouped=new Map;for(let operation of operations){let{verb,path}=operation;if(operation.overloading!==void 0&&isOverloadSameEndpoint(operation))continue;if(isSharedRoute(program,operation.operation))continue;let map2=grouped.get(path);if(map2===void 0)map2=new Map,grouped.set(path,map2);let list=map2.get(verb);if(list===void 0)list=[],map2.set(verb,list);list.push(operation)}for(let[path,map2]of grouped)for(let[verb,routes]of map2)if(routes.length>=2)for(let route of routes)diagnostics.add(createDiagnostic5({code:"duplicate-operation",format:{path,verb,operationName:route.operation.name},target:route.operation}))}function isOverloadSameEndpoint(overload){return overload.path===overload.overloading.path&&overload.verb===overload.overloading.verb}function getHttpOperationInternal(program,operation,options,cache){let existing=cache.get(operation);if(existing)return[existing,[]];let diagnostics=createDiagnosticCollector(),httpOperationRef={operation};cache.set(operation,httpOperationRef);let overloadBase=getOverloadedOperation(program,operation),overloading;if(overloadBase)overloading=httpOperationRef.overloading=diagnostics.pipe(getHttpOperationInternal(program,overloadBase,options,cache));let route=diagnostics.pipe(resolvePathAndParameters(program,operation,overloading,options??{})),responses=diagnostics.pipe(getResponsesForOperation(program,operation)),authentication=getAuthenticationForOperation(program,operation),httpOperation={path:route.path,uriTemplate:route.uriTemplate,verb:route.parameters.verb,container:operation.interface??operation.namespace??program.getGlobalNamespaceType(),parameters:route.parameters,responses,operation,authentication};Object.assign(httpOperationRef,httpOperation);let overloads=getOverloads(program,operation);if(overloads)httpOperationRef.overloads=overloads.map((x)=>diagnostics.pipe(getHttpOperationInternal(program,x,options,cache)));return diagnostics.wrap(httpOperationRef)}var exports_tsp_index9={};__export(exports_tsp_index9,{$provideTypeInfo:()=>$provideTypeInfo,$onValidate:()=>$onValidate3,$lib:()=>$lib6,$functions:()=>$functions,$decorators:()=>$decorators6});var $mergePatchModel=(ctx,target,source)=>{setMergePatchSource(ctx.program,target,source)},$mergePatchProperty=(ctx,target,source)=>{setMergePatchPropertySource(ctx.program,target,source)},MUTATOR_RESULT_CACHE=Symbol.for("TypeSpec.Http.MutatorResultCache");function cachedMutateSubgraph(program,mutator,type){let cache=mutator[MUTATOR_RESULT_CACHE]??=new WeakMap,cached=cache.get(type);if(cached)return cached;return cached=mutateSubgraph(program,[mutator],type),cache.set(type,cached),cached}var MERGE_PATCH_MUTATOR_CACHE=Symbol.for("TypeSpec.Http.MergePatchMutatorCache");function applyMergePatchTransform(ctx,input,nameTemplate,options){let reported=!1;navigateType(input,{intrinsic:(i)=>{if(!reported&&i.name==="null")reportDiagnostic7(ctx.program,{code:"merge-patch-contains-null",target:input}),reported=!0}},{visitDerivedTypes:!1,includeTemplateDeclaration:!1});let mutatorCache=ctx.program[MERGE_PATCH_MUTATOR_CACHE]??={},visibilityMode=options.visibilityMode.value.name,mutator=(mutatorCache[nameTemplate]??={})[visibilityMode]??=createMergePatchMutator(ctx,nameTemplate,visibilityMode),{type}=cachedMutateSubgraph(ctx.program,mutator,input);return compilerAssert(type.kind==="Model","Expected the root of the MergePatch transform to be a Model"),type}var $applyMergePatch=(ctx,target,source,nameTemplate,options)=>{let transformed=applyMergePatchTransform(ctx,source,nameTemplate,options);setMergePatchSource(ctx.program,target,source),setMediaTypeHint(ctx.program,target,"application/merge-patch+json"),target.properties=transformed.properties};function visibilityModeToFilters(program,visibilityMode){let Lifecycle=getLifecycleVisibilityEnum(program),vfUpdate={any:new Set([Lifecycle.members.get("Update")])},vfCreateOrUpdate={any:new Set([Lifecycle.members.get("Create"),Lifecycle.members.get("Update")])},vfCreate={any:new Set([Lifecycle.members.get("Create")])};switch(visibilityMode){case"Update":return[vfUpdate,vfCreateOrUpdate,vfCreate];case"CreateOrUpdate":return[vfCreateOrUpdate,vfCreateOrUpdate,vfCreate];default:compilerAssert(!1,`Unexpected MergePatch visibility mode: ${visibilityMode}`)}}function isDiscriminatedProperty(program,property){if(property.model===void 0)return!1;let discriminator=getDiscriminator(program,property.model);if(discriminator===void 0)return!1;if(discriminator.propertyName!==property.name)return!1;return!0}function overrideDiscriminatedUnionProperty(program,variant){let[discriminated,_2]=getDiscriminatedUnion(program,variant.union);if(!discriminated||variant.type.kind!=="Model")return;for(let[name,property]of variant.type.properties)if(name===discriminated.options.discriminatorPropertyName)setPropertyOverride(program,property,{optional:!1,erasable:!1})}function setPropertyOverride(program,property,values){let override=getMergePatchPropertyOverrides(program,property)??{};if(values.optional!==void 0)override.optional=values.optional;if(values.erasable!==void 0)override.erasable=values.erasable;if(values.updateBehavior!==void 0)override.updateBehavior=values.updateBehavior;setMergePatchPropertyOverrides(program,property,override)}function createMergePatchMutator(ctx,nameTemplate,visibilityMode){let Lifecycle=getLifecycleVisibilityEnum(ctx.program),[primaryFilter,optionalFilter,replaceFilter]=visibilityModeToFilters(ctx.program,visibilityMode),replaceNameTemplate=nameTemplate+"ReplaceOnly",optionalNameTemplate=visibilityMode==="CreateOrUpdate"?nameTemplate:nameTemplate+"OrCreate",replaceMutator=bindMutator(replaceFilter,replaceNameTemplate),optionalMutator=bindMutator(optionalFilter,optionalNameTemplate,replaceMutator);if(visibilityMode==="CreateOrUpdate")return optionalMutator;else return bindMutator(primaryFilter,nameTemplate,replaceMutator,optionalMutator);function bindMutator(visibilityFilter,nameTemplate2,_replaceInteriorMutator,_optionalInteriorMutator){function isReplaceMutator(){return _replaceInteriorMutator===void 0}let mpMutator={name:`MergePatchProperty${visibilityMode}`,ModelProperty:{filter:()=>MutatorFlow.DoNotRecur,mutate:(prop,clone,program,realm)=>{let decorators=[],overrides=getMergePatchPropertyOverrides(program,prop);for(let decorator of prop.decorators){let decFn=decorator.decorator;if(decFn===$visibility||decFn===$removeVisibility){let nextArgs=decorator.args.filter((arg)=>{if(arg.value.entityKind!=="Value")return!1;return!(arg.value.valueKind==="EnumValue"&&arg.value.value.enum===Lifecycle)});if(nextArgs.length>0)decorators.push({...decorator,args:nextArgs})}else if(!(decFn===$invisible&&decorator.args[0]?.value===Lifecycle))decorators.push(decorator)}if(clone.decorators=decorators,resetVisibilityModifiersForClass(program,clone,Lifecycle),isMergePatchSubject(prop.type)){let mutated=prop.optional?cachedMutateSubgraph(program,_optionalInteriorMutator??self,prop.type):cachedMutateSubgraph(program,self,prop.type);clone.type=mutated.type}if(!isReplaceMutator()){if(overrides?.erasable!==!1&&(prop.optional||prop.defaultValue!==void 0))clone.type=nullable(realm,clone.type);clone.optional=overrides?.optional??(isDiscriminatedProperty(program,prop)?!1:!0),clone.defaultValue=void 0}clone.decorators.push({decorator:$mergePatchProperty,args:[{value:prop,jsValue:prop}]})}}},self={name:`MergePatch${visibilityMode}`,Union:{filter:()=>MutatorFlow.DoNotRecur,mutate:(union,clone,program)=>{for(let[key,member]of union.variants)if(overrideDiscriminatedUnionProperty(program,member),isMergePatchSubject(member.type)){let variant={...member,type:cachedMutateSubgraph(program,_optionalInteriorMutator??self,member.type).type};clone.variants.set(key,variant)}if(union.name)clone.decorators=[...clone.decorators,{decorator:function(ctx2,target){setMediaTypeHint(ctx2.program,target,"application/merge-patch+json")},args:[]}];rename(ctx.program,clone,nameTemplate2)}},Model:{filter:()=>MutatorFlow.DoNotRecur,mutate:(model,clone,program,realm)=>{if($(realm).array.is(model)&&isMergePatchSubject(model.indexer.value))clone.indexer={key:model.indexer.key,value:cachedMutateSubgraph(program,_replaceInteriorMutator??self,model.indexer.value).type};else if($(realm).record.is(model)&&isMergePatchSubject(model.indexer.value))clone.indexer={key:model.indexer.key,value:mutateSubgraph(program,[_optionalInteriorMutator??self],model.indexer.value).type};for(let[key,prop]of model.properties)if(!isVisible(program,prop,visibilityFilter)){let clonedProp=clone.properties.get(key);if(clonedProp)clone.properties.delete(key),realm.remove(clonedProp)}else if(!isMetadata(program,prop)){let mutatedProp=mutateSubgraph(program,[mpMutator],prop).type;mutatedProp.model=clone,clone.properties.set(key,mutatedProp)}else{let decorator=isPathParam(program,prop)?"@path":isHeader(program,prop)?"@header":isCookieParam(program,prop)?"@cookie":isQueryParam(program,prop)?"@query":isStatusCode(program,prop)?"@statusCode":void 0;if(decorator)reportDiagnostic7(program,{code:"merge-patch-contains-metadata",target:prop,format:{metadataType:decorator,propertyName:prop.name}})}clone.decorators=clone.decorators.filter((d)=>d.decorator!==$applyMergePatch),clone.decorators.push({decorator:function(ctx2,target){setMergePatchSource(ctx2.program,target,model),setMediaTypeHint(ctx2.program,target,"application/merge-patch+json")},args:[]}),ctx.program.stateMap(HttpStateKeys.mergePatchModel).set(clone,model),rename(ctx.program,clone,nameTemplate2)}},ModelProperty:{filter:()=>MutatorFlow.DoNotRecur,mutate:(prop,clone,program)=>{if(isMergePatchSubject(prop.type))clone.type=cachedMutateSubgraph(program,prop.optional?_optionalInteriorMutator??self:self,prop.type).type;ctx.program.stateMap(HttpStateKeys.mergePatchProperty).set(clone,prop)}},UnionVariant:{filter:()=>MutatorFlow.DoNotRecur,mutate:(variant,clone,program)=>{if(isMergePatchSubject(variant.type)){let mutated=cachedMutateSubgraph(program,_optionalInteriorMutator||self,variant.type);clone.type=mutated.type}}},Tuple:{filter:()=>MutatorFlow.DoNotRecur,mutate:(tuple,clone,program)=>{for(let[index,element]of tuple.values.entries())if(isMergePatchSubject(element))clone.values[index]=cachedMutateSubgraph(program,_replaceInteriorMutator??self,element).type}}};return self}function nullable(realm,t){return $(realm).union.create({variants:[$(realm).unionVariant.create({type:t}),$(realm).unionVariant.create({type:$(realm).intrinsic.null})]})}}function isMergePatchSubject(type){return type.kind==="Model"||type.kind==="Union"||type.kind==="ModelProperty"||type.kind==="UnionVariant"||type.kind==="Tuple"}function rename(program,type,nameTemplate){if($(program).array.is(type)&&type.name==="Array")return;if(type.name&&nameTemplate)type.name=replaceTemplatedStringFromProperties(nameTemplate,type)}function replaceTemplatedStringFromProperties(formatString,sourceObject){if(sourceObject.kind==="TemplateParameter")return formatString;return formatString.replace(/{(\w+)}/g,(_2,propName)=>{return sourceObject[propName]})}var $provideTypeInfo=defineTypeInfoProvider(({program,target})=>{if(target.kind!=="Operation")return;let[operation]=getHttpOperation(program,target);if(!operation)return;let lines=[`\`HTTP Route\`: \`${operation.verb.toUpperCase()} ${operation.uriTemplate}\``],statusCodes=operation.responses.map((response)=>formatStatusCode(response.statusCodes));if(statusCodes.length>0)lines.push(`\`Responses\`: ${statusCodes.map((code)=>`\`${code}\``).join(", ")}`);return{content:lines.join(`

`)}});function formatStatusCode(statusCode){if(statusCode==="*")return"*";if(typeof statusCode==="number")return String(statusCode);return`${statusCode.start}-${statusCode.end}`}function $onValidate3(program){let[services,diagnostics]=getAllHttpServices(program);if(diagnostics.length>0)program.reportDiagnostics(diagnostics);validateSharedRouteConsistency(program,services)}function groupHttpOperations(operations){let paths=new Map;for(let operation of operations){let{verb,path}=operation,pathOps=paths.get(path);if(pathOps===void 0)pathOps=new Map,paths.set(path,pathOps);let ops=pathOps.get(verb);if(ops===void 0)pathOps.set(verb,[operation]);else ops.push(operation)}return paths}function validateSharedRouteConsistency(program,services){for(let service of services){let paths=groupHttpOperations(service.operations);for(let pathOps of paths.values())for(let ops of pathOps.values()){let hasShared=!1,hasNonShared=!1;for(let op of ops)if(isSharedRoute(program,op.operation))hasShared=!0;else hasNonShared=!0;if(hasShared&&hasNonShared)for(let op of ops)reportDiagnostic7(program,{code:"shared-inconsistency",target:op.operation,format:{verb:op.verb,path:op.path}})}}}var $decorators6={"TypeSpec.Http":{body:$body,bodyIgnore:$bodyIgnore,bodyRoot:$bodyRoot,cookie:$cookie,delete:$delete,get:$get,header:$header,head:$head,multipartBody:$multipartBody,patch:$patch,path:$path,post:$post,put:$put,query:$query2,route:$route,server:$server,sharedRoute:$sharedRoute,statusCode:$statusCode,useAuth:$useAuth},"TypeSpec.Http.Private":{httpFile:$httpFile,httpPart:$httpPart,plainData:$plainData,includeInapplicableMetadataInPayload:$includeInapplicableMetadataInPayload,applyMergePatch:$applyMergePatch,mergePatchModel:$mergePatchModel,mergePatchProperty:$mergePatchProperty}},$functions={"TypeSpec.Http.Private":{applyMergePatchTransform}};function $onValidate4(program){checkForIncorrectlyAssignedTerminalEvents(program),checkForSSEStreamWithoutEventsDecorator(program)}function checkForIncorrectlyAssignedTerminalEvents(program){program.stateSet(SSEStateKeys.terminalEvent).forEach((terminalEvent)=>{if(!("union"in terminalEvent))return;validateTerminalEvent(program,terminalEvent)})}function validateTerminalEvent(program,target){if(!isEvents(program,target.union))reportDiagnostic5(program,{code:"terminal-event-not-in-events",target})}function checkForSSEStreamWithoutEventsDecorator(program){navigateProgram(program,{model:(model)=>{validateSSEStream(program,model)}})}function validateSSEStream(program,model){let streamOf=getStreamOf(program,model);if(!streamOf)return;let contentTypeProperty=model.properties.get("contentType");if(!contentTypeProperty)return;let[contentTypes]=getContentTypes(contentTypeProperty);if(!contentTypes.includes("text/event-stream"))return;if(streamOf.kind!=="Union"){reportDiagnostic5(program,{code:"sse-stream-union-not-events",target:model});return}if(!isEvents(program,streamOf))reportDiagnostic5(program,{code:"sse-stream-union-not-events",target:model})}var $decorators7={"TypeSpec.SSE":{terminalEvent:$terminalEventDecorator}};var exports_decorators={};__export(exports_decorators,{namespace:()=>namespace,getVersion:()=>getVersion,getUseDependencies:()=>getUseDependencies,getTypeChangedFrom:()=>getTypeChangedFrom,getReturnTypeChangedFrom:()=>getReturnTypeChangedFrom,getRenamedFromVersions:()=>getRenamedFromVersions,getRenamedFrom:()=>getRenamedFrom,getRemovedOnVersions:()=>getRemovedOnVersions,getMadeRequiredOn:()=>getMadeRequiredOn,getMadeOptionalOn:()=>getMadeOptionalOn,getAddedOnVersions:()=>getAddedOnVersions,findVersionedNamespace:()=>findVersionedNamespace,VersionMap:()=>VersionMap,$versioned:()=>$versioned,$useDependency:()=>$useDependency,$typeChangedFrom:()=>$typeChangedFrom,$returnTypeChangedFrom:()=>$returnTypeChangedFrom,$renamedFrom:()=>$renamedFrom,$removed:()=>$removed,$madeRequired:()=>$madeRequired,$madeOptional:()=>$madeOptional,$added:()=>$added});var $lib7=createTypeSpecLibrary({name:"@typespec/versioning",diagnostics:{"versioned-dependency-tuple":{severity:"error",messages:{default:"Versioned dependency mapping must be a tuple [SourceVersion, TargetVersion]."}},"versioned-dependency-tuple-enum-member":{severity:"error",messages:{default:"Versioned dependency mapping must be between enum members."}},"versioned-dependency-same-namespace":{severity:"error",messages:{default:"Versioned dependency mapping must all point to the same namespace but 2 versions have different namespaces 'namespace1' and 'namespace2'."}},"versioned-dependency-not-picked":{severity:"error",messages:{default:paramMessage`The versionedDependency decorator must provide a version of the dependency '${"dependency"}'.`}},"version-not-found":{severity:"error",messages:{default:paramMessage`The provided version '${"version"}' from '${"enumName"}' is not declared as a version enum. Use '@versioned(${"enumName"})' on the containing namespace.`}},"version-duplicate":{severity:"error",messages:{default:paramMessage`Multiple versions from '${"name"}' resolve to the same value. Version enums must resolve to unique values.`}},"invalid-renamed-from-value":{severity:"error",messages:{default:"@renamedFrom.oldName cannot be empty string."}},"incompatible-versioned-reference":{severity:"error",messages:{default:paramMessage`'${"sourceName"}' is referencing versioned type '${"targetName"}' but is not versioned itself.`,addedAfter:paramMessage`'${"sourceName"}' was added in version '${"sourceAddedOn"}' but referencing type '${"targetName"}' added in version '${"targetAddedOn"}'.`,dependentAddedAfter:paramMessage`'${"sourceName"}' was added in version '${"sourceAddedOn"}' but contains type '${"targetName"}' added in version '${"targetAddedOn"}'.`,removedBefore:paramMessage`'${"sourceName"}' was removed in version '${"sourceRemovedOn"}' but referencing type '${"targetName"}' removed in version '${"targetRemovedOn"}'.`,dependentRemovedBefore:paramMessage`'${"sourceName"}' was removed in version '${"sourceRemovedOn"}' but contains type '${"targetName"}' removed in version '${"targetRemovedOn"}'.`,versionedDependencyAddedAfter:paramMessage`'${"sourceName"}' is referencing type '${"targetName"}' added in version '${"targetAddedOn"}' but version used is '${"dependencyVersion"}'.`,versionedDependencyRemovedBefore:paramMessage`'${"sourceName"}' is referencing type '${"targetName"}' removed in version '${"targetAddedOn"}' but version used is '${"dependencyVersion"}'.`,doesNotExist:paramMessage`'${"sourceName"}' is referencing type '${"targetName"}' which does not exist in version '${"version"}'.`}},"incompatible-versioned-namespace-use-dependency":{severity:"error",messages:{default:"The useDependency decorator can only be used on a Namespace if the namespace is unversioned. For versioned namespaces, put the useDependency decorator on the version enum members."}},"made-optional-not-optional":{severity:"error",messages:{default:paramMessage`Property '${"name"}' marked with @madeOptional but is required. Should be '${"name"}?'`}},"made-required-optional":{severity:"error",messages:{default:paramMessage`Property '${"name"}?' marked with @madeRequired but is optional. Should be '${"name"}'`}},"renamed-duplicate-property":{severity:"error",messages:{default:paramMessage`Property '${"name"}' marked with '@renamedFrom' conflicts with existing property in version ${"version"}.`}}},state:{versionIndex:{description:"Version index"},addedOn:{description:"State for @addedOn decorator"},removedOn:{description:"State for @removedOn decorator"},versions:{description:"State for @versioned decorator"},useDependencyNamespace:{description:"State for @useDependency decorator on Namespaces"},useDependencyEnum:{description:"State for @useDependency decorator on Enums"},renamedFrom:{description:"State for @renamedFrom decorator"},madeOptional:{description:"State for @madeOptional decorator"},madeRequired:{description:"State for @madeRequired decorator"},typeChangedFrom:{description:"State for @typeChangedFrom decorator"},returnTypeChangedFrom:{description:"State for @returnTypeChangedFrom decorator"}}}),{reportDiagnostic:reportDiagnostic8,createStateSymbol:createStateSymbol3,stateKeys:VersioningStateKeys}=$lib7;var exports_validate={};__export(exports_validate,{getCachedNamespaceDependencies:()=>getCachedNamespaceDependencies,$onValidate:()=>$onValidate5});function getVersionAdditionCodefixes(version,type,program,typeOptions){if(typeof version==="string")return getVersionAdditionCodeFixFromString(version,type,program,typeOptions);return getVersionAdditionCodeFixFromVersion(version,type,typeOptions)}function getVersionAdditionCodeFixFromVersion(version,type,typeOptions){if(type.node===void 0)return;let enumMember=version.enumMember,decoratorDeclaration=`@added(${enumMember.enum.name}.${enumMember.name})`;return[getDecorationAdditionCodeFix("add-version-to-type",decoratorDeclaration,getTypeName(type,typeOptions),getSourceLocation(type.node))]}function getVersionAdditionCodeFixFromString(version,type,program,typeOptions){let targetVersion=getAllVersions(program,type)?.find((v)=>v.value===version);if(targetVersion===void 0)return;return getVersionAdditionCodeFixFromVersion(targetVersion,type,typeOptions)}function getVersionRemovalCodeFixes(version,type,program,typeOptions){if(type.node===void 0)return;let targetVersion=getAllVersions(program,type)?.find((v)=>v.value===version);if(targetVersion===void 0)return;let enumMember=targetVersion.enumMember,decoratorDeclaration=`@removed(${enumMember.enum.name}.${enumMember.name})`;return[getDecorationAdditionCodeFix("remove-version-from-type",decoratorDeclaration,getTypeName(type,typeOptions),getSourceLocation(type.node))]}function getDecorationAdditionCodeFix(id,decoratorDeclaration,typeName,location){return{id,label:`Add '${decoratorDeclaration}' to '${typeName}'`,fix:(context)=>{return context.prependText(location,`${decoratorDeclaration}
`)}}}var relationCacheKey=Symbol.for("TypeSpec.Versioning.NamespaceRelationCache");function getCachedNamespaceDependencies(program){return program[relationCacheKey]}function $onValidate5(program){let namespaceDependencies=new Map;function addNamespaceDependency(source,target){if(!target||!("namespace"in target)||!target.namespace)return;let set=namespaceDependencies.get(source)??new Set;if(target.namespace!==source)set.add(target.namespace);namespaceDependencies.set(source,set)}program[relationCacheKey]=namespaceDependencies,navigateProgram(program,{model:(model)=>{if(isTemplateInstance(model))return;if(isTemplateDeclaration(model))return;if(!model.name)return;addNamespaceDependency(model.namespace,model.sourceModel),addNamespaceDependency(model.namespace,model.baseModel);for(let prop of model.properties.values()){if(addNamespaceDependency(model.namespace,prop.type),validateTargetVersionCompatible(program,model,prop,{isTargetADependent:!0}),getTypeChangedFrom(program,prop)!==void 0)validateMultiTypeReference(program,prop);else validateReference(program,prop,prop.type);validateMadeOptional(program,prop),validateMadeRequired(program,prop)}validateVersionedPropertyNames(program,model)},union:(union)=>{if(isTemplateInstance(union))return;if(isTemplateDeclaration(union))return;if(union.namespace===void 0)return;for(let variant of union.variants.values())addNamespaceDependency(union.namespace,variant.type);validateVersionedPropertyNames(program,union)},operation:(op)=>{if(isTemplateInstance(op))return;if(isTemplateDeclaration(op))return;let namespace=op.namespace??op.interface?.namespace;if(addNamespaceDependency(namespace,op.sourceOperation),addNamespaceDependency(namespace,op.returnType),op.interface)validateTargetVersionCompatible(program,op.interface,op,{isTargetADependent:!0});validateReference(program,op,op.returnType);for(let sourceModel of op.parameters.sourceModels)validateReference(program,op,sourceModel.model);for(let prop of op.parameters.properties.values())if(validateTargetVersionCompatible(program,op,prop,{isTargetADependent:!0}),getTypeChangedFrom(program,prop)!==void 0)validateMultiTypeReference(program,prop);else validateReference(program,[prop,op],prop.type)},interface:(iface)=>{if(isTemplateDeclaration(iface))return;for(let source of iface.sourceInterfaces)validateReference(program,iface,source)},namespace:(namespace)=>{validateVersionEnumValuesUnique(program,namespace);let versionedNamespace=findVersionedNamespace(program,namespace),dependencies=getVersionDependencies(program,namespace);if(dependencies===void 0)return;for(let[dependencyNs,value]of dependencies.entries())if(versionedNamespace){if(getUseDependencies(program,namespace,!1)!==void 0)reportDiagnostic8(program,{code:"incompatible-versioned-namespace-use-dependency",target:namespace})}else if(value instanceof Map)reportDiagnostic8(program,{code:"versioned-dependency-not-picked",format:{dependency:getNamespaceFullName(dependencyNs)},target:namespace})},enum:(en)=>{validateVersionedPropertyNames(program,en);let useDependencies=getUseDependencies(program,en);if(!useDependencies)return;for(let[depNs,deps]of useDependencies){let set=new Set;if(deps instanceof Map)for(let val of deps.values())set.add(val.namespace);else set.add(deps.namespace);namespaceDependencies.set(depNs,set)}}},{includeTemplateDeclaration:!0})}function validateMultiTypeReference(program,source,options){let versionTypeMap=getVersionedTypeMap(program,source);if(versionTypeMap===void 0)return;for(let[version,type]of versionTypeMap){if(type===void 0)continue;validateTypeAvailability(program,version,type,source,options)}}function validateTypeAvailability(program,version,targetType,source,options){let typesToCheck=[targetType];while(typesToCheck.length){let type=typesToCheck.pop(),availability=getAvailabilityMap(program,type)?.get(version?.name)??Availability.Available;if(![Availability.Added,Availability.Available].includes(availability))reportDiagnostic8(program,{code:"incompatible-versioned-reference",messageId:"doesNotExist",format:{sourceName:getTypeName(source,options),targetName:getTypeName(type,options),version:prettyVersion(version)},target:source,codefixes:getVersionAdditionCodefixes(version,type,program,options)});if(isTemplateInstance(type)){for(let arg of type.templateMapper.args)if(isType(arg))typesToCheck.push(arg)}else if(type.kind==="Union")for(let variant of type.variants.values())if(type.expression)typesToCheck.push(variant.type);else validateTargetVersionCompatible(program,variant,variant.type);else if(type.kind==="Tuple")for(let value of type.values)typesToCheck.push(value)}}function getVersionedNameMap(program,source){let allVersions=getAllVersions(program,source);if(allVersions===void 0)return;let map2=new Map(allVersions.map((v)=>[v,void 0])),availMap=getAvailabilityMap(program,source),alwaysAvail=availMap===void 0,renamedFrom=getRenamedFrom(program,source);if(renamedFrom!==void 0)for(let rename2 of renamedFrom){let{version,oldName}=rename2,versionIndex=allVersions.indexOf(version);if(versionIndex!==-1)map2.set(allVersions[versionIndex-1],oldName)}let lastName=void 0;switch(source.kind){case"ModelProperty":lastName=source.name;break;case"UnionVariant":if(typeof source.name==="string")lastName=source.name;break;case"EnumMember":lastName=source.name;break;default:throw Error(`Not implemented '${source.kind}'.`)}for(let version of allVersions.reverse()){if(!(alwaysAvail||[Availability.Added,Availability.Available].includes(availMap.get(version.name)))){map2.set(version,void 0);continue}let mapType=map2.get(version);if(mapType!==void 0)lastName=mapType;else map2.set(version,lastName)}return map2}function getVersionedTypeMap(program,source){let allVersions=getAllVersions(program,source);if(allVersions===void 0)return;let map2=new Map(allVersions.map((v)=>[v,void 0])),availMap=getAvailabilityMap(program,source),alwaysAvail=availMap===void 0,typeChangedFrom=getTypeChangedFrom(program,source);if(typeChangedFrom!==void 0)for(let[version,type]of typeChangedFrom){let versionIndex=allVersions.indexOf(version);if(versionIndex!==-1)map2.set(allVersions[versionIndex-1],type)}let lastType;switch(source.kind){case"ModelProperty":lastType=source.type;break;default:throw Error(`Not implemented '${source.kind}'.`)}for(let version of allVersions.reverse()){if(!(alwaysAvail||[Availability.Added,Availability.Available].includes(availMap.get(version.name)))){map2.set(version,void 0);continue}let mapType=map2.get(version);if(mapType!==void 0)lastType=mapType;else map2.set(version,lastType)}return map2}function validateVersionEnumValuesUnique(program,namespace){let[_2,versionMap]=getVersions(program,namespace);if(versionMap===void 0)return;let values=new Set(versionMap.getVersions().map((v)=>v.value));if(versionMap.size!==values.size){let enumName=versionMap.getVersions()[0].enumMember.enum.name;reportDiagnostic8(program,{code:"version-duplicate",format:{name:enumName},target:namespace})}}function validateVersionedPropertyNames(program,source){let allVersions=getAllVersions(program,source);if(allVersions===void 0)return;let versionedNameMap=new Map(allVersions.map((v)=>[v,[]])),values=[];if(source.kind==="Model")values=source.properties.values();else if(source.kind==="Enum")values=source.members.values();else if(source.kind==="Union")values=source.variants.values();for(let value of values){let nameMap=getVersionedNameMap(program,value);if(nameMap===void 0)continue;for(let[version,name]of nameMap){if(name===void 0)continue;versionedNameMap.get(version)?.push(name)}}for(let[version,names]of versionedNameMap.entries()){let nameCounts=new Map;for(let name of names){let count=nameCounts.get(name)??0;nameCounts.set(name,count+1)}for(let[name,count]of nameCounts.entries()){if(name===void 0)continue;if(count>1)reportDiagnostic8(program,{code:"renamed-duplicate-property",format:{name,version:prettyVersion(version)},target:source})}}}function validateMadeOptional(program,target){if(target.kind==="ModelProperty"){if(!getMadeOptionalOn(program,target))return;if(!target.optional){reportDiagnostic8(program,{code:"made-optional-not-optional",format:{name:target.name},target});return}}}function validateMadeRequired(program,target){if(target.kind==="ModelProperty"){if(!getMadeRequiredOn(program,target))return;if(target.optional){reportDiagnostic8(program,{code:"made-required-optional",format:{name:target.name},target});return}}}function validateReference(program,source,target){if(validateTargetVersionCompatible(program,source,target),"templateMapper"in target){for(let param of target.templateMapper?.args??[])if(isType(param))validateReference(program,source,param)}let sources=Array.isArray(source)?source:[source];switch(target.kind){case"Model":if(!target.name)for(let prop of target.properties.values())validateReference(program,[prop,...sources],prop.type);break;case"Union":if(typeof target.name!=="string")for(let variant of target.variants.values())validateReference(program,source,variant.type);break;case"Tuple":for(let value of target.values)validateReference(program,source,value);break}}function resolveAvailabilityForStack(program,type){let types=Array.isArray(type)?type:[type],first=types[0],map2=getAvailabilityMapFromStack(program,types);return{type:first,map:map2}}function getAvailabilityMapFromStack(program,typeStack){for(let type of typeStack){let map2=getAvailabilityMap(program,type);if(map2)return map2;switch(type.kind){case"Operation":{let parentMap=type.interface&&getAvailabilityMap(program,type.interface);if(parentMap)return parentMap;break}case"ModelProperty":{let parentMap=type.model&&getAvailabilityMap(program,type.model);if(parentMap)return parentMap;break}}}return}function validateTargetVersionCompatible(program,source,target,validateOptions={}){let sourceAvailability=resolveAvailabilityForStack(program,source),[sourceNamespace]=getVersions(program,sourceAvailability.type);if(sourceAvailability.map===void 0){let sources=Array.isArray(source)?source:[source],baseNs=getVersions(program,sources[0]);for(let type of sources)if(getVersions(program,type)!==baseNs)return}let targetAvailability=resolveAvailabilityForStack(program,target),[targetNamespace]=getVersions(program,targetAvailability.type);if(!targetAvailability.map||!targetNamespace)return;let versionMap;if(sourceNamespace!==targetNamespace){if(versionMap=(sourceNamespace&&getVersionDependencies(program,sourceNamespace))?.get(targetNamespace),versionMap===void 0)return;if(targetAvailability.map=translateAvailability(program,targetAvailability.map,versionMap,sourceAvailability.type,targetAvailability.type),!targetAvailability.map)return}if(validateOptions.isTargetADependent)validateAvailabilityForContains(program,sourceAvailability.map,targetAvailability.map,sourceAvailability.type,targetAvailability.type);else validateAvailabilityForRef(program,sourceAvailability.map,targetAvailability.map,sourceAvailability.type,targetAvailability.type,versionMap instanceof Map?versionMap:void 0)}function translateAvailability(program,avail,versionMap,source,target){if(!(versionMap instanceof Map)){let version=versionMap;if([Availability.Removed,Availability.Unavailable].includes(avail.get(version.name))){let addedAfter=findAvailabilityAfterVersion(version.name,Availability.Added,avail),removedBefore=findAvailabilityOnOrBeforeVersion(version.name,Availability.Removed,avail);if(addedAfter)reportDiagnostic8(program,{code:"incompatible-versioned-reference",messageId:"versionedDependencyAddedAfter",format:{sourceName:getTypeName(source),targetName:getTypeName(target),dependencyVersion:prettyVersion(version),targetAddedOn:addedAfter},target:source,codefixes:getVersionAdditionCodefixes(version,target,program)});if(removedBefore)reportDiagnostic8(program,{code:"incompatible-versioned-reference",messageId:"versionedDependencyRemovedBefore",format:{sourceName:getTypeName(source),targetName:getTypeName(target),dependencyVersion:prettyVersion(version),targetAddedOn:removedBefore},target:source,codefixes:getVersionAdditionCodefixes(version,target,program)})}return}else{let newAvail=new Map;for(let[key,val]of versionMap){let isAvail=avail.get(val.name);newAvail.set(key.name,isAvail)}return newAvail}}function findAvailabilityAfterVersion(version,status,avail){let search=!1;for(let[key,val]of avail){if(version===key){search=!0;continue}if(!search)continue;if(val===status)return key}return}function findAvailabilityOnOrBeforeVersion(version,status,avail){let search=!1;for(let[key,val]of avail){if([Availability.Added,Availability.Added].includes(val))search=!0;if(!search)continue;if(val===status)return key;if(key===version)break}return}function validateAvailabilityForRef(program,sourceAvail,targetAvail,source,target,versionMap){if(sourceAvail===void 0){if(!isAvailableInAllVersion(targetAvail)){let firstAvailableVersion=Array.from(targetAvail.entries()).filter(([_2,val])=>val===Availability.Available||val===Availability.Added).map(([key,_2])=>key).sort().shift();reportDiagnostic8(program,{code:"incompatible-versioned-reference",messageId:"default",format:{sourceName:getTypeName(source),targetName:getTypeName(target)},target:source,codefixes:firstAvailableVersion?getVersionAdditionCodefixes(firstAvailableVersion,source,program):void 0})}return}let keyValSource=[...sourceAvail.keys(),...targetAvail.keys()],sourceTypeChanged=getTypeChangedFrom(program,source);if(sourceTypeChanged!==void 0){let sourceTypeChangedKeys=[...sourceTypeChanged.keys()].map((item)=>item.name);keyValSource=[...keyValSource,...sourceTypeChangedKeys]}let sourceReturnTypeChanged=getReturnTypeChangedFrom(program,source);if(sourceReturnTypeChanged!==void 0){let sourceReturnTypeChangedKeys=[...sourceReturnTypeChanged.keys()].map((item)=>item.name);keyValSource=[...keyValSource,...sourceReturnTypeChangedKeys]}let keySet=new Set(keyValSource);for(let key of keySet){let sourceVal=sourceAvail.get(key),targetVal=targetAvail.get(key);if([Availability.Added].includes(sourceVal)&&[Availability.Removed,Availability.Unavailable].includes(targetVal)){let targetAddedOn=findAvailabilityAfterVersion(key,Availability.Added,targetAvail),targetVersion=key;if(versionMap)targetVersion=findMatchingTargetVersion(key,versionMap)??key;reportDiagnostic8(program,{code:"incompatible-versioned-reference",messageId:"addedAfter",format:{sourceName:getTypeName(source),targetName:getTypeName(target),sourceAddedOn:key,targetAddedOn},target:source,codefixes:getVersionAdditionCodefixes(targetVersion,target,program)})}if([Availability.Removed].includes(sourceVal)&&[Availability.Unavailable].includes(targetVal)){let targetRemovedOn=findAvailabilityOnOrBeforeVersion(key,Availability.Removed,targetAvail),targetVersion=key;if(versionMap)targetVersion=findMatchingTargetVersion(key,versionMap)??key;reportDiagnostic8(program,{code:"incompatible-versioned-reference",messageId:"removedBefore",format:{sourceName:getTypeName(source),targetName:getTypeName(target),sourceRemovedOn:key,targetRemovedOn},target:source,codefixes:getVersionAdditionCodefixes(targetVersion,target,program)})}}}function canIgnoreDependentVersioning(type,versioning){if(type.kind==="ModelProperty")return canIgnoreVersioningOnProperty(type,versioning);return!1}function canIgnoreVersioningOnProperty(prop,versioning){if(prop.sourceProperty===void 0)return!1;let decoratorFn=versioning==="added"?$added:$removed,selfDecorators=prop.decorators.filter((x)=>x.decorator===decoratorFn),sourceDecorators=prop.sourceProperty.decorators.filter((x)=>x.decorator===decoratorFn);return!selfDecorators.some((x)=>!sourceDecorators.some((y)=>x.node===y.node))}function validateAvailabilityForContains(program,sourceAvail,targetAvail,source,target,sourceOptions,targetOptions){if(!sourceAvail)return;let keySet=new Set([...sourceAvail.keys(),...targetAvail.keys()]);for(let key of keySet){let sourceVal=sourceAvail.get(key),targetVal=targetAvail.get(key);if(sourceVal===targetVal)continue;if([Availability.Added].includes(targetVal)&&[Availability.Removed,Availability.Unavailable].includes(sourceVal)&&!canIgnoreDependentVersioning(target,"added")){let sourceAddedOn=findAvailabilityOnOrBeforeVersion(key,Availability.Added,sourceAvail);reportDiagnostic8(program,{code:"incompatible-versioned-reference",messageId:"dependentAddedAfter",format:{sourceName:getTypeName(source,sourceOptions),targetName:getTypeName(target,targetOptions),sourceAddedOn,targetAddedOn:key},target,codefixes:getVersionAdditionCodefixes(key,source,program,targetOptions)})}if([Availability.Removed].includes(sourceVal)&&[Availability.Added,Availability.Available].includes(targetVal)&&!canIgnoreDependentVersioning(target,"removed")){let targetRemovedOn=findAvailabilityAfterVersion(key,Availability.Removed,targetAvail);reportDiagnostic8(program,{code:"incompatible-versioned-reference",messageId:"dependentRemovedBefore",format:{sourceName:getTypeName(source),targetName:getTypeName(target),sourceRemovedOn:key,targetRemovedOn},target,codefixes:getVersionRemovalCodeFixes(key,target,program,targetOptions)})}}}function isAvailableInAllVersion(avail){for(let val of avail.values())if([Availability.Removed,Availability.Unavailable].includes(val))return!1;return!0}function prettyVersion(version){return version?.value??"<n/a>"}function findMatchingTargetVersion(sourceVersion,versionMap){for(let[source,target]of versionMap.entries())if(source.value===sourceVersion)return target;return}class VersioningTimeline{#namespaces;#timeline;#momentIndex;#versionIndex;constructor(program,resolutions){let indexedVersions=new Set,namespaces=new Set,timeline=this.#timeline=resolutions.map((x)=>new TimelineMoment(x));for(let resolution of resolutions)for(let[namespace,version]of resolution.entries())indexedVersions.add(version),namespaces.add(namespace);this.#namespaces=[...namespaces];function findIndexToInsert(version){for(let[index,moment]of timeline.entries()){let versionAtMoment=moment.getVersion(version.namespace);if(versionAtMoment&&version.index<versionAtMoment.index)return index}return-1}for(let namespace of namespaces){let[,versions]=getVersions(program,namespace);if(versions===void 0)continue;for(let version of versions.getVersions())if(!indexedVersions.has(version)){indexedVersions.add(version);let index=findIndexToInsert(version),newMoment=new TimelineMoment(new Map([[version.namespace,version]]));if(index===-1)timeline.push(newMoment);else timeline.splice(index,0,newMoment)}}this.#versionIndex=new Map,this.#momentIndex=new Map;for(let[index,moment]of timeline.entries()){this.#momentIndex.set(moment,index);for(let version of moment.versions())if(!this.#versionIndex.has(version))this.#versionIndex.set(version,index)}}prettySerialize(){let hSep="-".repeat(this.#namespaces.length*13+1),content=this.#timeline.map((moment)=>{return"| "+this.#namespaces.map((x)=>(moment.getVersion(x)?.name??"").padEnd(10," ")).join(" | ")+" |"}).join(`
${hSep}
`);return["",hSep,content,hSep].join(`
`)}get(version){let index=this.getIndex(version);if(index===-1)if(version instanceof TimelineMoment)compilerAssert(!1,`Timeline moment "${version?.name}" should have been resolved`);else compilerAssert(!1,`Version "${version?.name}" from ${getTypeName(version.namespace)} should have been resolved. ${this.prettySerialize()}`);return this.#timeline[index]}getIndex(version){let index=version instanceof TimelineMoment?this.#momentIndex.get(version):this.#versionIndex.get(version);if(index===void 0)return-1;return index}isBefore(isBefore,base){let isBeforeIndex=this.getIndex(isBefore),baseIndex=this.getIndex(base);return isBeforeIndex<baseIndex}first(){return this.#timeline[0]}[Symbol.iterator](){return this.#timeline[Symbol.iterator]()}entries(){return this.#timeline.entries()}}class TimelineMoment{name;#versionMap;constructor(versionMap){this.#versionMap=versionMap,this.name=versionMap.values().next().value?.name??""}getVersion(namespace){return this.#versionMap.get(namespace)}versions(){return this.#versionMap.values()}}function getVersionDependencies(program,namespace){let explicit=getUseDependencies(program,namespace),usage=getCachedNamespaceDependencies(program)?.get(namespace);if(usage===void 0)return explicit;let result=new Map(explicit);for(let dep of usage)if(!explicit?.has(dep)){let version=getVersion(program,dep);if(version){let depVersions=version.getVersions();result.set(dep,depVersions[depVersions.length-1])}}return result}var versionCache=new WeakMap;function cacheVersion(key,versions){return versionCache.set(key,versions),versions}function getVersionsForEnum(program,en){let namespace=en.namespace;if(namespace===void 0)return[];let nsVersion=getVersion(program,namespace);if(nsVersion===void 0)return[];return[namespace,nsVersion]}function getVersions(p,t){let existing=versionCache.get(t);if(existing)return existing;switch(t.kind){case"Namespace":return resolveVersionsForNamespace(p,t);case"Operation":case"Interface":case"Model":case"Union":case"Scalar":case"Enum":if(t.namespace)return cacheVersion(t,getVersions(p,t.namespace)||[]);else if(t.kind==="Operation"&&t.interface)return cacheVersion(t,getVersions(p,t.interface)||[]);else return cacheVersion(t,[]);case"ModelProperty":if(t.sourceProperty)return getVersions(p,t.sourceProperty);else if(t.model)return getVersions(p,t.model);else return cacheVersion(t,[]);case"EnumMember":return cacheVersion(t,getVersions(p,t.enum)||[]);case"UnionVariant":return cacheVersion(t,getVersions(p,t.union)||[]);default:return cacheVersion(t,[])}}function resolveVersionsForNamespace(program,namespace){let nsVersion=getVersion(program,namespace);if(nsVersion!==void 0)return cacheVersion(namespace,[namespace,nsVersion]);let parentNamespaceVersion=namespace.namespace&&getVersions(program,namespace.namespace)[1],hasDependencies=getUseDependencies(program,namespace);if(parentNamespaceVersion||hasDependencies)return cacheVersion(namespace,[namespace,parentNamespaceVersion]);else return cacheVersion(namespace,[namespace,void 0])}function getAllVersions(p,t){let[namespace,_2]=getVersions(p,t);if(namespace===void 0)return;return getVersion(p,namespace)?.getVersions()}var Availability;(function(Availability2){Availability2.Unavailable="Unavailable",Availability2.Added="Added",Availability2.Available="Available",Availability2.Removed="Removed"})(Availability||(Availability={}));function getParentAddedVersion(program,type,versions){let parentMap=void 0;if(type.kind==="ModelProperty"&&type.model!==void 0)parentMap=getAvailabilityMap(program,type.model);else if(type.kind==="Operation"&&type.interface!==void 0)parentMap=getAvailabilityMap(program,type.interface);if(parentMap===void 0)return;for(let[key,value]of parentMap.entries())if(value===Availability.Added)return versions.find((x)=>x.name===key);return}function getParentRemovedVersion(program,type,versions){let parentMap=void 0;if(type.kind==="ModelProperty"&&type.model!==void 0)parentMap=getAvailabilityMap(program,type.model);else if(type.kind==="Operation"&&type.interface!==void 0)parentMap=getAvailabilityMap(program,type.interface);if(parentMap===void 0)return;for(let[key,value]of parentMap.entries())if(value===Availability.Removed)return versions.find((x)=>x.name===key);return}function resolveWhenFirstAdded(added,removed,parentAdded){if(!added.length&&!removed.length)return[parentAdded];if(added.length){if(!removed.length||added[0].index<removed[0].index)return added}if(removed.length){if(!added.length||removed[0].index<added[0].index)return[parentAdded,...added]}return added}function resolveRemoved(added,removed,parentRemoved){if(removed.length)return removed;let implicitlyRemoved=!added.length||parentRemoved&&added[0].index<parentRemoved.index;if(parentRemoved&&implicitlyRemoved)return[parentRemoved];return[]}function getAvailabilityMap(program,type){let avail=new Map,allVersions=getAllVersions(program,type);if(allVersions===void 0)return;let firstVersion=allVersions[0],parentAdded=getParentAddedVersion(program,type,allVersions)??firstVersion,parentRemoved=getParentRemovedVersion(program,type,allVersions),added=getAddedOnVersions(program,type)??[],removed=getRemovedOnVersions(program,type)??[],typeChanged=getTypeChangedFrom(program,type),returnTypeChanged=getReturnTypeChangedFrom(program,type);if(!added.length&&!removed.length&&typeChanged===void 0&&returnTypeChanged===void 0)return;added=resolveWhenFirstAdded(added,removed,parentAdded),removed=resolveRemoved(added,removed,parentRemoved);let isAvail=!1;for(let ver of allVersions){let add=added.find((x)=>x.index===ver.index);if(removed.find((x)=>x.index===ver.index))isAvail=!1,avail.set(ver.name,Availability.Removed);else if(add)isAvail=!0,avail.set(ver.name,Availability.Added);else if(isAvail)avail.set(ver.name,Availability.Available);else avail.set(ver.name,Availability.Unavailable)}return avail}function getVersionForEnumMember(program,member){let parentEnum=member.enum,[,versions]=getVersionsForEnum(program,parentEnum);return versions?.getVersionForEnumMember(member)}var namespace="TypeSpec.Versioning";function checkIsVersion(program,enumMember,diagnosticTarget){let version=getVersionForEnumMember(program,enumMember);if(!version)reportDiagnostic8(program,{code:"version-not-found",target:diagnosticTarget,format:{version:enumMember.name,enumName:enumMember.enum.name}});return version}var $added=(context,t,v)=>{let{program}=context,version=checkIsVersion(context.program,v,context.getArgumentTarget(0));if(!version)return;let record=program.stateMap(VersioningStateKeys.addedOn).get(t)??[];record.push(version),record.sort((a,b)=>a.index-b.index),program.stateMap(VersioningStateKeys.addedOn).set(t,record)};function $removed(context,t,v){let{program}=context,version=checkIsVersion(context.program,v,context.getArgumentTarget(0));if(!version)return;let record=program.stateMap(VersioningStateKeys.removedOn).get(t)??[];record.push(version),record.sort((a,b)=>a.index-b.index),program.stateMap(VersioningStateKeys.removedOn).set(t,record)}function getTypeChangedFrom(p,t){return p.stateMap(VersioningStateKeys.typeChangedFrom).get(t)}var $typeChangedFrom=(context,prop,v,oldType)=>{let{program}=context,version=checkIsVersion(context.program,v,context.getArgumentTarget(0));if(!version)return;let record=getTypeChangedFrom(program,prop)??new Map;record.set(version,oldType),record=new Map([...record.entries()].sort((a,b)=>a[0].index-b[0].index)),program.stateMap(VersioningStateKeys.typeChangedFrom).set(prop,record)};function getReturnTypeChangedFrom(p,t){return p.stateMap(VersioningStateKeys.returnTypeChangedFrom).get(t)}var $returnTypeChangedFrom=(context,op,v,oldReturnType)=>{let{program}=context,version=checkIsVersion(context.program,v,context.getArgumentTarget(0));if(!version)return;let record=getReturnTypeChangedFrom(program,op)??new Map;record.set(version,oldReturnType),record=new Map([...record.entries()].sort((a,b)=>a[0].index-b[0].index)),program.stateMap(VersioningStateKeys.returnTypeChangedFrom).set(op,record)},$renamedFrom=(context,t,v,oldName)=>{let{program}=context,version=checkIsVersion(context.program,v,context.getArgumentTarget(0));if(!version)return;if(oldName==="")reportDiagnostic8(program,{code:"invalid-renamed-from-value",target:t});let record=getRenamedFrom(program,t)??[];record.push({version,oldName}),record.sort((a,b)=>a.version.index-b.version.index),program.stateMap(VersioningStateKeys.renamedFrom).set(t,record)},$madeOptional=(context,t,v)=>{let{program}=context,version=checkIsVersion(context.program,v,context.getArgumentTarget(0));if(!version)return;program.stateMap(VersioningStateKeys.madeOptional).set(t,version)},$madeRequired=(context,t,v)=>{let{program}=context,version=checkIsVersion(context.program,v,context.getArgumentTarget(0));if(!version)return;program.stateMap(VersioningStateKeys.madeRequired).set(t,version)};function getMadeRequiredOn(p,t){return p.stateMap(VersioningStateKeys.madeRequired).get(t)}function getRenamedFrom(p,t){return p.stateMap(VersioningStateKeys.renamedFrom).get(t)}function getRenamedFromVersions(p,t){return getRenamedFrom(p,t)?.map((x)=>x.version)}function getAddedOnVersions(p,t){return p.stateMap(VersioningStateKeys.addedOn).get(t)}function getRemovedOnVersions(p,t){return p.stateMap(VersioningStateKeys.removedOn).get(t)}function getMadeOptionalOn(p,t){return p.stateMap(VersioningStateKeys.madeOptional).get(t)}class VersionMap{map=new Map;constructor(namespace2,enumType){let index=0;for(let member of enumType.members.values())this.map.set(member,{name:member.name,value:member.value?.toString()??member.name,enumMember:member,index,namespace:namespace2}),index++}getVersionForEnumMember(member){return this.map.get(member)}getVersions(){return[...this.map.values()]}get size(){return this.map.size}}var $versioned=(context,t,versions)=>{context.program.stateMap(VersioningStateKeys.versions).set(t,new VersionMap(t,versions))};function getVersion(program,namespace2){return program.stateMap(VersioningStateKeys.versions).get(namespace2)}function findVersionedNamespace(program,namespace2){let current=namespace2;while(current){if(program.stateMap(VersioningStateKeys.versions).has(current))return current;current=current.namespace}return}function $useDependency(context,target,...versionRecords){let versions=[];for(let record of versionRecords){let ver=checkIsVersion(context.program,record,context.getArgumentTarget(0));if(ver)versions.push(ver)}if(target.kind==="Namespace"){let state2=getNamespaceUseDependencyState(context.program,target);if(!state2)state2=versions;else state2.push(...versions);context.program.stateMap(VersioningStateKeys.useDependencyNamespace).set(target,state2)}else if(target.kind==="EnumMember"){let targetEnum=target.enum,state2=context.program.stateMap(VersioningStateKeys.useDependencyEnum).get(targetEnum);if(!state2)state2=new Map;let currentVersions=state2.get(target)??[];currentVersions.push(...versions),state2.set(target,currentVersions),context.program.stateMap(VersioningStateKeys.useDependencyEnum).set(targetEnum,state2)}}function getNamespaceUseDependencyState(program,target){return program.stateMap(VersioningStateKeys.useDependencyNamespace).get(target)}function getUseDependencies(program,target,searchEnum=!0){let result=new Map;if(target.kind==="Namespace"){let current=target;while(current){let data=getNamespaceUseDependencyState(program,current);if(!data){if(searchEnum){let versions=getVersion(program,current)?.getVersions();if(versions?.length){let enumDeps=getUseDependencies(program,versions[0].enumMember.enum);if(enumDeps)return enumDeps}}current=current.namespace}else{for(let v of data)result.set(v.namespace,v);return result}}return}else if(target.kind==="Enum"){let data=program.stateMap(VersioningStateKeys.useDependencyEnum).get(target);if(!data)return;let resolved=resolveVersionDependency(program,data);if(resolved instanceof Map)for(let[enumVer,value]of resolved)for(let val of value){let targetNamespace=val.enumMember.enum.namespace;if(!targetNamespace){reportDiagnostic8(program,{code:"version-not-found",target:val.enumMember.enum,format:{version:val.enumMember.name,enumName:val.enumMember.enum.name}});return}let subMap=result.get(targetNamespace);if(subMap)subMap.set(enumVer,val);else subMap=new Map([[enumVer,val]]);result.set(targetNamespace,subMap)}}return result}function resolveVersionDependency(program,data){if(!(data instanceof Map))return data;let mapping=new Map;for(let[key,value]of data){let sourceVersion=getVersionForEnumMember(program,key);if(sourceVersion!==void 0)mapping.set(sourceVersion,value)}return mapping}var exports_tsp_index11={};__export(exports_tsp_index11,{$onValidate:()=>$onValidate6,$lib:()=>$lib8,$decorators:()=>$decorators8});var $lib8=createTypeSpecLibrary({name:"@typespec/rest",diagnostics:{"not-key-type":{severity:"error",messages:{default:"Cannot copy keys from a non-key type (KeysOf<T> or ParentKeysOf<T>)"}},"resource-missing-key":{severity:"error",messages:{default:paramMessage`Type '${"modelName"}' is used as a resource and therefore must have a key. Use @key to designate a property as the key.`}},"resource-missing-error":{severity:"error",messages:{default:paramMessage`Type '${"modelName"}' is used as an error and therefore must have the @error decorator applied.`}},"duplicate-key":{severity:"error",messages:{default:paramMessage`More than one key found on model type ${"resourceName"}`}},"duplicate-parent-key":{severity:"error",messages:{default:paramMessage`Resource type '${"resourceName"}' has a key property named '${"keyName"}' which conflicts with the key name of a parent or child resource.`}},"invalid-action-name":{severity:"error",messages:{default:"Action name cannot be empty string."}},"shared-route-unspecified-action-name":{severity:"error",messages:{default:paramMessage`An operation marked as '@sharedRoute' must have an explicit collection action name passed to '${"decoratorName"}'.`}},"circular-parent-resource":{severity:"error",messages:{default:paramMessage`Resource has a parent cycle (${"cycle"})`}}}}),{reportDiagnostic:reportDiagnostic9,createDiagnostic:createDiagnostic6,createStateSymbol:createStateSymbol4}=$lib8;class CycleTracker{#items=[];add(type){let existingIndex=this.#items.indexOf(type);if(existingIndex!==-1)return this.#items.slice(existingIndex);this.#items.push(type);return}}var resourceKeysKey=createStateSymbol4("resourceKeys"),resourceTypeForKeyParamKey=createStateSymbol4("resourceTypeForKeyParam");function setResourceTypeKey(program,resourceType,keyProperty){program.stateMap(resourceKeysKey).set(resourceType,{resourceType,keyProperty})}function getResourceTypeKey(program,resourceType){let resourceKey=program.stateMap(resourceKeysKey).get(resourceType);if(resourceKey)return resourceKey;if(resourceType.properties.forEach((p)=>{if(isKey(program,p))if(resourceKey)reportDiagnostic9(program,{code:"duplicate-key",format:{resourceName:resourceType.name},target:p});else resourceKey={resourceType,keyProperty:p},setResourceTypeKey(program,resourceType,resourceKey.keyProperty)}),resourceKey===void 0&&resourceType.baseModel!==void 0){if(resourceKey=getResourceTypeKey(program,resourceType.baseModel),resourceKey!==void 0)setResourceTypeKey(program,resourceType,resourceKey.keyProperty)}return resourceKey}function $resourceTypeForKeyParam(context,entity,resourceType){context.program.stateMap(resourceTypeForKeyParamKey).set(entity,resourceType)}setTypeSpecNamespace("Private",$resourceTypeForKeyParam);var VISIBILITY_DECORATORS=[$visibility,$invisible,$removeVisibility];function cloneKeyProperties(context,target,resourceType){let{program}=context,parentType=getParentResource(program,resourceType);if(parentType)cloneKeyProperties(context,target,parentType);let resourceKey=getResourceTypeKey(program,resourceType);if(resourceKey){let{keyProperty}=resourceKey,keyName=getKey(program,keyProperty),decorators=[...keyProperty.decorators.filter((d)=>VISIBILITY_DECORATORS.every((visDec)=>d.decorator.name!==visDec.name)),{decorator:$resourceTypeForKeyParam,args:[{node:target.node,value:resourceType,jsValue:resourceType}]}];if(!keyProperty.decorators.some((d)=>d.decorator.name===$path.name))decorators.push({decorator:$path,args:[]});let newProp=program.checker.cloneType(keyProperty,{name:keyName,decorators,optional:!1,model:target,sourceProperty:void 0});target.properties.set(keyName,newProp)}}function $copyResourceKeyParameters(context,entity,filter){let reportNoKeyError=()=>reportDiagnostic9(context.program,{code:"not-key-type",target:entity}),templateArguments=entity.templateMapper?.args;if(!templateArguments||templateArguments.length!==1)return reportNoKeyError();if(templateArguments[0].kind!=="Model"){if(isErrorType(templateArguments[0]))return;return reportNoKeyError()}let resourceType=templateArguments[0];if(filter==="parent"){let parentType=getParentResource(context.program,resourceType);if(parentType)cloneKeyProperties(context,entity,parentType)}else cloneKeyProperties(context,entity,resourceType)}var[getParentResource,setParentResource]=useStateMap(createStateSymbol4("parentResourceTypes")),$parentResource=(context,entity,parentType)=>{let{program}=context;if(!checkCircularParentResource(program,entity,parentType))return;setParentResource(program,entity,parentType)};function checkCircularParentResource(program,entity,parentType){let cycleTracker=new CycleTracker;cycleTracker.add(entity);let currentType=parentType;while(currentType){let cycle=cycleTracker.add(currentType);if(cycle){for(let type of cycle)cycleTracker.add(type),reportDiagnostic9(program,{code:"circular-parent-resource",format:{cycle:[...cycle,cycle[0]].map((x)=>getTypeName(x)).join(" -> ")},target:type});return!1}currentType=getParentResource(program,currentType)}return!0}var getStreamOf2;try{getStreamOf2=(await import("./chunk-6sq5f96k.js")).getStreamOf}catch{getStreamOf2=()=>{throw Error("@typespec/streams was not found")}}function addActionFragment(program,target,pathFragments){let actionSegment=getActionSegment(program,target);if(actionSegment&&actionSegment!==""){let actionSeparator=getActionSeparator(program,target)??"/";pathFragments.push(`${actionSeparator}${actionSegment}`)}}function addSegmentFragment(program,target,pathFragments){let segment=getSegment(program,target);if(segment&&segment!=="")pathFragments.push(`/${segment}`)}var resourceOperationToVerb={read:"get",create:"post",createOrUpdate:"patch",createOrReplace:"put",update:"patch",delete:"delete",list:"get"};function getResourceOperationHttpVerb(program,operation){let resourceOperation=getResourceOperation(program,operation);return getOperationVerb(program,operation)??(resourceOperation&&resourceOperationToVerb[resourceOperation.operation])??(getActionDetails(program,operation)||getCollectionActionDetails(program,operation)?"post":void 0)}function autoRouteProducer(program,operation,parentSegments,overloadBase,options){let diagnostics=createDiagnosticCollector(),routePath=getRoutePath(program,operation)?.path,segments=[...parentSegments,...routePath?[routePath]:[]],filteredParameters=[],filteredParamProperties=new Set,paramOptions={...options?.paramOptions??{},verbSelector:getResourceOperationHttpVerb},parameters=diagnostics.pipe(getOperationParameters(program,operation,"",void 0,paramOptions));for(let httpParam of parameters.parameters){let{type,param}=httpParam;if(type==="path"){addSegmentFragment(program,param,segments);let filteredParam=options.autoRouteOptions?.routeParamFilter?.(operation,param);if(filteredParam?.routeParamString){if(segments.push(`/${filteredParam.routeParamString}`),filteredParam?.excludeFromOperationParams===!0)continue}else if(param.type.kind==="String"){segments.push(`${param.type.value}`);continue}else segments.push(`${getUriTemplatePathParam(httpParam)}`)}filteredParameters.push(httpParam),filteredParamProperties.add(httpParam.param)}parameters.parameters=filteredParameters;for(let i=parameters.properties.length-1;i>=0;i--){let httpProp=parameters.properties[i];if(!["header","query","path","cookie"].includes(httpProp.kind))continue;if(!filteredParamProperties.has(httpProp.property))parameters.properties.splice(i,1)}addSegmentFragment(program,operation,segments),addActionFragment(program,operation,segments);let pathPart=joinPathSegments(segments);return diagnostics.wrap({uriTemplate:addQueryParamsToUriTemplate(pathPart,filteredParameters),parameters:{...parameters,parameters:filteredParameters}})}var autoRouteKey=createStateSymbol4("autoRoute"),$autoRoute=(context,entity)=>{if(entity.kind==="Operation")setRouteProducer(context.program,entity,autoRouteProducer);else for(let[_2,op]of entity.operations)context.call($autoRoute,op),op.decorators.push({decorator:$autoRoute,args:[]});context.program.stateSet(autoRouteKey).add(entity)};var segmentsKey=createStateSymbol4("segments");function $segment(context,entity,name){context.program.stateMap(segmentsKey).set(entity,name)}function getResourceSegment(program,resourceType){let resourceKey=getResourceTypeKey(program,resourceType);return resourceKey?getSegment(program,resourceKey.keyProperty):getSegment(program,resourceType)}var $segmentOf=(context,entity,resourceType)=>{if(resourceType.kind==="TemplateParameter")return;let segment=getResourceSegment(context.program,resourceType);if(segment)context.call($segment,entity,segment)};function getSegment(program,entity){return program.stateMap(segmentsKey).get(entity)}var actionSeparatorKey=createStateSymbol4("actionSeparator"),$actionSeparator=(context,entity,separator)=>{context.program.stateMap(actionSeparatorKey).set(entity,separator)};function getActionSeparator(program,entity){let stateMap=program.stateMap(actionSeparatorKey),directSeparator=stateMap.get(entity);if(directSeparator!==void 0)return directSeparator;if(entity.kind==="Operation"){if(entity.interface){let interfaceSeparator=stateMap.get(entity.interface);if(interfaceSeparator!==void 0)return interfaceSeparator;if(entity.interface.namespace)return getNamespaceActionSeparator(program,entity.interface.namespace)}if(entity.namespace)return getNamespaceActionSeparator(program,entity.namespace)}if(entity.kind==="Interface"&&entity.namespace)return getNamespaceActionSeparator(program,entity.namespace);return}function getNamespaceActionSeparator(program,namespace2){let separator=program.stateMap(actionSeparatorKey).get(namespace2);if(separator!==void 0)return separator;if(namespace2.namespace)return getNamespaceActionSeparator(program,namespace2.namespace);return}var $resource=(context,entity,collectionName)=>{let key=getResourceTypeKey(context.program,entity);if(!key){reportDiagnostic9(context.program,{code:"resource-missing-key",format:{modelName:entity.name},target:entity});return}context.call($segment,key.keyProperty,collectionName),key.keyProperty.decorators.push({decorator:$segment,args:[{value:context.program.checker.createLiteralType(collectionName),jsValue:collectionName}]})},resourceOperationsKey=createStateSymbol4("resourceOperations");function resourceRouteProducer(program,operation,parentSegments,overloadBase,options){let paramOptions={...options?.paramOptions??{},verbSelector:getResourceOperationHttpVerb};return DefaultRouteProducer(program,operation,parentSegments,overloadBase,{...options,paramOptions})}function setResourceOperation(context,entity,resourceType,operation){if(resourceType.kind==="TemplateParameter")return;if(context.program.stateMap(resourceOperationsKey).set(entity,{operation,resourceType}),!getRouteProducer(context.program,entity))setRouteProducer(context.program,entity,resourceRouteProducer)}function getResourceOperation(program,typespecOperation){return program.stateMap(resourceOperationsKey).get(typespecOperation)}var $readsResource=(context,entity,resourceType)=>{setResourceOperation(context,entity,resourceType,"read")};function $createsResource(context,entity,resourceType){context.call($segmentOf,entity,resourceType),setResourceOperation(context,entity,resourceType,"create")}function $createsOrReplacesResource(context,entity,resourceType){setResourceOperation(context,entity,resourceType,"createOrReplace")}function $createsOrUpdatesResource(context,entity,resourceType){setResourceOperation(context,entity,resourceType,"createOrUpdate")}function $updatesResource(context,entity,resourceType){setResourceOperation(context,entity,resourceType,"update")}function $deletesResource(context,entity,resourceType){setResourceOperation(context,entity,resourceType,"delete")}var $listsResource=(context,entity,resourceType)=>{context.call($segmentOf,entity,resourceType),setResourceOperation(context,entity,resourceType,"list")};function lowerCaseFirstChar(str){return str[0].toLocaleLowerCase()+str.substring(1)}function makeActionName(op,name){return{name:lowerCaseFirstChar(name||op.name),kind:name?"specified":"automatic"}}var actionsSegmentKey=createStateSymbol4("actionSegment"),$actionSegment=(context,entity,name)=>{context.program.stateMap(actionsSegmentKey).set(entity,name)};function getActionSegment(program,entity){return program.stateMap(actionsSegmentKey).get(entity)}var actionsKey=createStateSymbol4("actions"),$action=(context,entity,name)=>{if(name===""){reportDiagnostic9(context.program,{code:"invalid-action-name",target:entity});return}let action=makeActionName(entity,name);context.call($actionSegment,entity,action.name),context.program.stateMap(actionsKey).set(entity,action)};function getActionDetails(program,operation){return program.stateMap(actionsKey).get(operation)}var collectionActionsKey=createStateSymbol4("collectionActions"),$collectionAction=(context,entity,resourceType,name)=>{if(resourceType.kind==="TemplateParameter")return;let segment=getResourceSegment(context.program,resourceType);if(segment)context.call($segment,entity,segment);let action=makeActionName(entity,name);context.call($actionSegment,entity,action.name),action.name=`${segment}/${action.name}`,context.program.stateMap(collectionActionsKey).set(entity,action)};function getCollectionActionDetails(program,operation){return program.stateMap(collectionActionsKey).get(operation)}var resourceLocationsKey=createStateSymbol4("resourceLocations"),$resourceLocation=(context,entity,resourceType)=>{if(resourceType.kind==="TemplateParameter")return;context.program.stateMap(resourceLocationsKey).set(entity,resourceType)};setTypeSpecNamespace("Private",$resourceLocation,$actionSegment,getActionSegment);function checkForDuplicateResourceKeyNames(program){let seenTypes=new Set;function checkResourceModelKeys(model){if(model.name==="")return;let visited=new Set,currentType=model,keyProperties=new DuplicateTracker;while(currentType){if(visited.has(currentType))break;else visited.add(currentType);let resourceKey=getResourceTypeKey(program,currentType);if(resourceKey){let keyName=getKey(program,resourceKey.keyProperty);keyProperties.track(keyName,resourceKey)}currentType=getParentResource(program,currentType)}for(let[keyName,dupes]of keyProperties.entries())for(let key of dupes){let fullName=`${getTypeName(key.resourceType)}.${keyName}`;if(!seenTypes.has(fullName))seenTypes.add(fullName),reportDiagnostic9(program,{code:"duplicate-parent-key",format:{resourceName:key.resourceType.name,keyName},target:key.keyProperty})}}for(let service of listServices(program))navigateTypesInNamespace(service.type,{model:(model)=>checkResourceModelKeys(model)})}function checkForSharedRouteUnnamedActions(program){for(let service of listServices(program))navigateTypesInNamespace(service.type,{operation:(op)=>{let actionDetails=getActionDetails(program,op);if(isSharedRoute(program,op)&&(actionDetails?.kind==="automatic"||getCollectionActionDetails(program,op)?.kind==="automatic"))reportDiagnostic9(program,{code:"shared-route-unspecified-action-name",target:op,format:{decoratorName:actionDetails?"@action":"@collectionAction"}})}})}function $onValidate6(program){checkForDuplicateResourceKeyNames(program),checkForSharedRouteUnnamedActions(program)}var $decorators8={"TypeSpec.Rest":{autoRoute:$autoRoute,segment:$segment,segmentOf:$segmentOf,actionSeparator:$actionSeparator,resource:$resource,parentResource:$parentResource,readsResource:$readsResource,createsResource:$createsResource,createsOrReplacesResource:$createsOrReplacesResource,createsOrUpdatesResource:$createsOrUpdatesResource,updatesResource:$updatesResource,deletesResource:$deletesResource,listsResource:$listsResource,action:$action,collectionAction:$collectionAction,copyResourceKeyParameters:$copyResourceKeyParameters}};var exports_internal_decorators={};__export(exports_internal_decorators,{namespace:()=>namespace2,$decorators:()=>$decorators9});var namespace2="TypeSpec.Rest.Private",validatedMissingKey=createStateSymbol4("validatedMissing"),$validateHasKey=(context,target,value)=>{if(context.program.stateSet(validatedMissingKey).has(value))return;if((value.kind==="Model"&&getResourceTypeKey(context.program,value))===void 0)reportDiagnostic9(context.program,{code:"resource-missing-key",format:{modelName:getTypeName(value)},target:value}),context.program.stateSet(validatedMissingKey).add(value)},validatedErrorKey=createStateSymbol4("validatedError"),$validateIsError=(context,target,value)=>{if(context.program.stateSet(validatedErrorKey).has(value))return;if(!(value.kind==="Model"&&isErrorModel(context.program,value)))reportDiagnostic9(context.program,{code:"resource-missing-error",format:{modelName:getTypeName(value)},target:value}),context.program.stateSet(validatedErrorKey).add(value)},$decorators9={"TypeSpec.Rest.Private":{actionSegment:$actionSegment,resourceLocation:$resourceLocation,resourceTypeForKeyParam:$resourceTypeForKeyParam,validateHasKey:$validateHasKey,validateIsError:$validateIsError}};var exports_tsp_index12={};__export(exports_tsp_index12,{$lib:()=>$lib9,$decorators:()=>$decorators10});var $lib9=createTypeSpecLibrary({name:"@typespec/openapi",diagnostics:{"invalid-extension-key":{severity:"error",messages:{default:paramMessage`OpenAPI extension must start with 'x-' but was '${"value"}'`}},"duplicate-type-name":{severity:"error",messages:{default:paramMessage`Duplicate type name: '${"value"}'. Check @friendlyName decorators and overlap with types in TypeSpec or service namespace.`,parameter:paramMessage`Duplicate parameter key: '${"value"}'. Check @friendlyName decorators and overlap with types in TypeSpec or service namespace.`}},"not-url":{severity:"error",messages:{default:paramMessage`${"property"}: ${"value"} is not a valid URL.`}},"duplicate-tag":{severity:"error",messages:{default:paramMessage`"Metadata for tag '${"tagName"}' was specified twice."`}},"mixed-tag-metadata-form":{severity:"error",messages:{default:'Cannot mix the array form and the inline form of @tagMetadata on the same namespace. Use either @tagMetadata(#[...]) or multiple @tagMetadata("name", #{...}) calls, not both.'}},"tag-metadata-array-with-metadata-arg":{severity:"error",messages:{default:"When using the array form of @tagMetadata, the second argument (tagMetadata) must not be provided. Include all tag metadata inside the array elements."}},"tag-metadata-target-service":{severity:"error",messages:{default:paramMessage`@tagMetadata must be used on the service namespace. Did you mean to annotate '${"namespace"}'  with '@service'?`}},"default-response-with-status-code":{severity:"warning",messages:{statusCode:"@defaultResponse should not be used on a model that already has a status code defined. The status code will be ignored in favor of the default response.",error:"@defaultResponse should not be used on a model that is marked with @error. Use either @defaultResponse or @error, not both."}},"license-url-identifier-conflict":{severity:"error",messages:{default:"License 'url' and 'identifier' are mutually exclusive. Specify only one of them."}}},state:{tagsMetadata:{description:"State for the @tagMetadata decorator."}}}),{createDiagnostic:createDiagnostic7,reportDiagnostic:reportDiagnostic10,createStateSymbol:createStateSymbol5,stateKeys:OpenAPIKeys}=$lib9;function isOpenAPIExtensionKey(key){return key.startsWith("x-")}function validateIsUri(program,target,url,propertyName){try{return new URL(url),!0}catch{return reportDiagnostic10(program,{code:"not-url",target,format:{property:propertyName,value:url}}),!1}}function validateAdditionalInfoModel(program,target,jsonObject,reference){let propertyModel=program.resolveTypeReference(reference)[0];if(jsonObject&&propertyModel){let diagnostics=checkNoAdditionalProperties(jsonObject,target,propertyModel);if(program.reportDiagnostics(diagnostics),diagnostics.length>0)return!1}return!0}function checkNoAdditionalProperties(jsonObject,target,source){let targetNode=getObjectLiteralNode(target);return checkNoAdditionalPropertiesInternal(jsonObject,target,source,targetNode)}function checkNoAdditionalPropertiesInternal(jsonObject,target,source,targetNode){let diagnostics=[];for(let name of Object.keys(jsonObject)){let sourceProperty=getProperty(source,name),propertyNode=getObjectLiteralProperty(targetNode,name);if(sourceProperty){if(sourceProperty.type.kind==="Model"){let nestedTarget=getObjectLiteralNode(propertyNode?.value),nestedDiagnostics=checkNoAdditionalPropertiesInternal(jsonObject[name],propertyNode?.value??target,sourceProperty.type,nestedTarget);diagnostics.push(...nestedDiagnostics)}}else if(!isOpenAPIExtensionKey(name))diagnostics.push(createDiagnostic7({code:"invalid-extension-key",format:{value:name},target:propertyNode?.id??target}))}return diagnostics}function getObjectLiteralNode(target){return target!==void 0&&"kind"in target&&target.kind===SyntaxKind.ObjectLiteral?target:void 0}function getObjectLiteralProperty(node,name){return node?.properties.find((property)=>property.kind===SyntaxKind.ObjectLiteralProperty&&property.id.sv===name)}var[getOperationId,setOperationId]=useStateMap(createStateSymbol5("operationIds")),$operationId=(context,entity,opId)=>{setOperationId(context.program,entity,opId)},openApiExtensionKey=createStateSymbol5("openApiExtension"),$extension2=(context,entity,extensionName,value)=>{compilerAssert(!value||!isType(value),"OpenAPI extension value must be a value but was a type",context.getArgumentTarget(1));let processed=convertRemainingValuesToExtensions2(context.program,value);setExtension2(context.program,entity,extensionName,processed)};function convertRemainingValuesToExtensions2(program,value){switch(typeof value){case"string":case"number":case"boolean":return value;case"object":if(value===null)return null;if(Array.isArray(value))return value.map((x)=>convertRemainingValuesToExtensions2(program,x));if(isTypeSpecValue2(value))return serializeValueAsJson(program,value,value.type);else return Object.fromEntries(Object.entries(value).filter(([,val])=>val!==void 0).map(([key,val])=>[key,convertRemainingValuesToExtensions2(program,val)]));default:return value}}function isTypeSpecValue2(value){return"entityKind"in value&&value.entityKind==="Value"}function setInfo(program,entity,data){program.stateMap(infoKey).set(entity,data)}function setExtension2(program,entity,extensionName,data){let openApiExtensions=program.stateMap(openApiExtensionKey),typeExtensions=openApiExtensions.get(entity)??new Map;typeExtensions.set(extensionName,data),openApiExtensions.set(entity,typeExtensions)}var defaultResponseKey=createStateSymbol5("defaultResponse"),$defaultResponse=(context,entity)=>{return setStatusCode(context.program,entity,["*"]),context.program.stateSet(defaultResponseKey).add(entity),{onTargetFinish:()=>{let diagnostics=[];for(let prop of entity.properties.values())if(isStatusCode(context.program,prop)){diagnostics.push(createDiagnostic7({code:"default-response-with-status-code",messageId:"statusCode",target:entity}));break}if(isErrorModel(context.program,entity))diagnostics.push(createDiagnostic7({code:"default-response-with-status-code",messageId:"error",target:entity}));return diagnostics}}};var externalDocsKey=createStateSymbol5("externalDocs"),$externalDocs=(context,target,url,description)=>{let doc={url};if(description)doc.description=description;context.program.stateMap(externalDocsKey).set(target,doc)};var infoKey=createStateSymbol5("info"),$info=(context,entity,data)=>{if(data===void 0)return;if(!validateAdditionalInfoModel(context.program,context.getArgumentTarget(0),data,"TypeSpec.OpenAPI.AdditionalInfo"))return;if(data.termsOfService){if(!validateIsUri(context.program,context.getArgumentTarget(0),data.termsOfService,"TermsOfService"))return}if(data.license?.url!==void 0&&data.license?.identifier!==void 0){reportDiagnostic10(context.program,{code:"license-url-identifier-conflict",target:context.getArgumentTarget(0)});return}setInfo(context.program,entity,data)};var[getTagsMetadata,setTagsMetadata]=useStateMap(OpenAPIKeys.tagsMetadata),[isTagsMetadataArrayFormUsed,setTagsMetadataArrayFormUsed]=useStateSet(createStateSymbol5("tagsMetadataArrayForm")),tagMetadataDecorator=(context,entity,name,tagMetadata)=>{if(!entity.decorators.some((decorator)=>decorator.definition?.name==="@service"&&decorator.definition?.namespace.name==="TypeSpec")){reportDiagnostic10(context.program,{code:"tag-metadata-target-service",format:{namespace:entity.name},target:context.getArgumentTarget(0)});return}if(typeof name!=="string"){if(tagMetadata!==void 0){reportDiagnostic10(context.program,{code:"tag-metadata-array-with-metadata-arg",target:context.getArgumentTarget(1)});return}let existingTags=getTagsMetadata(context.program,entity);if(existingTags&&existingTags.length>0||isTagsMetadataArrayFormUsed(context.program,entity)){reportDiagnostic10(context.program,{code:"mixed-tag-metadata-form",target:context.getArgumentTarget(0)});return}let seenTags=new Set;for(let tagItem of name){if(seenTags.has(tagItem.name)){reportDiagnostic10(context.program,{code:"duplicate-tag",format:{tagName:tagItem.name},target:context.getArgumentTarget(0)});return}if(seenTags.add(tagItem.name),!validateAdditionalInfoModel(context.program,context.getArgumentTarget(0),tagItem,"TypeSpec.OpenAPI.TagMetadataWithName"))return;if(tagItem.externalDocs?.url){if(!validateIsUri(context.program,context.getArgumentTarget(0),tagItem.externalDocs.url,"externalDocs.url"))return}}setTagsMetadataArrayFormUsed(context.program,entity),setTagsMetadata(context.program,entity,[...name])}else{if(isTagsMetadataArrayFormUsed(context.program,entity)){reportDiagnostic10(context.program,{code:"mixed-tag-metadata-form",target:context.getArgumentTarget(0)});return}let tags=getTagsMetadata(context.program,entity)??[];if(tags.some((t)=>t.name===name)){reportDiagnostic10(context.program,{code:"duplicate-tag",format:{tagName:name},target:context.getArgumentTarget(0)});return}let resolvedMetadata=tagMetadata??{};if(!validateAdditionalInfoModel(context.program,context.getArgumentTarget(1),resolvedMetadata,"TypeSpec.OpenAPI.TagMetadata"))return;if(resolvedMetadata.externalDocs?.url){if(!validateIsUri(context.program,context.getArgumentTarget(1),resolvedMetadata.externalDocs.url,"externalDocs.url"))return}tags.push({name,...resolvedMetadata}),setTagsMetadata(context.program,entity,tags)}};var $decorators10={"TypeSpec.OpenAPI":{defaultResponse:$defaultResponse,extension:$extension2,externalDocs:$externalDocs,info:$info,operationId:$operationId,tagMetadata:tagMetadataDecorator}};var libraries_default={"@typespec/http":{version:"1.16.0",files:{"package.json":`{
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
`}}};var typeSpecLibraries=libraries_default,typeSpecLibraryModules={"@typespec/graphql/dist/src/tsp-index.js":exports_tsp_index4,"@typespec/json-schema/dist/src/tsp-index.js":exports_tsp_index5,"@typespec/protobuf/dist/src/tsp-index.js":exports_tsp_index6,"@typespec/openapi3/dist/src/tsp-index.js":exports_tsp_index7,"@typespec/sse/dist/src/tsp-index.js":exports_tsp_index10,"@typespec/events/dist/src/tsp-index.js":exports_tsp_index8,"@typespec/versioning/dist/src/decorators.js":exports_decorators,"@typespec/versioning/dist/src/validate.js":exports_validate,"@typespec/http/dist/src/tsp-index.js":exports_tsp_index9,"@typespec/rest/dist/src/tsp-index.js":exports_tsp_index11,"@typespec/rest/dist/src/internal-decorators.js":exports_internal_decorators,"@typespec/openapi/dist/src/tsp-index.js":exports_tsp_index12,"@typespec/streams/dist/src/tsp-index.js":exports_tsp_index3};var package_default={id:"umf.typespec",version:"0.1.0",coreVersion:"0.1.0",description:"TypeSpec source bundles with pinned syntax and supplied-file compilation",schema:{$schema:"https://json-schema.org/draft/2020-12/schema",$id:"urn:umf:typespec:0.1.0",type:"object",required:["profile","entrypoint","files"],properties:{profile:{const:"typespec-1.16.0-sources"},entrypoint:{type:"string",minLength:1},files:{type:"object",minProperties:1,propertyNames:{type:"string",minLength:1},additionalProperties:{type:"string"}},libraries:{type:"object",propertyNames:{type:"string",minLength:1},additionalProperties:{type:"string",minLength:1}}}},scopes:["element"],semantics:"CONTRACT-013. Exact source files and explicit entrypoint; compiler validity and downstream projection fidelity are separate.",capabilities:{validation:"semantic",directions:["import","export"],native:{system:"TypeSpec",version:"1.16.0",subset:"Supplied .tsp sources with pinned compiler/standard library and eleven explicitly selected official library versions; library-owned configuration retained; separate pinned JSON Schema emission API; no arbitrary project JavaScript or lossless projection claim"},evidence:["tests/typespec/native.test.ts","fixtures/typespec/compiler-oracle-results.json","spec/extensions/typespec/standard-manifest.json","tests/typespec/semantic.test.ts","fixtures/typespec/semantic-oracle-results.json","tests/typespec/corpus.test.ts","fixtures/typespec/upstream/manifest.json","fixtures/typespec/corpus-results.json","fixtures/typespec/corpus-oracle-results.json","tests/typespec/libraries.test.ts","spec/extensions/typespec/libraries-manifest.json","fixtures/typespec/library-corpus-results.json","fixtures/typespec/library-corpus-oracle-results.json","tests/typespec/all-libraries.test.ts","fixtures/typespec/all-library-corpus-results.json","fixtures/typespec/all-library-corpus-oracle-results.json","tests/typespec/emission.test.ts","fixtures/typespec/emission-oracle-results.json","fixtures/typespec/emission-json-oracle-results.json","fixtures/typespec/emission/numeric.tsp","fixtures/typespec/emission/numeric-oracle-results.json","tests/typespec/projection.test.ts","fixtures/typespec/emission/projection-oracle-results.json"]}};var exports_src2={};__export(exports_src2,{setExtension:()=>setExtension,namespace:()=>namespace3,isOneOf:()=>isOneOf,isJsonSchemaDeclaration:()=>isJsonSchemaDeclaration,getUniqueItems:()=>getUniqueItems,getPrefixItems:()=>getPrefixItems,getMultipleOfAsNumeric:()=>getMultipleOfAsNumeric,getMultipleOf:()=>getMultipleOf,getMinProperties:()=>getMinProperties,getMinContains:()=>getMinContains,getMaxProperties:()=>getMaxProperties,getMaxContains:()=>getMaxContains,getJsonSchemaTypes:()=>getJsonSchemaTypes,getJsonSchema:()=>getJsonSchema,getId:()=>getId,getExtensions:()=>getExtensions,getContentSchema:()=>getContentSchema,getContentMediaType:()=>getContentMediaType,getContentEncoding:()=>getContentEncoding,getContains:()=>getContains,getBaseUri:()=>getBaseUri,findBaseUri:()=>findBaseUri,JsonSchemaEmitter:()=>JsonSchemaEmitter,EmitterOptionsSchema:()=>EmitterOptionsSchema2,$uniqueItems:()=>$uniqueItems,$prefixItems:()=>$prefixItems,$oneOf:()=>$oneOf,$onEmit:()=>$onEmit,$multipleOf:()=>$multipleOf,$minProperties:()=>$minProperties,$minContains:()=>$minContains,$maxProperties:()=>$maxProperties,$maxContains:()=>$maxContains,$lib:()=>$lib2,$jsonSchema:()=>$jsonSchema,$id:()=>$id,$flags:()=>$flags,$extension:()=>$extension,$decorators:()=>$decorators2,$contentSchema:()=>$contentSchema,$contentMediaType:()=>$contentMediaType,$contentEncoding:()=>$contentEncoding,$contains:()=>$contains,$baseUri:()=>$baseUri});class CustomKeyMap{#items=new Map;#keyer;constructor(keyer){this.#keyer=keyer}get(items){return this.#items.get(this.#keyer(items))}set(items,value){let key=this.#keyer(items);this.#items.set(key,value)}static objectKeyer(){let knownKeys=new WeakMap,count=0;return{getKey(o){if(knownKeys.has(o))return knownKeys.get(o);let key=count;return count++,knownKeys.set(o,key),key}}}}class Placeholder{#listeners=[];setValue(value){for(let listener of this.#listeners)listener(value)}onValue(cb){this.#listeners.push(cb)}}function scopeChain(scope){let chain=[];while(scope)chain.unshift(scope),scope=scope.parentScope;return chain}function resolveDeclarationReferenceScope(target,currentScope){let targetScope=target.scope,targetChain=scopeChain(targetScope),currentChain=scopeChain(currentScope),diffStart=0;while(targetChain[diffStart]&&currentChain[diffStart]&&targetChain[diffStart]===currentChain[diffStart])diffStart++;let pathUp=currentChain.slice(diffStart),pathDown=targetChain.slice(diffStart),commonScope=targetChain[diffStart-1]??null;return{pathUp,pathDown,commonScope}}class ReferenceCycle{containsDeclaration;#entries;constructor(entries){let firstDeclarationIndex=entries.findIndex((entry)=>entry.entity.kind==="declaration");this.containsDeclaration=firstDeclarationIndex!==-1,this.#entries=this.containsDeclaration?[...entries.slice(firstDeclarationIndex),...entries.slice(0,firstDeclarationIndex)]:entries}get first(){return this.#entries[0]}[Symbol.iterator](){return this.#entries[Symbol.iterator]()}[Symbol.toStringTag](){return[...this.#entries,this.#entries[0]].map((x)=>getTypeName(x.type)).join(" -> ")}toString(){return this[Symbol.toStringTag]()}}class StringBuilder extends Placeholder{segments=[];#placeholders=new Set;#notifyComplete(){let value=this.segments.join("");this.setValue(value)}#setPlaceholderValue(ph,value){for(let[i,segment]of this.segments.entries())if(segment===ph)this.segments[i]=value;if(this.#placeholders.delete(ph),this.#placeholders.size===0)this.#notifyComplete()}pushLiteralSegment(segment){if(this.#shouldConcatLiteral())this.segments[this.segments.length-1]+=segment;else this.segments.push(segment)}pushPlaceholder(ph){this.#placeholders.add(ph),ph.onValue((value)=>{this.#setPlaceholderValue(ph,value)}),this.segments.push(ph)}pushStringBuilder(builder){for(let segment of builder.segments)this.push(segment)}push(segment){if(typeof segment==="string")this.pushLiteralSegment(segment);else if(segment instanceof StringBuilder)this.pushStringBuilder(segment);else this.pushPlaceholder(segment)}reduce(){if(this.#placeholders.size===0)return this.segments.join("");return this}#shouldConcatLiteral(){return this.segments.length>0&&typeof this.segments[this.segments.length-1]==="string"}}class TypeEmitter{emitter;constructor(emitter){this.emitter=emitter}programContext(program){return{}}namespace(namespace3){for(let ns of namespace3.namespaces.values())this.emitter.emitType(ns);for(let model of namespace3.models.values())if(!isTemplateDeclaration(model))this.emitter.emitType(model);for(let operation of namespace3.operations.values())if(!isTemplateDeclaration(operation))this.emitter.emitType(operation);for(let enumeration of namespace3.enums.values())this.emitter.emitType(enumeration);for(let union of namespace3.unions.values())if(!isTemplateDeclaration(union))this.emitter.emitType(union);for(let iface of namespace3.interfaces.values())if(!isTemplateDeclaration(iface))this.emitter.emitType(iface);for(let scalar2 of namespace3.scalars.values())this.emitter.emitType(scalar2);return this.emitter.result.none()}namespaceContext(namespace3){return{}}namespaceReferenceContext(namespace3){return{}}modelLiteral(model){if(model.baseModel)this.emitter.emitType(model.baseModel);return this.emitter.emitModelProperties(model),this.emitter.result.none()}modelLiteralContext(model){return{}}modelLiteralReferenceContext(model){return{}}modelDeclaration(model,name){if(model.baseModel)this.emitter.emitType(model.baseModel);return this.emitter.emitModelProperties(model),this.emitter.result.none()}modelDeclarationContext(model,name){return{}}modelDeclarationReferenceContext(model,name){return{}}modelInstantiation(model,name){if(model.baseModel)this.emitter.emitType(model.baseModel);return this.emitter.emitModelProperties(model),this.emitter.result.none()}modelInstantiationContext(model,name){return{}}modelInstantiationReferenceContext(model,name){return{}}modelProperties(model){for(let prop of model.properties.values())this.emitter.emitModelProperty(prop);return this.emitter.result.none()}modelPropertiesContext(model){return{}}modelPropertiesReferenceContext(model){return{}}modelPropertyLiteral(property){return this.emitter.emitTypeReference(property.type),this.emitter.result.none()}modelPropertyLiteralContext(property){return{}}modelPropertyLiteralReferenceContext(property){return{}}modelPropertyReference(property){return this.emitter.emitTypeReference(property.type)}enumMemberReference(member){return this.emitter.result.none()}arrayDeclaration(array,name,elementType){return this.emitter.emitType(array.indexer.value),this.emitter.result.none()}arrayDeclarationContext(array,name,elementType){return{}}arrayDeclarationReferenceContext(array,name,elementType){return{}}arrayLiteral(array,elementType){return this.emitter.result.none()}arrayLiteralContext(array,elementType){return{}}arrayLiteralReferenceContext(array,elementType){return{}}scalarDeclaration(scalar2,name){if(scalar2.baseScalar)this.emitter.emitType(scalar2.baseScalar);return this.emitter.result.none()}scalarDeclarationContext(scalar2,name){return{}}scalarDeclarationReferenceContext(scalar2,name){return{}}scalarInstantiation(scalar2,name){return this.emitter.result.none()}scalarInstantiationContext(scalar2,name){return{}}intrinsic(intrinsic,name){return this.emitter.result.none()}intrinsicContext(intrinsic,name){return{}}booleanLiteralContext(boolean){return{}}booleanLiteral(boolean){return this.emitter.result.none()}stringTemplateContext(string){return{}}stringTemplate(stringTemplate){return this.emitter.result.none()}stringLiteralContext(string){return{}}stringLiteral(string){return this.emitter.result.none()}numericLiteralContext(number){return{}}numericLiteral(number){return this.emitter.result.none()}operationDeclaration(operation,name){return this.emitter.emitOperationParameters(operation),this.emitter.emitOperationReturnType(operation),this.emitter.result.none()}operationDeclarationContext(operation,name){return{}}operationDeclarationReferenceContext(operation,name){return{}}interfaceDeclarationOperationsContext(iface){return{}}interfaceDeclarationOperationsReferenceContext(iface){return{}}interfaceOperationDeclarationContext(operation,name){return{}}interfaceOperationDeclarationReferenceContext(operation,name){return{}}operationParameters(operation,parameters){return this.emitter.result.none()}operationParametersContext(operation,parameters){return{}}operationParametersReferenceContext(operation,parameters){return{}}operationReturnType(operation,returnType){return this.emitter.result.none()}operationReturnTypeContext(operation,returnType){return{}}operationReturnTypeReferenceContext(operation,returnType){return{}}interfaceDeclaration(iface,name){return this.emitter.emitInterfaceOperations(iface),this.emitter.result.none()}interfaceDeclarationContext(iface,name){return{}}interfaceDeclarationReferenceContext(iface,name){return{}}interfaceDeclarationOperations(iface){for(let op of iface.operations.values())this.emitter.emitInterfaceOperation(op);return this.emitter.result.none()}interfaceOperationDeclaration(operation,name){return this.emitter.emitOperationParameters(operation),this.emitter.emitOperationReturnType(operation),this.emitter.result.none()}enumDeclaration(en,name){return this.emitter.emitEnumMembers(en),this.emitter.result.none()}enumDeclarationContext(en,name){return{}}enumDeclarationReferenceContext(en,name){return{}}enumMembers(en){for(let member of en.members.values())this.emitter.emitType(member);return this.emitter.result.none()}enumMembersContext(en){return{}}enumMember(member){return this.emitter.result.none()}enumMemberContext(member){return{}}unionDeclaration(union,name){return this.emitter.emitUnionVariants(union),this.emitter.result.none()}unionDeclarationContext(union){return{}}unionDeclarationReferenceContext(union){return{}}unionInstantiation(union,name){return this.emitter.emitUnionVariants(union),this.emitter.result.none()}unionInstantiationContext(union,name){return{}}unionInstantiationReferenceContext(union,name){return{}}unionLiteral(union){return this.emitter.emitUnionVariants(union),this.emitter.result.none()}unionLiteralContext(union){return{}}unionLiteralReferenceContext(union){return{}}unionVariants(union){for(let variant of union.variants.values())this.emitter.emitType(variant);return this.emitter.result.none()}unionVariantsContext(){return{}}unionVariantsReferenceContext(){return{}}unionVariant(variant){return this.emitter.emitTypeReference(variant.type),this.emitter.result.none()}unionVariantContext(union){return{}}unionVariantReferenceContext(union){return{}}tupleLiteral(tuple){return this.emitter.emitTupleLiteralValues(tuple),this.emitter.result.none()}tupleLiteralContext(tuple){return{}}tupleLiteralValues(tuple){for(let value of tuple.values.values())this.emitter.emitType(value);return this.emitter.result.none()}tupleLiteralValuesContext(tuple){return{}}tupleLiteralValuesReferenceContext(tuple){return{}}tupleLiteralReferenceContext(tuple){return{}}sourceFile(sourceFile){let emittedSourceFile={path:sourceFile.path,contents:""};for(let decl of sourceFile.globalScope.declarations)emittedSourceFile.contents+=decl.value+`
`;return emittedSourceFile}async writeOutput(sourceFiles){for(let file of sourceFiles){let outputFile=await this.emitter.emitSourceFile(file);await emitFile(this.emitter.getProgram(),{path:outputFile.path,content:outputFile.contents})}}reference(targetDeclaration,pathUp,pathDown,commonScope){return this.emitter.result.none()}circularReference(target,scope,cycle){if(!cycle.containsDeclaration)throw Error(`Circular references to non-declarations are not supported by this emitter. Cycle:
${cycle}`);if(target.kind!=="declaration")return target;compilerAssert(scope,"Emit context must have a scope set in order to create references to declarations.");let{pathUp,pathDown,commonScope}=resolveDeclarationReferenceScope(target,scope);return this.reference(target,pathUp,pathDown,commonScope)}declarationName(declarationType){if(compilerAssert(declarationType.name!==void 0,"Can't emit a declaration that doesn't have a name."),declarationType.kind==="Enum"||declarationType.kind==="Intrinsic")return declarationType.name;if(declarationType.kind==="Operation"&&declarationType.interface)return declarationType.name;if(!declarationType.templateMapper)return declarationType.name;let unspeakable=!1,parameterNames=declarationType.templateMapper.args.map((t)=>{if(t.entityKind==="Indeterminate")t=t.type;if(!("kind"in t))return;switch(t.kind){case"Model":case"Scalar":case"Interface":case"Operation":case"Enum":case"Union":case"Intrinsic":if(!t.name){unspeakable=!0;return}let declName=this.emitter.emitDeclarationName(t);if(declName===void 0){unspeakable=!0;return}return declName[0].toUpperCase()+declName.slice(1);default:unspeakable=!0;return}});if(unspeakable)return;return declarationType.name+parameterNames.join("")}}class EmitterResult{}class Declaration extends EmitterResult{name;scope;value;kind="declaration";meta={};constructor(name,scope,value){if(value instanceof Placeholder)value.onValue((v)=>this.value=v);super();this.name=name,this.scope=scope,this.value=value}}class RawCode extends EmitterResult{value;kind="code";constructor(value){if(value instanceof Placeholder)value.onValue((v)=>this.value=v);super();this.value=value}}class NoEmit extends EmitterResult{kind="none"}class CircularEmit extends EmitterResult{emitEntityKey;kind="circular";constructor(emitEntityKey){super();this.emitEntityKey=emitEntityKey}}function createAssetEmitter(program,TypeEmitterClass,emitContext){let sourceFiles=[],options={noEmit:program.compilerOptions.dryRun??!1,emitterOutputDir:emitContext.emitterOutputDir,...emitContext.options},typeId=CustomKeyMap.objectKeyer(),contextId=CustomKeyMap.objectKeyer(),entryId=CustomKeyMap.objectKeyer(),typeToEmitEntity=new CustomKeyMap(([method,type,context2])=>{return`${method}-${typeId.getKey(type)}-${contextId.getKey(context2)}`}),waitingCircularRefs=new CustomKeyMap(([method,type,context2])=>{return`${method}-${typeId.getKey(type)}-${contextId.getKey(context2)}`}),knownContexts=new CustomKeyMap(([entry,context2])=>{return`${entryId.getKey(entry)}-${contextId.getKey(context2)}`}),lexicalTypeStack=[],referenceTypeChain=[],context={lexicalContext:{},referenceContext:{}},programContext=null,incomingReferenceContext=null,incomingReferenceContextTarget=null,stateInterner=createInterner(),stackEntryInterner=createInterner(),assetEmitter={getContext(){return{...context.lexicalContext,...context.referenceContext}},getOptions(){return options},getProgram(){return program},result:{declaration(name,value){let scope=currentScope();return compilerAssert(scope,"Emit context must have a scope set in order to create declarations. Consider setting scope to a new source file's global scope in the `programContext` method of `TypeEmitter`."),new Declaration(name,scope,value)},rawCode(value){return new RawCode(value)},none(){return new NoEmit}},createScope(block,name,parentScope=null){let newScope;if(!parentScope)newScope={kind:"sourceFile",name,sourceFile:block,parentScope,childScopes:[],declarations:[]};else newScope={kind:"namespace",name,namespace:block,childScopes:[],declarations:[],parentScope};return parentScope?.childScopes.push(newScope),newScope},createSourceFile(path){let basePath=options.emitterOutputDir,sourceFile={globalScope:void 0,path:joinPaths(basePath,resolveContainedPath(path)),imports:new Map,meta:{}};return sourceFile.globalScope=this.createScope(sourceFile,""),sourceFiles.push(sourceFile),sourceFile},emitTypeReference(target,options2){return withPatchedReferenceContext(options2?.referenceContext,()=>{let oldIncomingReferenceContext=incomingReferenceContext,oldIncomingReferenceContextTarget=incomingReferenceContextTarget;incomingReferenceContext=context.referenceContext??null,incomingReferenceContextTarget=incomingReferenceContext?target:null;let result;if(target.kind==="ModelProperty")result=invokeTypeEmitter("modelPropertyReference",target);else if(target.kind==="EnumMember")result=invokeTypeEmitter("enumMemberReference",target);if(result)return incomingReferenceContext=oldIncomingReferenceContext,incomingReferenceContextTarget=oldIncomingReferenceContextTarget,result;let entity=this.emitType(target);incomingReferenceContext=oldIncomingReferenceContext,incomingReferenceContextTarget=oldIncomingReferenceContextTarget;let placeholder=null;if(entity.kind==="circular"){let waiting=waitingCircularRefs.get(entity.emitEntityKey);if(!waiting)waiting=[],waitingCircularRefs.set(entity.emitEntityKey,waiting);let typeChainSnapshot=referenceTypeChain;return waiting.push({state:{lexicalTypeStack,context},cb:(resolvedEntity)=>invokeReference(this,resolvedEntity,!0,resolveReferenceCycle(typeChainSnapshot,entity,typeToEmitEntity))}),placeholder=new Placeholder,this.result.rawCode(placeholder)}else return invokeReference(this,entity,!1);function invokeReference(assetEmitter2,entity2,circular,cycle){let ref2,scope=currentScope();if(circular)ref2=typeEmitter.circularReference(entity2,scope,cycle);else{if(entity2.kind!=="declaration")return entity2;compilerAssert(scope,"Emit context must have a scope set in order to create references to declarations.");let{pathUp,pathDown,commonScope}=resolveDeclarationReferenceScope(entity2,scope);ref2=typeEmitter.reference(entity2,pathUp,pathDown,commonScope)}if(!(ref2 instanceof EmitterResult))ref2=assetEmitter2.result.rawCode(ref2);if(placeholder)switch(compilerAssert(ref2.kind!=="circular","TypeEmitter `reference` returned circular emit"),compilerAssert(ref2.kind==="none"||!(ref2.value instanceof Placeholder),"TypeEmitter's `reference` method cannot return a placeholder."),ref2.kind){case"code":case"declaration":placeholder.setValue(ref2.value);break;case"none":placeholder.setValue("");break}return ref2}})},emitDeclarationName(type){return typeEmitter.declarationName(type)},async writeOutput(){return typeEmitter.writeOutput(sourceFiles)},getSourceFiles(){return sourceFiles},emitType(type,context2){if(context2?.referenceContext)incomingReferenceContext=context2?.referenceContext??incomingReferenceContext,incomingReferenceContextTarget=type??incomingReferenceContextTarget;let declName=isDeclaration(type)&&type.kind!=="Namespace"?typeEmitter.declarationName(type):null,key=typeEmitterKey(type),args;switch(key){case"scalarDeclaration":case"scalarInstantiation":case"modelDeclaration":case"modelInstantiation":case"operationDeclaration":case"interfaceDeclaration":case"interfaceOperationDeclaration":case"enumDeclaration":case"unionDeclaration":case"unionInstantiation":args=[declName];break;case"arrayDeclaration":let arrayDeclElement=type.indexer.value;args=[declName,arrayDeclElement];break;case"arrayLiteral":args=[type.indexer.value];break;case"intrinsic":args=[declName];break;default:args=[]}return invokeTypeEmitter(key,type,...args)},emitProgram(options2){let namespace3=program.getGlobalNamespaceType();if(options2?.emitGlobalNamespace){this.emitType(namespace3);return}for(let ns of namespace3.namespaces.values()){if(ns.name==="TypeSpec"&&!options2?.emitTypeSpecNamespace)continue;this.emitType(ns)}for(let model of namespace3.models.values())if(!isTemplateDeclaration(model))this.emitType(model);for(let operation of namespace3.operations.values())if(!isTemplateDeclaration(operation))this.emitType(operation);for(let enumeration of namespace3.enums.values())this.emitType(enumeration);for(let union of namespace3.unions.values())if(!isTemplateDeclaration(union))this.emitType(union);for(let iface of namespace3.interfaces.values())if(!isTemplateDeclaration(iface))this.emitType(iface);for(let scalar2 of namespace3.scalars.values())this.emitType(scalar2)},emitModelProperties(model){let res=invokeTypeEmitter("modelProperties",model);if(res instanceof EmitterResult)return res;else return this.result.rawCode(res)},emitModelProperty(property){return invokeTypeEmitter("modelPropertyLiteral",property)},emitOperationParameters(operation){return invokeTypeEmitter("operationParameters",operation,operation.parameters)},emitOperationReturnType(operation){return invokeTypeEmitter("operationReturnType",operation,operation.returnType)},emitInterfaceOperations(iface){return invokeTypeEmitter("interfaceDeclarationOperations",iface)},emitInterfaceOperation(operation){let name=typeEmitter.declarationName(operation);if(name===void 0)compilerAssert(!1,"Unnamed operations are not supported");return invokeTypeEmitter("interfaceOperationDeclaration",operation,name)},emitEnumMembers(en){return invokeTypeEmitter("enumMembers",en)},emitUnionVariants(union){return invokeTypeEmitter("unionVariants",union)},emitTupleLiteralValues(tuple){return invokeTypeEmitter("tupleLiteralValues",tuple)},async emitSourceFile(sourceFile){return await typeEmitter.sourceFile(sourceFile)}},typeEmitter=new TypeEmitterClass(assetEmitter);return assetEmitter;function invokeTypeEmitter(method,...args){let type=args[0],entity,emitEntityKey,cached=!1;if(withTypeContext(method,args,()=>{emitEntityKey=[method,type,context];let seenEmitEntity=typeToEmitEntity.get(emitEntityKey);if(seenEmitEntity){entity=seenEmitEntity,cached=!0;return}typeToEmitEntity.set(emitEntityKey,new CircularEmit(emitEntityKey)),compilerAssert(typeEmitter[method],`TypeEmitter doesn't have a method named ${method}.`),entity=liftToRawCode(typeEmitter[method](...args))}),cached)return entity;if(entity instanceof Placeholder)return entity.onValue((v)=>handleCompletedEntity(v)),entity;return handleCompletedEntity(entity),entity;function handleCompletedEntity(entity2){typeToEmitEntity.set(emitEntityKey,entity2);let waitingRefCbs=waitingCircularRefs.get(emitEntityKey);if(waitingRefCbs){for(let record of waitingRefCbs)withContext(record.state,()=>{record.cb(entity2)});waitingCircularRefs.set(emitEntityKey,[])}if(entity2.kind==="declaration")entity2.scope.declarations.push(entity2)}function liftToRawCode(value){if(value instanceof EmitterResult)return value;return assetEmitter.result.rawCode(value)}}function isInternalMethod(method){return method==="interfaceDeclarationOperations"||method==="interfaceOperationDeclaration"||method==="operationParameters"||method==="operationReturnType"||method==="modelProperties"||method==="enumMembers"||method==="tupleLiteralValues"||method==="unionVariants"}function setContextForType(method,args){let type=args[0],newTypeStack,isUnspeakableInstantiation=(method==="modelInstantiation"||method==="unionInstantiation")&&args[1]===void 0;if(isDeclaration(type)&&type.kind!=="Intrinsic"&&!isInternalMethod(method)&&!isUnspeakableInstantiation){newTypeStack=[stackEntryInterner.intern({method,args:stackEntryInterner.intern(args)})];let ns=type.namespace;while(ns){if(ns.name==="")break;newTypeStack.unshift(stackEntryInterner.intern({method:"namespace",args:stackEntryInterner.intern([ns])})),ns=ns.namespace}}else newTypeStack=[...lexicalTypeStack,stackEntryInterner.intern({method,args:stackEntryInterner.intern(args)})];if(lexicalTypeStack=newTypeStack,!programContext)programContext=stateInterner.intern({lexicalContext:typeEmitter.programContext(program),referenceContext:stateInterner.intern({})});context=programContext;for(let entry of lexicalTypeStack){if(incomingReferenceContext&&entry.args[0]===incomingReferenceContextTarget)context=stateInterner.intern({lexicalContext:context.lexicalContext,referenceContext:stateInterner.intern({...context.referenceContext,...incomingReferenceContext})});let seenContext=knownContexts.get([entry,context]);if(seenContext){context=seenContext;continue}let lexicalKey=entry.method+"Context",referenceKey=entry.method+"ReferenceContext";if(keyHasContext(entry.method))compilerAssert(typeEmitter[lexicalKey],`TypeEmitter doesn't have a method named ${lexicalKey}`);if(keyHasReferenceContext(entry.method))compilerAssert(typeEmitter[referenceKey],`TypeEmitter doesn't have a method named ${referenceKey}`);let newContext=keyHasContext(entry.method)?typeEmitter[lexicalKey](...entry.args):{},newReferenceContext=keyHasReferenceContext(entry.method)?typeEmitter[referenceKey](...entry.args):{},newContextState=stateInterner.intern({lexicalContext:stateInterner.intern({...context.lexicalContext,...newContext}),referenceContext:stateInterner.intern({...context.referenceContext,...newReferenceContext})});knownContexts.set([entry,context],newContextState),context=newContextState}if(!isInternalMethod(method))referenceTypeChain=[...referenceTypeChain,stackEntryInterner.intern({method,type,context})]}function withTypeContext(method,args,cb){let oldContext=context,oldTypeStack=lexicalTypeStack,oldRefTypeStack=referenceTypeChain;setContextForType(method,args),cb(),context=oldContext,lexicalTypeStack=oldTypeStack,referenceTypeChain=oldRefTypeStack}function withPatchedReferenceContext(referenceContext,cb){if(referenceContext!==void 0){let oldContext=context;context=stateInterner.intern({lexicalContext:context.lexicalContext,referenceContext:stateInterner.intern({...context.referenceContext,...referenceContext})});let result=cb();return context=oldContext,result}else return cb()}function withContext(newContext,cb){let oldContext=context,oldTypeStack=lexicalTypeStack;context=newContext.context,lexicalTypeStack=newContext.lexicalTypeStack,cb(),context=oldContext,lexicalTypeStack=oldTypeStack}function typeEmitterKey(type){switch(type.kind){case"Model":if($(program).array.is(type)&&type.name==="Array")return"arrayLiteral";if(type.name==="")return"modelLiteral";if(type.templateMapper)return"modelInstantiation";if(type.indexer&&type.indexer.key.name==="integer")return"arrayDeclaration";return"modelDeclaration";case"Namespace":return"namespace";case"ModelProperty":return"modelPropertyLiteral";case"StringTemplate":return"stringTemplate";case"Boolean":return"booleanLiteral";case"String":return"stringLiteral";case"Number":return"numericLiteral";case"Operation":if(type.interface)return"interfaceOperationDeclaration";else return"operationDeclaration";case"Interface":return"interfaceDeclaration";case"Enum":return"enumDeclaration";case"EnumMember":return"enumMember";case"Union":if(!type.name)return"unionLiteral";if(type.templateMapper)return"unionInstantiation";return"unionDeclaration";case"UnionVariant":return"unionVariant";case"Tuple":return"tupleLiteral";case"Scalar":if(type.templateMapper)return"scalarInstantiation";else return"scalarDeclaration";case"Intrinsic":return"intrinsic";default:compilerAssert(!1,`Encountered type ${type.kind} which we don't know how to emit.`)}}function currentScope(){return context.referenceContext?.scope??context.lexicalContext?.scope??null}}function isDeclaration(type){switch(type.kind){case"Namespace":case"Interface":case"Enum":case"Operation":case"Scalar":case"Intrinsic":return!0;case"Model":return type.name?type.name!==""&&type.name!=="Array":!1;case"Union":return type.name?type.name!=="":!1;default:return!1}}function createInterner(){let emptyObject={},root=new Map;function intern(object){if(object===null||typeof object!=="object")return object;let keys2=Object.keys(object);if(keys2.length===0)return emptyObject;let node=root.get(keys2.length);if(!node)node=new Map,root.set(keys2.length,node);let sortedKeys=keys2.sort(),curr=node;for(let key of sortedKeys){if(!curr.has(key))curr.set(key,new Map);curr=curr.get(key)}let valueNode=curr.valueNode;if(!valueNode)valueNode=new Map,curr.valueNode=valueNode;let values=sortedKeys.map((k)=>object[k]),leaf=valueNode;for(let i=0;i<values.length;i++){let v=values[i],isObj=v&&typeof v==="object",next;if(isObj){if(!leaf.has("obj"))leaf.set("obj",new WeakMap);if(next=leaf.get("obj"),!next.has(v))next.set(v,new Map);next=next.get(v)}else{if(!leaf.has("prim"))leaf.set("prim",new Map);if(next=leaf.get("prim"),!next.has(v))next.set(v,new Map);next=next.get(v)}leaf=next}if(leaf.has("interned"))return leaf.get("interned");return leaf.set("interned",object),object}return{intern}}var noContext=new Set(["modelPropertyReference","enumMemberReference"]);function keyHasContext(key){return!noContext.has(key)}var noReferenceContext=new Set([...noContext,"booleanLiteral","stringTemplate","stringLiteral","numericLiteral","scalarInstantiation","enumMember","enumMembers","intrinsic"]);function keyHasReferenceContext(key){return!noReferenceContext.has(key)}function resolveReferenceCycle(stack,entity,typeToEmitEntity){for(let i=stack.length-1;i>=0;i--)if(stack[i].type===entity.emitEntityKey[1])return new ReferenceCycle(stack.slice(i).map((x)=>{return{type:x.type,entity:typeToEmitEntity.get([x.method,x.type,x.context])}}));throw Error(`Couldn't resolve the circular reference stack for ${getTypeName(entity.emitEntityKey[1])}`)}function resolveContainedPath(path){return path.split(/[/\\]/).filter((segment)=>segment!==""&&segment!=="."&&segment!=="..").map(sanitizePathSegment).join("/")}class ArrayBuilder extends Array{#setPlaceholderValue(p,value){for(let[i,item]of this.entries())if(item===p)this[i]=value}push(...values){for(let v of values){let toPush;if(v instanceof EmitterResult)if(compilerAssert(v.kind!=="circular","Can't push a circular emit result."),v.kind==="none")toPush=void 0;else toPush=v.value;else toPush=v;if(toPush instanceof Placeholder)toPush.onValue((v2)=>this.#setPlaceholderValue(toPush,v2));super.push(toPush)}return values.length}}var placeholderSym=Symbol("placeholder"),setSym=Symbol("ObjectBuilder.set");class ObjectBuilder{static SET=setSym;[placeholderSym];constructor(initializer={}){let copyProperties=(source)=>{for(let[key,value]of Object.entries(source))this[setSym](key,value)},registerPlaceholder=(placeholder)=>{placeholder.onValue(copyProperties)};if(initializer instanceof ObjectBuilder){if(initializer[placeholderSym])this[placeholderSym]=initializer[placeholderSym],registerPlaceholder(initializer[placeholderSym]);copyProperties(initializer)}else if(initializer instanceof Placeholder)this[placeholderSym]=initializer,registerPlaceholder(initializer);else copyProperties(initializer)}set(key,v){this[setSym](key,v)}[setSym](key,v){let value=v;if(v instanceof EmitterResult)if(compilerAssert(v.kind!=="circular","Can't set a circular emit result."),v.kind==="none"){this[key]=void 0;return}else value=v.value;if(value instanceof Placeholder)value.onValue((v2)=>{this[key]=v2});this[key]=value}}function setProperty(builder,key,value){builder[setSym](key,value)}class JsonSchemaEmitter extends TypeEmitter{#idDuplicateTracker=new DuplicateTracker;#typeForSourceFile=new Map;#declDefKey=new Map;#applyModelIndexer(schema,model){if(model.indexer){setProperty(schema,"unevaluatedProperties",this.emitter.emitTypeReference(model.indexer.value));return}if(!this.emitter.getOptions()["seal-object-schemas"])return;if(!model.derivedModels.filter(includeDerivedModel).length)setProperty(schema,"unevaluatedProperties",{not:{}})}modelDeclaration(model,name){let discriminator=getDiscriminator(this.emitter.getProgram(),model),strategy=this.emitter.getOptions()["polymorphic-models-strategy"];if((strategy==="oneOf"||strategy==="anyOf")&&discriminator&&model.derivedModels.length>0)return this.#createDiscriminatedUnionDeclaration(model,name,discriminator,strategy);let shouldInlineBase=model.baseModel&&this.#isBaseUsingDiscriminatedUnion(model.baseModel),schema=this.#initializeSchema(model,name,{type:"object",properties:shouldInlineBase?this.#getAllModelProperties(model):this.emitter.emitModelProperties(model),required:shouldInlineBase?this.#getAllRequiredModelProperties(model):this.#requiredModelProperties(model)});if(model.baseModel&&!shouldInlineBase){let allOf=new ArrayBuilder;allOf.push(this.emitter.emitTypeReference(model.baseModel)),setProperty(schema,"allOf",allOf)}return this.#applyModelIndexer(schema,model),this.#applyConstraints(model,schema),this.#createDeclaration(model,name,schema)}#isBaseUsingDiscriminatedUnion(model){let discriminator=getDiscriminator(this.emitter.getProgram(),model),strategy=this.emitter.getOptions()["polymorphic-models-strategy"];return(strategy==="oneOf"||strategy==="anyOf")&&!!discriminator&&model.derivedModels.length>0}modelLiteral(model){let schema=new ObjectBuilder({type:"object",properties:this.emitter.emitModelProperties(model),required:this.#requiredModelProperties(model)});return this.#applyModelIndexer(schema,model),schema}modelInstantiation(model,name){if(!name)return this.modelLiteral(model);return this.modelDeclaration(model,name)}arrayDeclaration(array,name,elementType){let schema=this.#initializeSchema(array,name,{type:"array",items:this.emitter.emitTypeReference(elementType)});return this.#applyConstraints(array,schema),this.#createDeclaration(array,name,schema)}arrayLiteral(array,elementType){return new ObjectBuilder({type:"array",items:this.emitter.emitTypeReference(elementType)})}#requiredModelProperties(model){let requiredProps=[];for(let prop of model.properties.values())if(!prop.optional)requiredProps.push(prop.name);let discriminator=getDiscriminator(this.emitter.getProgram(),model);if(discriminator&&!model.properties.has(discriminator.propertyName))requiredProps.push(discriminator.propertyName);return requiredProps.length>0?requiredProps:void 0}#getAllRequiredModelProperties(model){let requiredProps=[],visited=new Set,collectRequired=(m)=>{if(visited.has(m))return;if(visited.add(m),m.baseModel)collectRequired(m.baseModel);for(let prop of m.properties.values())if(!prop.optional&&!requiredProps.includes(prop.name))requiredProps.push(prop.name);let discriminator=getDiscriminator(this.emitter.getProgram(),m);if(discriminator&&!m.properties.has(discriminator.propertyName)&&!requiredProps.includes(discriminator.propertyName))requiredProps.push(discriminator.propertyName)};return collectRequired(model),requiredProps.length>0?requiredProps:void 0}#getAllModelProperties(model){let props=new ObjectBuilder,visited=new Set,collectProperties=(m)=>{if(visited.has(m))return;if(visited.add(m),m.baseModel)collectProperties(m.baseModel);for(let[name,prop]of m.properties){let result=this.emitter.emitModelProperty(prop);setProperty(props,name,result)}let discriminator=getDiscriminator(this.emitter.getProgram(),m);if(discriminator&&!(discriminator.propertyName in props))setProperty(props,discriminator.propertyName,{type:"string",description:`Discriminator property for ${m.name}.`})};return collectProperties(model),props}modelProperties(model){let props=new ObjectBuilder;for(let[name,prop]of model.properties){let result=this.emitter.emitModelProperty(prop);setProperty(props,name,result)}let discriminator=getDiscriminator(this.emitter.getProgram(),model);if(discriminator&&!(discriminator.propertyName in props))setProperty(props,discriminator.propertyName,{type:"string",description:`Discriminator property for ${model.name}.`});return props}modelPropertyLiteral(property){let propertyType=this.emitter.emitTypeReference(property.type);compilerAssert(propertyType.kind==="code","Unexpected non-code result from emit reference");let result=new ObjectBuilder(propertyType.value);if(property.defaultValue)result.default=this.#getDefaultValue(property,property.defaultValue);if(result.anyOf&&isOneOf(this.emitter.getProgram(),property))result.oneOf=result.anyOf,delete result.anyOf;return this.#applyConstraints(property,result),result}#getDefaultValue(modelProperty,defaultType){return serializeValueAsJson(this.emitter.getProgram(),defaultType,modelProperty)}booleanLiteral(boolean){return{type:"boolean",const:boolean.value}}stringLiteral(string){return{type:"string",const:string.value}}stringTemplate(string){if(string.stringValue!==void 0)return{type:"string",const:string.stringValue};let diagnostics=explainStringTemplateNotSerializable(string);return this.emitter.getProgram().reportDiagnostics(diagnostics.map((x)=>({...x,severity:"warning"}))),{type:"string"}}numericLiteral(number){return{type:"number",const:number.value}}enumDeclaration(en,name){let enumTypes=new Set,enumValues=new Set;for(let member of en.members.values())enumTypes.add(typeof member.value==="number"?"number":"string"),enumValues.add(member.value??member.name);let enumTypesArray=[...enumTypes],withConstraints=this.#initializeSchema(en,name,{type:enumTypesArray.length===1?enumTypesArray[0]:enumTypesArray,enum:[...enumValues]});return this.#applyConstraints(en,withConstraints),this.#createDeclaration(en,name,withConstraints)}enumMemberReference(member){switch(typeof member.value){case"undefined":return{type:"string",const:member.name};case"string":return{type:"string",const:member.value};case"number":return{type:"number",const:member.value}}}tupleLiteral(tuple){return new ObjectBuilder({type:"array",prefixItems:this.emitter.emitTupleLiteralValues(tuple)})}tupleLiteralValues(tuple){let values=new ArrayBuilder;for(let value of tuple.values.values())values.push(this.emitter.emitType(value));return values}unionInstantiation(union,name){if(!name)return this.unionLiteral(union);return this.unionDeclaration(union,name)}unionDeclaration(union,name){let key=isOneOf(this.emitter.getProgram(),union)?"oneOf":"anyOf",withConstraints=this.#initializeSchema(union,name,{[key]:this.emitter.emitUnionVariants(union)});return this.#applyConstraints(union,withConstraints),this.#createDeclaration(union,name,withConstraints)}unionLiteral(union){let key=isOneOf(this.emitter.getProgram(),union)?"oneOf":"anyOf";return new ObjectBuilder({[key]:this.emitter.emitUnionVariants(union)})}unionVariants(union){let variants=new ArrayBuilder;for(let variant of union.variants.values())variants.push(this.emitter.emitType(variant));return variants}unionVariant(variant){let variantType=this.emitter.emitTypeReference(variant.type);compilerAssert(variantType.kind==="code","Unexpected non-code result from emit reference");let result=new ObjectBuilder(variantType.value);return this.#applyConstraints(variant,result),result}modelPropertyReference(property){let refSchema=this.emitter.emitTypeReference(property.type);compilerAssert(refSchema.kind==="code","Unexpected non-code result from emit reference");let schema=new ObjectBuilder(refSchema.value);return this.#applyConstraints(property,schema),schema}reference(targetDeclaration,pathUp,pathDown,commonScope){if(targetDeclaration.value instanceof Placeholder)throw Error("Can't form reference to declaration that hasn't been created yet");let currentSfScope=pathUp[pathUp.length-1],targetSfScope=pathDown[0];if(targetSfScope&&currentSfScope&&!targetSfScope.sourceFile.meta.shouldEmit)currentSfScope.sourceFile.meta.bundledRefs.push(targetDeclaration);if(targetDeclaration.value.$id)return{$ref:targetDeclaration.value.$id};if(!commonScope)if(targetSfScope&&!targetSfScope.sourceFile.meta.shouldEmit)return{$ref:"#/$defs/"+targetDeclaration.name};else return{$ref:getRelativePathFromDirectory(getDirectoryPath(currentSfScope.sourceFile.path),targetSfScope.sourceFile.path,!1)};if(!currentSfScope&&!targetSfScope)return{$ref:"#/$defs/"+targetDeclaration.name};throw Error("JSON Pointer refs to arbitrary schemas is not supported")}scalarInstantiation(scalar2,name){if(!name)return this.#getSchemaForScalar(scalar2);return this.scalarDeclaration(scalar2,name)}scalarInstantiationContext(scalar2,name){if(name===void 0)return{};else return this.#newFileScope(scalar2)}scalarDeclaration(scalar2,name){let isStd=this.#isStdType(scalar2),schema=this.#getSchemaForScalar(scalar2);if(isStd)return schema;let builderSchema=this.#initializeSchema(scalar2,name,schema);return this.#createDeclaration(scalar2,name,builderSchema)}#getSchemaForScalar(scalar2){let result,isStd=this.#isStdType(scalar2);if(isStd)result=this.#getSchemaForStdScalars(scalar2);else if(scalar2.baseScalar)result=this.#getSchemaForScalar(scalar2.baseScalar);else return reportDiagnostic2(this.emitter.getProgram(),{code:"unknown-scalar",format:{name:scalar2.name},target:scalar2}),{};let objectBuilder=new ObjectBuilder(result);if(this.#applyConstraints(scalar2,objectBuilder),isStd)delete objectBuilder.description;return objectBuilder}#getSchemaForStdScalars(baseBuiltIn){switch(baseBuiltIn.name){case"uint8":return{type:"integer",minimum:0,maximum:255};case"uint16":return{type:"integer",minimum:0,maximum:65535};case"uint32":return{type:"integer",minimum:0,maximum:4294967295};case"int8":return{type:"integer",minimum:-128,maximum:127};case"int16":return{type:"integer",minimum:-32768,maximum:32767};case"int32":case"unixTimestamp32":return{type:"integer",minimum:-2147483648,maximum:2147483647};case"int64":if((this.emitter.getOptions()["int64-strategy"]??"string")==="string")return{type:"string"};else return{type:"integer"};case"uint64":if((this.emitter.getOptions()["int64-strategy"]??"string")==="string")return{type:"string"};else return{type:"integer"};case"decimal":case"decimal128":return{type:"string"};case"integer":return{type:"integer"};case"safeint":return{type:"integer"};case"float":return{type:"number"};case"float32":return{type:"number"};case"float64":return{type:"number"};case"numeric":return{type:"number"};case"string":return{type:"string"};case"boolean":return{type:"boolean"};case"plainDate":return{type:"string",format:"date"};case"plainTime":return{type:"string",format:"time"};case"offsetDateTime":case"utcDateTime":return{type:"string",format:"date-time"};case"duration":return{type:"string",format:"duration"};case"url":return{type:"string",format:"uri"};case"bytes":return{type:"string",contentEncoding:"base64"};default:return reportDiagnostic2(this.emitter.getProgram(),{code:"unknown-scalar",format:{name:baseBuiltIn.name},target:baseBuiltIn}),{}}}#applySchemaExamples(type,target){let program=this.emitter.getProgram(),examples=getExamples(program,type);if(examples.length>0)setProperty(target,"examples",examples.map((x)=>serializeValueAsJson(program,x.value,type)))}#applyConstraints(type,schema){let applyConstraint=(fn,key)=>{let value=fn(this.emitter.getProgram(),type);if(value!==void 0)schema[key]=value},applyTypeConstraint=(fn,key)=>{let constraintType=fn(this.emitter.getProgram(),type);if(constraintType){let ref2=this.emitter.emitTypeReference(constraintType);compilerAssert(ref2.kind==="code","Unexpected non-code result from emit reference"),setProperty(schema,key,ref2.value)}};if(type.kind!=="UnionVariant")this.#applySchemaExamples(type,schema);if(applyConstraint(getMinLength,"minLength"),applyConstraint(getMaxLength,"maxLength"),applyConstraint(getMinValue,"minimum"),applyConstraint(getMinValueExclusive,"exclusiveMinimum"),applyConstraint(getMaxValue,"maximum"),applyConstraint(getMaxValueExclusive,"exclusiveMaximum"),applyConstraint(getPattern,"pattern"),applyConstraint(getMinItems,"minItems"),applyConstraint(getMaxItems,"maxItems"),!this.#isStdType(type)||type.name!=="url")applyConstraint(getFormat,"format");applyConstraint(getMultipleOf,"multipleOf"),applyTypeConstraint(getContains,"contains"),applyConstraint(getMinContains,"minContains"),applyConstraint(getMaxContains,"maxContains"),applyConstraint(getUniqueItems,"uniqueItems"),applyConstraint(getMinProperties,"minProperties"),applyConstraint(getMaxProperties,"maxProperties"),applyConstraint(getContentEncoding,"contentEncoding"),applyConstraint(getContentMediaType,"contentMediaType"),applyTypeConstraint(getContentSchema,"contentSchema"),applyConstraint(getDoc,"description"),applyConstraint(getSummary,"title"),applyConstraint((p,t)=>getDeprecated(p,t)!==void 0?!0:void 0,"deprecated");let prefixItems=getPrefixItems(this.emitter.getProgram(),type);if(prefixItems){let prefixItemsSchema=new ArrayBuilder;for(let item of prefixItems.values)prefixItemsSchema.push(this.emitter.emitTypeReference(item));setProperty(schema,"prefixItems",prefixItemsSchema)}let extensions=getExtensions(this.emitter.getProgram(),type);for(let{key,value}of extensions)if(this.#isTypeLike(value))setProperty(schema,key,this.emitter.emitTypeReference(value));else setProperty(schema,key,value)}#isTypeLike(value){return typeof value==="object"&&value!==null&&isType(value)}#createDeclaration(type,name,schema){let decl=this.emitter.result.declaration(name,schema),sf=decl.scope.sourceFile;sf.meta.shouldEmit=this.#shouldEmitRootSchema(type);let explicitId=getId(this.emitter.getProgram(),type);if(explicitId)this.#declDefKey.set(decl,explicitId);return decl}#initializeSchema(type,name,props){let rootSchemaProps=this.#shouldEmitRootSchema(type)?this.#getRootSchemaProps(type,name):{};return new ObjectBuilder({...rootSchemaProps,...props})}#getRootSchemaProps(type,name){return{$schema:"https://json-schema.org/draft/2020-12/schema",$id:this.#getDeclId(type,name)}}#shouldEmitRootSchema(type){return this.emitter.getOptions().emitAllRefs||this.emitter.getOptions().emitAllModels||isJsonSchemaDeclaration(this.emitter.getProgram(),type)}#isStdType(type){return this.emitter.getProgram().checker.isStdType(type)}#createDiscriminatedUnionDeclaration(model,name,discriminator,strategy){let variants=new ArrayBuilder,knownDiscriminatorValues=[];for(let derived of model.derivedModels){if(!includeDerivedModel(derived))continue;let derivedRef=this.emitter.emitTypeReference(derived);variants.push(derivedRef);let values=this.#getDiscriminatorValues(derived,discriminator.propertyName);knownDiscriminatorValues.push(...values)}if(this.#isOpenDiscriminator(model,discriminator.propertyName)){let catchAllVariant=this.#createCatchAllVariant(model,discriminator.propertyName,knownDiscriminatorValues);variants.push(catchAllVariant)}let schema=this.#initializeSchema(model,name,{type:"object",properties:this.emitter.emitModelProperties(model),required:this.#requiredModelProperties(model),[strategy]:variants});return this.#applyConstraints(model,schema),this.#createDeclaration(model,name,schema)}#isOpenDiscriminator(model,discriminatorPropertyName){let prop=model.properties.get(discriminatorPropertyName);if(!prop)return!1;return this.#typeIncludesString(prop.type)}#typeIncludesString(type){let program=this.emitter.getProgram();switch(type.kind){case"Scalar":return isStringType(program,type);case"Union":for(let variant of type.variants.values())if(this.#typeIncludesString(variant.type))return!0;return!1;default:return!1}}#getDiscriminatorValues(model,discriminatorPropertyName){let prop=model.properties.get(discriminatorPropertyName);if(!prop)return[];return this.#getStringLiteralValues(prop.type)}#getStringLiteralValues(type){switch(type.kind){case"String":return[type.value];case"Union":return[...type.variants.values()].flatMap((v)=>this.#getStringLiteralValues(v.type));case"UnionVariant":return this.#getStringLiteralValues(type.type);case"EnumMember":return typeof type.value!=="number"?[type.value??type.name]:[];default:return[]}}#createCatchAllVariant(model,discriminatorPropertyName,knownValues){let properties=new ObjectBuilder;for(let[propName,prop]of model.properties)if(propName===discriminatorPropertyName)setProperty(properties,propName,{type:"string",not:{enum:knownValues}});else{let result=this.emitter.emitModelProperty(prop);setProperty(properties,propName,result)}if(!model.properties.has(discriminatorPropertyName))setProperty(properties,discriminatorPropertyName,{type:"string",not:{enum:knownValues}});let required=this.#requiredModelProperties(model);return{type:"object",properties,...required&&{required}}}intrinsic(intrinsic,name){switch(intrinsic.name){case"null":return{type:"null"};case"unknown":return{};case"never":case"void":return{not:{}};case"ErrorType":return{};default:let _assertNever=intrinsic.name;compilerAssert(!1,"Unreachable")}}#reportDuplicateIds(){for(let[id,targets]of this.#idDuplicateTracker.entries())for(let target of targets)reportDiagnostic2(this.emitter.getProgram(),{code:"duplicate-id",format:{id},target})}async writeOutput(sourceFiles){if(this.emitter.getProgram().compilerOptions.dryRun)return;this.#reportDuplicateIds();let toEmit=[],bundleId=this.emitter.getOptions().bundleId;if(bundleId){let content={$schema:"https://json-schema.org/draft/2020-12/schema",$id:bundleId,$defs:{}};for(let sf of sourceFiles)if(sf.meta.shouldEmit){let decl=sf.globalScope.declarations[0];content.$defs[this.#getDefKey(decl)]=this.#finalizeSourceFileContent(sf)}await emitFile(this.emitter.getProgram(),{path:joinPaths(this.emitter.getOptions().emitterOutputDir,bundleId),content:this.#serializeSourceFileContent(content)})}else{for(let sf of sourceFiles){let emittedSf=await this.emitter.emitSourceFile(sf);if(sf.meta.shouldEmit)toEmit.push(emittedSf)}for(let emittedSf of toEmit)await emitFile(this.emitter.getProgram(),{path:emittedSf.path,content:emittedSf.contents})}}sourceFile(sourceFile){let content=this.#finalizeSourceFileContent(sourceFile);return{contents:this.#serializeSourceFileContent(content),path:sourceFile.path}}#finalizeSourceFileContent(sourceFile){let decls=sourceFile.globalScope.declarations;compilerAssert(decls.length===1,"Multiple decls in single schema per file mode");let content={...decls[0].value},bundledDecls=new Set;if(sourceFile.meta.bundledRefs.length>0){content.$defs={};let refsToBundle=[...sourceFile.meta.bundledRefs];while(refsToBundle.length>0){let decl=refsToBundle.shift();if(bundledDecls.has(decl))continue;bundledDecls.add(decl),content.$defs[this.#getDefKey(decl)]=decl.value;let refSf=decl.scope.sourceFile;refsToBundle.push(...refSf.meta.bundledRefs)}}return content}#serializeSourceFileContent(content){if(this.emitter.getOptions()["file-type"]==="json")return JSON.stringify(content,null,4);else return stringify(content,{aliasDuplicateObjects:!1,lineWidth:0})}#getCurrentSourceFile(){let scope=this.emitter.getContext().scope;compilerAssert(scope,"Scope should exists");while(scope&&scope.kind!=="sourceFile")scope=scope.parentScope;return compilerAssert(scope,"Top level scope should be a source file"),scope.sourceFile}#getDeclId(type,name){let baseUri=findBaseUri(this.emitter.getProgram(),type),explicitId=getId(this.emitter.getProgram(),type);if(explicitId)return this.#trackId(idWithBaseURI(explicitId,baseUri),type);let base=this.emitter.getOptions().emitterOutputDir,file=this.#getCurrentSourceFile().path,relative=getRelativePathFromDirectory(base,file,!1);if(baseUri)return this.#trackId(new URL(relative,baseUri).href,type);else return this.#trackId(relative,type);function idWithBaseURI(id,baseUri2){if(baseUri2)return new URL(id,baseUri2).href;else return id}}#trackId(id,target){return this.#idDuplicateTracker.track(id,target),id}#getDefKey(decl){return this.#declDefKey.get(decl)??decl.name}modelDeclarationContext(model,name){if(this.#isStdType(model)&&model.name==="object")return{};return this.#newFileScope(model)}modelInstantiationContext(model,name){if(name===void 0)return{};else return this.#newFileScope(model)}arrayDeclarationContext(array){return this.#newFileScope(array)}enumDeclarationContext(en){return this.#newFileScope(en)}unionDeclarationContext(union){return this.#newFileScope(union)}scalarDeclarationContext(scalar2){if(this.#isStdType(scalar2))return{};else return this.#newFileScope(scalar2)}#newFileScope(type){let sourceFile=this.emitter.createSourceFile(`${sanitizePathSegment(this.declarationName(type))}.${this.#fileExtension()}`);return sourceFile.meta.shouldEmit=!0,sourceFile.meta.bundledRefs=[],this.#typeForSourceFile.set(sourceFile,type),{scope:sourceFile.globalScope}}#fileExtension(){return this.emitter.getOptions()["file-type"]==="json"?"json":"yaml"}}async function $onEmit(context){let emitter=createAssetEmitter(context.program,JsonSchemaEmitter,context);if(emitter.getOptions().emitAllModels)emitter.emitProgram({emitTypeSpecNamespace:!1});else for(let item of getJsonSchemaTypes(context.program))emitter.emitType(item);await emitter.writeOutput()}var namespace3="TypeSpec.JsonSchema";var standard_library_default={"/compiler/lib/intrinsics.tsp":`import "../dist/src/lib/intrinsic/tsp-index.js";
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
}`};var trustedJs={"/compiler/dist/src/lib/tsp-index.js":exports_tsp_index2,"/compiler/dist/src/lib/intrinsic/tsp-index.js":exports_tsp_index};async function compileTypeSpecProgram(files,entrypoint,libraries={},emission){let texts=new Map(Object.entries(standard_library_default)),modules={...trustedJs};if(emission)modules["/project/node_modules/@typespec/json-schema/dist/src/index.js"]=exports_src2;for(let[name,version]of Object.entries(libraries)){let library=Object.hasOwn(typeSpecLibraries,name)?typeSpecLibraries[name]:void 0;if(!library||library.version!==version)throw new UmfError("TYPESPEC_LIBRARY_UNAVAILABLE","No registered library "+name+"@"+version);for(let[path,text]of Object.entries(library.files))texts.set("/project/node_modules/"+name+"/"+path,text);for(let[path,module]of Object.entries(typeSpecLibraryModules))if(path.startsWith(name+"/"))modules["/project/node_modules/"+path]=module}for(let[path,text]of Object.entries(files)){if(texts.has("/project/"+path))throw new UmfError("TYPESPEC_LIBRARY_COLLISION","Supplied source conflicts with registered library: "+path);texts.set("/project/"+path,text)}let missing=(path)=>Object.assign(Error("No supplied file: "+path),{code:"ENOENT"});return await compile({async readFile(path){let text=texts.get(path);if(text===void 0)throw missing(path);return createSourceFile(text,path)},async readUrl(url){throw missing(url)},async writeFile(path,content){if(!emission)throw Error("Emission is not enabled");if(!path.startsWith("/output/")||path.split("/").includes(".."))throw new UmfError("TYPESPEC_OUTPUT_PATH","Emitter output escaped its memory root");let name=path.slice(8);if(Object.hasOwn(emission.outputs,name))throw new UmfError("TYPESPEC_OUTPUT_COLLISION","Duplicate emitter output: "+name);if(content.length+Object.values(emission.outputs).reduce((n,s)=>n+s.length,0)>LIMITS.maxTextLength)throw new UmfError("LIMIT","Emitter output exceeds text limit");emission.outputs[name]=content},async readDir(path){let prefix=path.replace(/\/$/,"")+"/";return[...new Set([...texts.keys()].filter((k)=>k.startsWith(prefix)).map((k)=>k.slice(prefix.length).split("/")[0]))]},async rm(){throw Error("Removal is not enabled")},async mkdirp(){return},async stat(path){let file=texts.has(path),directory=[...texts.keys()].some((k)=>k.startsWith(path.replace(/\/$/,"")+"/"));if(!file&&!directory&&!Object.hasOwn(modules,path))throw missing(path);return{isFile:()=>file||Object.hasOwn(modules,path),isDirectory:()=>directory}},async realpath(path){return path},getExecutionRoot:()=>"/compiler",getLibDirs:()=>["/compiler/lib/std"],async getJsImport(path){if(Object.hasOwn(modules,path))return modules[path];throw missing(path)},getSourceFileKind:getSourceFileKindFromExt,fileURLToPath:(url)=>decodeURIComponent(new URL(url).pathname),pathToFileURL:(path)=>"file://"+path,logSink:{log(){}}},"/project/"+entrypoint,emission?{noEmit:!1,outputDir:"/output",emit:["@typespec/json-schema"],options:{"@typespec/json-schema":emission.options}}:{noEmit:!0})}function typeSpecCompilerReport(program,libraries={}){let diagnostics=program.diagnostics.map((d)=>{let at=getSourceLocation(d.target);return{code:d.code,severity:d.severity,message:d.message,...at?{file:at.file.path,start:at.pos,end:at.end}:{}}});return{compiler:"@typespec/compiler@1.16.0",libraries:{...libraries},valid:!program.hasError(),complete:!1,diagnostics,limitations:["Compiler checks supplied TypeSpec and the pinned standard library; only explicitly selected registered libraries are available; arbitrary JavaScript and emitters are not loaded","Compiler validity does not establish cross-system projection equivalence"]}}async function checkTypeSpecSources(files,entrypoint,libraries={}){return typeSpecCompilerReport(await compileTypeSpecProgram(files,entrypoint,libraries),libraries)}var TYPESPEC_EXTENSION="umf.typespec",typespecPackage=package_default,structure=createValidator().compile(package_default.schema);function inspect(value){let p=value,out=[];for(let key of Object.keys(p))if(!["profile","entrypoint","files","libraries"].includes(key))out.push({code:"TYPESPEC_REPRESENTATION",path:"/"+pointer(key),severity:"warning",message:"Unknown source representation field cannot be emitted natively"});for(let[name,version]of Object.entries(p.libraries??{}))if(!Object.hasOwn(typeSpecLibraries,name)||typeSpecLibraries[name].version!==version)out.push({code:"TYPESPEC_LIBRARY_UNAVAILABLE",path:"/libraries/"+pointer(name),severity:"warning",message:"Unregistered library selection retained: "+name+"@"+version});if(!Object.hasOwn(p.files,p.entrypoint))out.push({code:"TYPESPEC_ENTRYPOINT",path:"/entrypoint",severity:"error",message:"Entrypoint must name a supplied file"});let size=0;for(let[path,text]of Object.entries(p.files)){if(!/^(?:[A-Za-z0-9_.-]+\/)*[A-Za-z0-9_.-]+\.tsp$/.test(path)||path.split("/").some((x)=>x==="."||x==="..")){out.push({code:"TYPESPEC_PATH",path:"/files/"+pointer(path),severity:"error",message:"Source paths must be relative .tsp paths without traversal"});continue}if(size+=text.length,size>LIMITS.maxTextLength){out.push({code:"LIMIT",path:"/files",severity:"error",message:"Source bundle exceeds text limit"});break}try{for(let d of parse(text,{comments:!0,docs:!0}).parseDiagnostics)out.push({code:"TYPESPEC_"+d.code,path:"/files/"+pointer(path),severity:d.severity,message:d.message})}catch(error2){out.push({code:"TYPESPEC_PARSE",path:"/files/"+pointer(path),severity:"error",message:String(error2)})}}return out.push({code:"TYPESPEC_COMPILATION_REQUIRED",path:"",severity:"warning",message:"Syntax preservation does not establish compiler validity; call compileTypeSpecDocument with supplied imports"}),out}function typespecRegistry(){return new Registry().register(typespecPackage,inspect)}function inspectTypeSpec(document){return validateDocument(document,typespecRegistry())}function payload(document){let checked=inspectTypeSpec(document);if(!checked.valid)throw new UmfError("TYPESPEC_DOCUMENT",JSON.stringify(checked.diagnostics));let value=document.modules.find((m)=>m.id==="schema")?.elements.find((e)=>e.id==="schema")?.extensions[TYPESPEC_EXTENSION];if(document.vocabularies[TYPESPEC_EXTENSION]?.version!=="0.1.0"||!structure(value))throw new UmfError("TYPESPEC_PAYLOAD","Missing TypeSpec source bundle");return copyJson(value)}function importTypeSpecSources(input,options){let supplied=copyJson(input);if(Object.keys(supplied).some((k)=>!["entrypoint","files","libraries"].includes(k)))throw new UmfError("TYPESPEC_INPUT","Unknown source-bundle option");let value={profile:"typespec-1.16.0-sources",...supplied},doc={umf:"0.1.0",id:options.id,vocabularies:{[TYPESPEC_EXTENSION]:{version:"0.1.0"}},modules:[{id:"schema",namespace:"",elements:[{id:"schema",extensions:{[TYPESPEC_EXTENSION]:value}}]}]};return payload(doc),doc}function exportTypeSpecSources(document){let p=payload(document);if(inspectTypeSpec(document).diagnostics.some((d)=>d.code==="TYPESPEC_REPRESENTATION"))throw new UmfError("TYPESPEC_REPRESENTATION","Native export would discard unknown content");return{entrypoint:p.entrypoint,files:p.files,...p.libraries?{libraries:p.libraries}:{}}}function compileTypeSpecDocument(document){let p=exportTypeSpecSources(document);return checkTypeSpecSources(p.files,p.entrypoint,p.libraries)}function proposeTypeSpecSourceEdit(document,path,text){let p=payload(document);if(!Object.hasOwn(p.files,path)||typeof text!=="string")throw new UmfError("TYPESPEC_EDIT","Edit must replace a supplied source file");p.files[path]=text;let next=copyJson(document);return next.modules.find((m)=>m.id==="schema").elements.find((e)=>e.id==="schema").extensions[TYPESPEC_EXTENSION]=p,payload(next),{document:next,validation:inspectTypeSpec(next)}}function getTypeSpecSyntax(document,path){let p=payload(document);if(!Object.hasOwn(p.files,path))throw new UmfError("TYPESPEC_FILE","No supplied source file");let out=[];function walk(node,depth){if(depth>LIMITS.maxDepth||out.length>=LIMITS.maxValues)throw new UmfError("LIMIT","Syntax traversal exceeds structural limit");out.push({kind:SyntaxKind[node.kind],start:node.pos,end:node.end}),visitChildren(node,(child)=>{walk(child,depth+1);return})}return walk(parse(p.files[path],{comments:!0,docs:!0}),0),out}export{typespecRegistry,typespecPackage,proposeTypeSpecSourceEdit,inspectTypeSpec,importTypeSpecSources,getTypeSpecSyntax,exportTypeSpecSources,compileTypeSpecDocument,TYPESPEC_EXTENSION};
