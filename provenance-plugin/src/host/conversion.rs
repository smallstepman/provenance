use std::collections::BTreeMap;

use provenance_core::{
    ActorId, Attributes, Availability, ClaimId, Direction, EntityAddress, EntityKind,
    EntityObservation, EntityRef, EntitySchema, EntityType, EntityTypePattern, EventId,
    EventIntent, ExplanationDirection, ExplanationRole, ExplanationSemantics, FieldName,
    FieldSchema, Integrity, Intent, InternalEntityKind, InternalEntityRef, Namespace, ObjectId,
    OperationId, QueryKey, QueryTemplate, Relation, RelationSchema, RelationSelector, RelationType,
    ReplicaId, Resource, ResourceObservation, ResourceRequirement, RetentionStrength,
    SchemaDefinition, SchemaKey, SessionId, SourceOperation, Transaction, Value, ValueType,
};
use provenance_data_model::{NodeRef, NodeType};

use super::bindings;
use super::model::PluginModel;

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ConversionError {
    #[error("duplicate attribute `{0}`")]
    DuplicateAttribute(String),
    #[error("duplicate schema entry `{0}`")]
    DuplicateSchemaEntry(String),
    #[error("duplicate relation type `{0}:{1}`")]
    DuplicateRelation(String, String),
    #[error("invalid integer `{0}`: expected a signed 128-bit integer")]
    InvalidInteger(String),
    #[error("invalid query depth `{0}`: does not fit in usize")]
    InvalidQueryDepth(u64),
}

pub fn transaction(
    transaction: bindings::Transaction,
) -> Result<Transaction<PluginModel>, ConversionError> {
    Ok(Transaction {
        seed: transaction.seed,
        source: transaction.source.map(source_operation).transpose()?,
        intents: transaction
            .intents
            .into_iter()
            .map(intent)
            .collect::<Result<_, _>>()?,
        attributes: attributes(transaction.attributes)?,
    })
}

pub fn source_operation(
    source: bindings::SourceOperation,
) -> Result<SourceOperation<PluginModel>, ConversionError> {
    Ok(SourceOperation {
        id: address(source.id),
        parents: source.parents.into_iter().map(address).collect(),
    })
}

pub fn address(address: bindings::EntityAddress) -> EntityAddress<PluginModel> {
    EntityAddress::new(address.namespace, address.kind, address.id)
}

fn entity_type(value: bindings::EntityType) -> EntityType {
    EntityType::new(value.namespace, value.kind)
}

fn entity_type_pattern(pattern: bindings::EntityTypePattern) -> EntityTypePattern {
    match pattern {
        bindings::EntityTypePattern::Internal(kind) => {
            EntityTypePattern::Internal(internal_entity_kind(kind))
        }
        bindings::EntityTypePattern::External(value) => {
            EntityTypePattern::External(entity_type(value))
        }
        bindings::EntityTypePattern::Any => EntityTypePattern::Any,
    }
}

fn internal_entity_kind(kind: bindings::InternalEntityKind) -> InternalEntityKind {
    match kind {
        bindings::InternalEntityKind::Operation => InternalEntityKind::Operation,
        bindings::InternalEntityKind::Event => InternalEntityKind::Event,
        bindings::InternalEntityKind::Session => InternalEntityKind::Session,
        bindings::InternalEntityKind::Actor => InternalEntityKind::Actor,
        bindings::InternalEntityKind::Object => InternalEntityKind::Object,
        bindings::InternalEntityKind::Replica => InternalEntityKind::Replica,
    }
}

fn entity_ref(entity: bindings::EntityRef) -> EntityRef<PluginModel> {
    match entity {
        bindings::EntityRef::Internal(reference) => EntityRef::Internal(match reference {
            bindings::InternalEntityRef::Operation(id) => {
                InternalEntityRef::Operation(OperationId::<PluginModel>::new(id))
            }
            bindings::InternalEntityRef::Event(id) => {
                InternalEntityRef::Event(EventId::<PluginModel>::new(id))
            }
            bindings::InternalEntityRef::Session(id) => {
                InternalEntityRef::Session(SessionId::<PluginModel>::new(id))
            }
            bindings::InternalEntityRef::Actor(id) => {
                InternalEntityRef::Actor(ActorId::<PluginModel>::new(id))
            }
            bindings::InternalEntityRef::Object(id) => {
                InternalEntityRef::Object(ObjectId::<PluginModel>::new(id))
            }
            bindings::InternalEntityRef::Replica(id) => {
                InternalEntityRef::Replica(ReplicaId::<PluginModel>::new(id))
            }
        }),
        bindings::EntityRef::External(value) => EntityRef::External(address(value)),
    }
}

fn node_ref(reference: bindings::NodeRef) -> NodeRef<PluginModel> {
    match reference {
        bindings::NodeRef::Entity(id) => NodeRef::Entity(provenance_data_model::Id::new(id)),
        bindings::NodeRef::Activity(id) => NodeRef::Activity(provenance_data_model::Id::new(id)),
        bindings::NodeRef::Agent(id) => NodeRef::Agent(provenance_data_model::Id::new(id)),
        bindings::NodeRef::External(value) => NodeRef::External(address(value)),
        bindings::NodeRef::Assertion(id) => NodeRef::Assertion(provenance_data_model::Id::new(id)),
    }
}

fn node_type(value: bindings::NodeType) -> NodeType {
    match value {
        bindings::NodeType::Entity(entity_type) => NodeType::Entity {
            namespace: entity_type.namespace.map(Namespace::from),
            kind: EntityKind::from(entity_type.kind),
        },
        bindings::NodeType::Activity(entity_type) => NodeType::Activity {
            namespace: entity_type.namespace.map(Namespace::from),
            kind: EntityKind::from(entity_type.kind),
        },
        bindings::NodeType::Agent(entity_type) => NodeType::Agent {
            namespace: entity_type.namespace.map(Namespace::from),
            kind: EntityKind::from(entity_type.kind),
        },
        bindings::NodeType::Assertion => NodeType::Assertion,
        bindings::NodeType::External(external_type) => {
            NodeType::External(entity_type(external_type))
        }
        bindings::NodeType::Any => NodeType::Any,
    }
}

fn scalar_value_type(value_type: bindings::ScalarValueType) -> ValueType {
    match value_type {
        bindings::ScalarValueType::NullValue => ValueType::Null,
        bindings::ScalarValueType::Boolean => ValueType::Bool,
        bindings::ScalarValueType::Integer => ValueType::Integer,
        bindings::ScalarValueType::Decimal => ValueType::Decimal,
        bindings::ScalarValueType::StringValue => ValueType::String,
        bindings::ScalarValueType::BytesValue => ValueType::Bytes,
        bindings::ScalarValueType::EntityValue(pattern) => {
            ValueType::Entity(entity_type_pattern(pattern))
        }
        bindings::ScalarValueType::RefValue(value) => ValueType::Ref(node_type(value)),
        bindings::ScalarValueType::AnyValue => ValueType::Any,
    }
}

fn value_type(value_type: bindings::ValueType) -> Result<ValueType, ConversionError> {
    Ok(match value_type {
        bindings::ValueType::NullValue => ValueType::Null,
        bindings::ValueType::Boolean => ValueType::Bool,
        bindings::ValueType::Integer => ValueType::Integer,
        bindings::ValueType::Decimal => ValueType::Decimal,
        bindings::ValueType::StringValue => ValueType::String,
        bindings::ValueType::BytesValue => ValueType::Bytes,
        bindings::ValueType::EntityValue(pattern) => {
            ValueType::Entity(entity_type_pattern(pattern))
        }
        bindings::ValueType::RefValue(value) => ValueType::Ref(node_type(value)),
        bindings::ValueType::ListValue(item) => ValueType::List(Box::new(scalar_value_type(item))),
        bindings::ValueType::MapValue(map) => {
            let mut fields = BTreeMap::new();
            for field in map.fields {
                let name = field.name;
                if fields
                    .insert(
                        FieldName::from(name.clone()),
                        FieldSchema {
                            value_type: scalar_value_type(field.value_type),
                            required: field.required,
                        },
                    )
                    .is_some()
                {
                    return Err(ConversionError::DuplicateSchemaEntry(name));
                }
            }
            ValueType::Map {
                fields,
                allow_unknown: map.allow_unknown,
            }
        }
        bindings::ValueType::AnyValue => ValueType::Any,
    })
}

fn scalar_value(value: bindings::ScalarValue) -> Result<Value<PluginModel>, ConversionError> {
    Ok(match value {
        bindings::ScalarValue::NullValue => Value::Null,
        bindings::ScalarValue::Boolean(value) => Value::Bool(value),
        bindings::ScalarValue::IntegerText(value) => Value::Integer(
            value
                .parse()
                .map_err(|_| ConversionError::InvalidInteger(value.clone()))?,
        ),
        bindings::ScalarValue::DecimalText(value) => Value::Decimal(value.into_boxed_str()),
        bindings::ScalarValue::StringValue(value) => Value::String(value.into_boxed_str()),
        bindings::ScalarValue::BytesValue(value) => Value::Bytes(value),
        bindings::ScalarValue::EntityValue(value) => Value::Entity(entity_ref(value)),
        bindings::ScalarValue::RefValue(value) => Value::Ref(node_ref(value)),
    })
}

fn value(value: bindings::Value) -> Result<Value<PluginModel>, ConversionError> {
    Ok(match value {
        bindings::Value::NullValue => Value::Null,
        bindings::Value::Boolean(value) => Value::Bool(value),
        bindings::Value::IntegerText(value) => Value::Integer(
            value
                .parse()
                .map_err(|_| ConversionError::InvalidInteger(value.clone()))?,
        ),
        bindings::Value::DecimalText(value) => Value::Decimal(value.into_boxed_str()),
        bindings::Value::StringValue(value) => Value::String(value.into_boxed_str()),
        bindings::Value::BytesValue(value) => Value::Bytes(value),
        bindings::Value::EntityValue(value) => Value::Entity(entity_ref(value)),
        bindings::Value::RefValue(value) => Value::Ref(node_ref(value)),
        bindings::Value::ListValue(values) => Value::List(
            values
                .into_iter()
                .map(scalar_value)
                .collect::<Result<_, _>>()?,
        ),
        bindings::Value::MapValue(values) => Value::Map(scalar_attributes(values)?),
    })
}

fn scalar_attributes(
    values: Vec<bindings::ScalarAttribute>,
) -> Result<Attributes<PluginModel>, ConversionError> {
    let mut result = BTreeMap::new();
    for attribute in values {
        let name = attribute.name;
        if result
            .insert(
                FieldName::from(name.clone()),
                scalar_value(attribute.value)?,
            )
            .is_some()
        {
            return Err(ConversionError::DuplicateAttribute(name));
        }
    }
    Ok(result)
}

fn attributes(
    values: Vec<bindings::Attribute>,
) -> Result<Attributes<PluginModel>, ConversionError> {
    let mut result = BTreeMap::new();
    for attribute in values {
        let name = attribute.name;
        if result
            .insert(FieldName::from(name.clone()), value(attribute.value)?)
            .is_some()
        {
            return Err(ConversionError::DuplicateAttribute(name));
        }
    }
    Ok(result)
}

fn entity_schema(
    schema: bindings::EntitySchema,
    namespace: &Namespace,
) -> Result<EntitySchema, ConversionError> {
    let mut fields = BTreeMap::new();
    for field in schema.fields {
        let name = field.name;
        if fields
            .insert(
                FieldName::from(name.clone()),
                FieldSchema {
                    value_type: value_type(field.schema.value_type)?,
                    required: field.schema.required,
                },
            )
            .is_some()
        {
            return Err(ConversionError::DuplicateSchemaEntry(name));
        }
    }
    Ok(EntitySchema::new(
        EntityType::new(namespace.clone(), schema.kind),
        fields,
        schema.allow_unknown_fields,
    ))
}

fn explanation(explanation: bindings::ExplanationSemantics) -> ExplanationSemantics {
    let role = match explanation.role {
        bindings::ExplanationRole::Primary => ExplanationRole::Primary,
        bindings::ExplanationRole::Supporting => ExplanationRole::Supporting,
        bindings::ExplanationRole::Contextual => ExplanationRole::Contextual,
        bindings::ExplanationRole::Causal => ExplanationRole::Causal,
        bindings::ExplanationRole::Contradicting => ExplanationRole::Contradicting,
        bindings::ExplanationRole::Invalidating => ExplanationRole::Invalidating,
        bindings::ExplanationRole::Attribution => ExplanationRole::Attribution,
        bindings::ExplanationRole::Influence => ExplanationRole::Influence,
    };
    let direction = match explanation.direction {
        bindings::ExplanationDirection::SourceExplainsTarget => {
            ExplanationDirection::FromExplainedByTo
        }
        bindings::ExplanationDirection::TargetExplainsSource => {
            ExplanationDirection::ToExplainedByFrom
        }
        bindings::ExplanationDirection::Symmetric => ExplanationDirection::Symmetric,
        bindings::ExplanationDirection::SubjectExplainsByObject => {
            ExplanationDirection::SubjectExplainedByObject
        }
        bindings::ExplanationDirection::ObjectExplainsBySubject => {
            ExplanationDirection::ObjectExplainedBySubject
        }
    };
    ExplanationSemantics::new(role, direction)
}

fn schema_definition(
    schema: bindings::SchemaDefinition,
) -> Result<SchemaDefinition, ConversionError> {
    let key = schema_key(schema.key);
    let namespace = key.namespace.clone();

    let mut entities = BTreeMap::new();
    for entity in schema.entities {
        let kind = EntityKind::from(entity.kind.clone());
        if entities
            .insert(kind.clone(), entity_schema(entity, &namespace)?)
            .is_some()
        {
            return Err(ConversionError::DuplicateSchemaEntry(
                kind.as_str().to_owned(),
            ));
        }
    }

    let mut relations = BTreeMap::new();
    for relation in schema.relations {
        let relation_type_value = relation_type(relation.relation_type);
        let name = relation_type_value.name.clone();
        if relations
            .insert(
                name.clone(),
                RelationSchema::new(
                    relation_type_value,
                    entity_type_pattern(relation.source),
                    entity_type_pattern(relation.target),
                    relation.explanation.map(explanation),
                ),
            )
            .is_some()
        {
            return Err(ConversionError::DuplicateRelation(
                name.as_str().to_owned(),
                key.namespace.as_str().to_owned(),
            ));
        }
    }

    Ok(SchemaDefinition {
        key,
        requires: schema.requires.into_iter().map(schema_key).collect(),
        entities,
        relations,
    })
}

fn schema_key(key: bindings::SchemaKey) -> SchemaKey {
    SchemaKey::new(key.namespace, key.version)
}

fn relation_type(relation: bindings::RelationType) -> RelationType {
    RelationType::new(relation.namespace, relation.name)
}

fn entity_observation(
    observation: bindings::EntityObservation,
) -> Result<EntityObservation<PluginModel>, ConversionError> {
    Ok(EntityObservation {
        entity: address(observation.entity),
        schema: schema_key(observation.schema),
        attributes: attributes(observation.attributes)?,
    })
}

fn relation(relation: bindings::Relation) -> Result<Relation<PluginModel>, ConversionError> {
    Ok(Relation {
        schema: schema_key(relation.schema),
        relation_type: relation_type(relation.relation_type),
        from: entity_ref(relation.source),
        to: entity_ref(relation.target),
        attributes: attributes(relation.attributes)?,
    })
}

fn resource_requirement(
    requirement: bindings::ResourceRequirement,
) -> Result<ResourceRequirement<PluginModel>, ConversionError> {
    Ok(ResourceRequirement {
        resource: Resource {
            entity: entity_ref(requirement.target),
        },
        strength: retention_strength(requirement.strength),
    })
}

fn event_intent(event: bindings::EventIntent) -> Result<EventIntent<PluginModel>, ConversionError> {
    Ok(EventIntent {
        session: event.session.map(|id| SessionId::<PluginModel>::new(id)),
        actor: event.actor.map(|id| ActorId::<PluginModel>::new(id)),
        additional_parents: event
            .additional_parents
            .into_iter()
            .map(|id| EventId::<PluginModel>::new(id))
            .collect(),
        subjects: event.subjects.into_iter().map(entity_ref).collect(),
        relations: event
            .relations
            .into_iter()
            .map(relation)
            .collect::<Result<_, _>>()?,
        requires: event
            .requires
            .into_iter()
            .map(resource_requirement)
            .collect::<Result<_, _>>()?,
        attributes: attributes(event.attributes)?,
    })
}

fn retention_strength(strength: bindings::RetentionStrength) -> RetentionStrength {
    match strength {
        bindings::RetentionStrength::Referenced => RetentionStrength::Referenced,
        bindings::RetentionStrength::Pinned => RetentionStrength::Pinned,
        bindings::RetentionStrength::Escrowed => RetentionStrength::Escrowed,
    }
}

fn named_query(
    query: bindings::NamedQueryDefinition,
) -> Result<provenance_core::NamedQueryDefinition, ConversionError> {
    Ok(provenance_core::NamedQueryDefinition {
        key: QueryKey::new(query.key.namespace, query.key.version, query.key.name),
        input: entity_type_pattern(query.input),
        template: query_template(query.template)?,
    })
}

fn query_template(template: bindings::QueryTemplate) -> Result<QueryTemplate, ConversionError> {
    Ok(match template {
        bindings::QueryTemplate::History => QueryTemplate::History,
        bindings::QueryTemplate::Explain(query) => QueryTemplate::Explain {
            max_depth: query
                .max_depth
                .map(|depth| {
                    usize::try_from(depth).map_err(|_| ConversionError::InvalidQueryDepth(depth))
                })
                .transpose()?,
            roles: query
                .roles
                .into_iter()
                .map(|role| match role {
                    bindings::ExplanationRole::Primary => ExplanationRole::Primary,
                    bindings::ExplanationRole::Supporting => ExplanationRole::Supporting,
                    bindings::ExplanationRole::Contextual => ExplanationRole::Contextual,
                    bindings::ExplanationRole::Causal => ExplanationRole::Causal,
                    bindings::ExplanationRole::Contradicting => ExplanationRole::Contradicting,
                    bindings::ExplanationRole::Invalidating => ExplanationRole::Invalidating,
                    bindings::ExplanationRole::Attribution => ExplanationRole::Attribution,
                    bindings::ExplanationRole::Influence => ExplanationRole::Influence,
                })
                .collect(),
        },
        bindings::QueryTemplate::Traverse(query) => QueryTemplate::Traverse {
            direction: match query.direction {
                bindings::Direction::Incoming => Direction::Incoming,
                bindings::Direction::Outgoing => Direction::Outgoing,
                bindings::Direction::Both => Direction::Both,
            },
            relations: match query.relations {
                bindings::RelationSelector::Any => RelationSelector::Any,
                bindings::RelationSelector::Types(types) => {
                    RelationSelector::Types(types.into_iter().map(relation_type).collect())
                }
                bindings::RelationSelector::Explanatory => RelationSelector::Explanatory,
            },
            max_depth: query
                .max_depth
                .map(|depth| {
                    usize::try_from(depth).map_err(|_| ConversionError::InvalidQueryDepth(depth))
                })
                .transpose()?,
        },
    })
}

fn availability(availability: bindings::Availability) -> Availability<PluginModel> {
    match availability {
        bindings::Availability::Unknown => Availability::Unknown,
        bindings::Availability::Local => Availability::Local,
        bindings::Availability::Remote(replicas) => Availability::Remote(
            replicas
                .into_iter()
                .map(|id| ReplicaId::<PluginModel>::new(id))
                .collect(),
        ),
        bindings::Availability::Missing => Availability::Missing,
    }
}

fn integrity(integrity: bindings::Integrity) -> Integrity {
    match integrity {
        bindings::Integrity::Unknown => Integrity::Unknown,
        bindings::Integrity::Unverified => Integrity::Unverified,
        bindings::Integrity::Verified => Integrity::Verified,
        bindings::Integrity::Invalid => Integrity::Invalid,
    }
}

fn intent(value: bindings::Intent) -> Result<Intent<PluginModel>, ConversionError> {
    Ok(match value {
        bindings::Intent::RegisterSchema(schema_value) => {
            Intent::RegisterSchema(schema_definition(schema_value)?)
        }
        bindings::Intent::RegisterNamedQuery(query) => {
            Intent::RegisterNamedQuery(named_query(query)?)
        }
        bindings::Intent::OpenSession(session) => Intent::OpenSession {
            seed: session.seed,
            parent: session.parent.map(|id| SessionId::<PluginModel>::new(id)),
            attributes: attributes(session.attributes)?,
        },
        bindings::Intent::EndSession(id) => Intent::EndSession {
            session: SessionId::<PluginModel>::new(id),
        },
        bindings::Intent::DeclareActor(actor) => Intent::DeclareActor {
            seed: actor.seed,
            session: actor.session.map(|id| SessionId::<PluginModel>::new(id)),
            attributes: attributes(actor.attributes)?,
        },
        bindings::Intent::PutObject(object) => Intent::PutObject {
            seed: object.seed,
            payload: object.payload,
            attributes: attributes(object.attributes)?,
        },
        bindings::Intent::ObserveEntity(observation) => {
            Intent::ObserveEntity(entity_observation(observation)?)
        }
        bindings::Intent::RecordEvent(event_value) => {
            Intent::RecordEvent(event_intent(event_value)?)
        }
        bindings::Intent::ReleaseRetention(id) => Intent::ReleaseRetention {
            claim: ClaimId::<PluginModel>::new(id),
        },
        bindings::Intent::DeclareReplica(replica) => Intent::DeclareReplica {
            seed: replica.seed,
            attributes: attributes(replica.attributes)?,
        },
        bindings::Intent::ObserveResource(observation) => Intent::ObserveResource {
            resource: Resource {
                entity: entity_ref(observation.target),
            },
            observation: ResourceObservation {
                availability: availability(observation.observation.availability),
                integrity: integrity(observation.observation.integrity),
                observed_retention: observation
                    .observation
                    .observed_retention
                    .map(retention_strength),
            },
        },
    })
}
