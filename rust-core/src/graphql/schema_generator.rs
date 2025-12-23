use async_graphql::{
    dynamic::*, 
    // Context, EmptyMutation, EmptySubscription, Schema, 
    // Object, SimpleObject, InputObject, Enum, Interface, Union,
};
// use std::sync::Arc;

pub struct GraphQLSchemaGenerator;

impl GraphQLSchemaGenerator {
    pub fn new() -> Self {
        Self
    }

    pub fn generate(&self) -> Schema {
        // Root Query
        let query = Object::new("Query")
            .field(Field::new("hello", TypeRef::named_nn(TypeRef::STRING), |_| {
                FieldFuture::new(async { Ok(Some(FieldValue::owned_any("world".to_string()))) })
            }));

        // Root Mutation
        let mutation = Object::new("Mutation");
        
        // Subscription (Empty for now)
        let subscription = Subscription::new("Subscription");

        Schema::build(query.into(), mutation.into(), subscription.into())
            .finish()
            .unwrap()
    }
}
