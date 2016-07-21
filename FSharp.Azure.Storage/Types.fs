namespace FSharp.Azure.Storage.Table

open System

/// Declares that carrying property specifies the Partition key for the record instance.
/// Property must be of type string.
[<Sealed; AttributeUsage(AttributeTargets.Property, AllowMultiple = false)>]
type PartitionKeyAttribute () = inherit Attribute()

/// Declares that carrying property specifies the Row key for the record instance.
/// Property must be of type string.        
[<Sealed; AttributeUsage(AttributeTargets.Property, AllowMultiple = false)>]
type RowKeyAttribute () = inherit Attribute()

[<Sealed; AttributeUsage(AttributeTargets.Property, AllowMultiple = false)>]
type ETagAttribute () = inherit Attribute()

[<Sealed; AttributeUsage(AttributeTargets.Property, AllowMultiple = false)>]
type TimestampAttribute () = inherit Attribute()

[<Sealed; AttributeUsage(AttributeTargets.Property, AllowMultiple = false)>]
type NoDefaultValueAttribute () = inherit Attribute()

/// Specify a custom Table storage attribute name for the given record field.
[<AttributeUsage(AttributeTargets.Property, AllowMultiple = false)>]
type CustomNameAttribute(name : string) =
    inherit System.Attribute()
    do if name = null then raise <| ArgumentNullException("'Name' parameter cannot be null.")
    member __.Name = name

/// Declares that carrying should be ignored on serialization/deserialization.
type IgnorePropertyAttribute = Microsoft.WindowsAzure.Storage.Table.IgnorePropertyAttribute

/// Declares that the given property should be serialized using the given
/// Serialization/Deserialization methods before being uploaded to the table.
[<AbstractClass; AttributeUsage(AttributeTargets.Property, AllowMultiple = false)>]
type PropertySerializerAttribute<'PickleType>() =
    inherit Attribute()
    /// Serializes a value to the given pickle type
    abstract Serialize   :  'T -> 'PickleType
    /// Deserializes a value from the given pickle type
    abstract Deserialize : 'PickleType -> 'T

    interface IPropertySerializer with
        member __.PickleType = typeof<'PickleType>
        member __.Serialize value = __.Serialize value :> obj
        member __.Deserialize pickle = __.Deserialize (pickle :?> 'PickleType)

/// Declares that the given property should be serialized using the given
/// Serialization/Deserialization methods before being uploaded to the table.
and internal IPropertySerializer =
    abstract PickleType  : Type
    abstract Serialize   : value:'T -> obj
    abstract Deserialize : pickle:obj -> 'T

type EntityIdentifier = { [<PartitionKey>] PartitionKey : string; [<RowKey>] RowKey : string; }
type OperationResult = { HttpStatusCode : int; Etag : string }
type EntityMetadata = { Etag : string; Timestamp : DateTimeOffset }

type IEntityIdentifiable = 
    abstract member GetIdentifier : unit -> EntityIdentifier

type Operation<'T> =
    | Insert of entity : 'T
    | InsertOrMerge of entity : 'T
    | InsertOrReplace of entity : 'T
    | Replace of entity : 'T * etag : string
    | ForceReplace of entity : 'T
    | Merge of entity : 'T * etag : string
    | ForceMerge of entity : 'T
    | Delete of entity : 'T * etag : string
    | ForceDelete of entity : 'T
//    member this.GetEntity() = 
//        match this with
//        | Insert (entity) -> entity
//        | InsertOrMerge (entity) -> entity
//        | InsertOrReplace (entity) -> entity
//        | Merge (entity, _) -> entity
//        | ForceMerge (entity) -> entity
//        | Replace (entity, _) -> entity
//        | ForceReplace (entity) -> entity
//        | Delete (entity, _) -> entity
//        | ForceDelete (entity) -> entity