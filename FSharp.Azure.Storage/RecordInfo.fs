namespace FSharp.Azure.Storage.Table

open System
open System.Reflection
open System.Collections.Generic
open FSharp.Reflection

open FSharp.Azure.Storage.Utilities
open FSharp.Azure.Storage.Table.Converters
open Microsoft.WindowsAzure.Storage.Table

module internal RecordInfo =

    [<Flags>]
    type PropertyFlags =
        | None           = 0
        | PartitionKey   = 1
        | RowKey         = 2
        | Ignored        = 4
        | ETag           = 8
        | Timestamp      = 16
        | Serialized     = 32
        | NoDefaultValue = 64
    
    type RecordPropertyInfo =
        {
            Name : string
            Index : int
            PropertyInfo : PropertyInfo
            Flags : PropertyFlags
            Converter : IFieldConverter
        }
    with
        member inline __.IsPartitionKey = __.Flags.HasFlag PropertyFlags.PartitionKey
        member inline __.IsRowKey = __.Flags.HasFlag PropertyFlags.RowKey
        member inline __.IsIgnored = __.Flags.HasFlag PropertyFlags.Ignored
        member inline __.IsETag = __.Flags.HasFlag PropertyFlags.ETag
        member inline __.IsTimestamp = __.Flags.HasFlag PropertyFlags.Timestamp
        member inline __.IsSerializer = __.Flags.HasFlag PropertyFlags.Serialized
        member inline __.IsNoDefaultValue = __.Flags.HasFlag PropertyFlags.NoDefaultValue

    type RecordInfo =
        {
            Type : Type
            Properties : RecordPropertyInfo []
            Keys : (RecordPropertyInfo * RecordPropertyInfo) option
            ETag : RecordPropertyInfo option
            Timestamp : RecordPropertyInfo option
            RecordCtor : obj[] -> obj
        }

    let extractRecordInfo (recordType : Type) =
        if not <| FSharpType.IsRecord(recordType, allTypes) then
            invalidArg (string recordType) "not a valid F# record type"

        let ctor = FSharpValue.PreComputeRecordConstructor(recordType, allTypes)
        let properties = FSharpType.GetRecordFields(recordType, allTypes)

        let extractPropertyInfo (i : int) (p : PropertyInfo) =
            let name =
                match p.TryGetAttribute<CustomNameAttribute> () with
                | None -> p.Name
                | Some attr -> attr.Name

            let flags = ref PropertyFlags.None
            let addFlag f =
                if !flags <> PropertyFlags.None then
                    sprintf "Incompatible combination of attributes '%O' in property '%s'" (!flags ||| f) p.Name
                    |> invalidArg (string recordType)
                else
                    flags := !flags ||| f

            if p.ContainsAttribute<PartitionKeyAttribute>() then addFlag PropertyFlags.PartitionKey
            if p.ContainsAttribute<RowKeyAttribute>() then addFlag PropertyFlags.RowKey
            if p.ContainsAttribute<IgnorePropertyAttribute>() then addFlag PropertyFlags.Ignored
            if p.ContainsAttribute<TimestampAttribute>() then addFlag PropertyFlags.Timestamp
            if p.ContainsAttribute<ETagAttribute>() then addFlag PropertyFlags.ETag
            if p.ContainsAttribute<NoDefaultValueAttribute>() then addFlag PropertyFlags.NoDefaultValue
            
            let converter =
                match p.TryGetAttribute<IPropertySerializer>() with
                | Some serializer ->
                    addFlag PropertyFlags.Serialized
                    mkSerializerConverterUntyped serializer p.PropertyType
                | None -> extractConverterUntyped p.PropertyType

            {
                Name = name
                Index = i
                Flags = !flags
                PropertyInfo = p
                Converter = converter
            }

        let props = properties |> Array.mapi extractPropertyInfo

        let keyProps =
            match props |> Array.filter (fun p -> p.IsPartitionKey) with
            | [|p|] when p.PropertyInfo.DeclaringType = typeof<string> ->
                match props |> Array.filter (fun p -> p.IsRowKey) with
                | [|r|] when r.PropertyInfo.DeclaringType <> typeof<string> ->
                    invalidArg (string recordType) "RowKey property must be of type string."

                | [|r|] -> Some(p,r)
                | [||] -> invalidArg (string recordType) "has got PartitionKey attribute but missing RowKey attribute."
                | _ ->  invalidArg (string recordType) "duplicate RowKey attributes."

            | [|_|] -> invalidArg (string recordType) "PartitionKey property must be of type string."
            | [||] -> None
            | _ -> invalidArg (string recordType) "duplicate PartitionKey attributes."

        let etagProp =
            match props |> Array.filter (fun p -> p.IsETag) with
            | [|p|] when p.PropertyInfo.DeclaringType = typeof<string> || 
                         p.PropertyInfo.DeclaringType = typeof<string option> -> Some p

            | [|_|] -> invalidArg (string recordType) "ETag property must be of type string or string option."
            | [||] -> None
            | _ -> invalidArg (string recordType) "duplicate ETag attributes."

        let timestampProp =
            match props |> Array.filter (fun p -> p.IsTimestamp) with
            | [|p|] when p.PropertyInfo.DeclaringType = typeof<DateTimeOffset> ||
                         p.PropertyInfo.DeclaringType = typeof<Nullable<DateTimeOffset>> ||
                         p.PropertyInfo.DeclaringType = typeof<DateTimeOffset option> -> Some p

            | [|_|] -> invalidArg (string recordType) "Timestamp property must be of type DateTimeOffset, DateTimeOffset option or Nullable<DateTimeOffset>."
            | [||] -> None
            | _ -> invalidArg (string recordType) "duplicate Timestamp attributes."

        props
        |> Seq.groupBy (fun p -> p.Name)
        |> Seq.filter (fun (name,rs) -> Seq.length rs > 1)
        |> Seq.iter (fun (name,_) -> invalidArg (string recordType) <| sprintf "duplicate field name '%s'." name)

        {
            Type = recordType
            RecordCtor = ctor
            Properties = props
            Keys = keyProps
            ETag = etagProp
            Timestamp = timestampProp
        }

    let tryExtractEntityIdentifierReader<'Record> (info : RecordInfo) =
        assert(info.Type = typeof<'Record>)
        match info.Keys with
        | None -> None
        | Some(pKP,rKP) ->
            Some(fun (r : 'Record) ->
                let pK = pKP.PropertyInfo.GetValue(r) :?> string
                let rK = rKP.PropertyInfo.GetValue(r) :?> string
                { PartitionKey = pK ; RowKey = rK })

    let mkTableEntity (info : RecordInfo) (identifier : EntityIdentifier) (etag : string) (record : 'Record) =
        assert(info.Type = typeof<'Record>)

        let dict = new System.Collections.Generic.Dictionary<string, EntityProperty>()
        for prop in info.Properties do
            if prop.IsIgnored then () else
            let value = prop.PropertyInfo.GetValue record
            match prop.Converter.FromField value with
            | Blank -> ()
            | tv -> dict.Add(prop.Name, TableValue.ToEntityProperty tv) 

        {
            new ITableEntity with
                member __.PartitionKey = identifier.PartitionKey
                member __.RowKey = identifier.RowKey
                member __.ETag = etag
                member __.WriteEntity(_) = dict :> _
                member __.Timestamp = Unchecked.defaultof<DateTimeOffset>

                member __.PartitionKey with set _ = notImplemented()
                member __.RowKey with set _ = notImplemented()
                member __.ETag with set _ = notImplemented()
                member __.Timestamp with set _ = notImplemented()
                member __.ReadEntity(_, _) = notImplemented()
        }

    let mkResolver<'Record> (info : RecordInfo) : EntityResolver<'Record> =
        assert(info.Type = typeof<'Record>)
        new EntityResolver<_>(fun pKey rKey timestamp props etag ->
            info.Properties
            |> Array.map (fun p ->
                if p.IsPartitionKey then p.Converter.ToField(String pKey)
                elif p.IsRowKey then p.Converter.ToField(String rKey)
                elif p.IsETag then p.Converter.ToField (String etag)
                elif p.IsTimestamp then p.Converter.ToField (DateTimeOffset timestamp)
                else
                    let ok,found = props.TryGetValue p.Name
                    if ok then 
                        let tv = TableValue.OfEntityProperty found
                        p.Converter.ToField tv
                    elif p.IsNoDefaultValue then
                        invalidOp <| sprintf "Could not find value for property '%s'" p.Name
                    else
                        p.Converter.DefaultValue)

            |> info.RecordCtor
            |> unbox<'Record>)