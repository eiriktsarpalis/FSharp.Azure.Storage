namespace FSharp.Azure.Storage.Table

open System
open FSharp.AWS.DynamoDB.TypeShape
open Microsoft.WindowsAzure.Storage.Table

module internal Converters =

    [<NoEquality; NoComparison>]
    [<CompilationRepresentation(CompilationRepresentationFlags.UseNullAsTrueValue)>]
    type TableValue =
        | Blank
        | Bytes of byte[]
        | Bool of bool
        | DateTimeOffset of DateTimeOffset
        | Double of double
        | String of string
        | Guid of Guid
        | Int32 of int
        | Int64 of int64
    with
        member tv.Value =
            match tv with
            | Blank -> null
            | Bytes b -> box b
            | Bool b -> box b
            | DateTimeOffset d -> box d
            | Double f -> box f
            | String s -> box s
            | Guid g -> box g
            | Int32 i -> box i
            | Int64 i -> box i

        static member ToEntityProperty(tv : TableValue) =
            let inline n v = new Nullable<_>(v)
            match tv with
            | Blank -> failwith "undefined value"
            | Bytes bs -> new EntityProperty(bs)
            | Bool b -> new EntityProperty(n b)
            | DateTimeOffset d -> new EntityProperty(n d)
            | Double f -> new EntityProperty(n f)
            | String s -> new EntityProperty(s)
            | Guid g -> new EntityProperty(n g)
            | Int32 i -> new EntityProperty(n i)
            | Int64 i -> new EntityProperty(n i)

        static member OfEntityProperty(ep : EntityProperty) =
            match ep.PropertyType with
            | EdmType.Boolean -> Bool ep.BooleanValue.Value
            | EdmType.Binary -> Bytes ep.BinaryValue
            | EdmType.DateTime -> DateTimeOffset ep.DateTimeOffsetValue.Value
            | EdmType.Double -> Double ep.DoubleValue.Value
            | EdmType.String -> String ep.StringValue
            | EdmType.Guid -> Guid ep.GuidValue.Value
            | EdmType.Int32 -> Int32 ep.Int32Value.Value
            | EdmType.Int64 -> Int64 ep.Int64Value.Value
            | _ -> invalidOp "invalid EntityProperty type"

    type IFieldConverter =
        abstract RecordType   : Type
        abstract TableType    : Type
        abstract IsMonotonic  : bool
        abstract DefaultValue : obj
        abstract FromField    : obj -> TableValue
        abstract ToField      : TableValue -> obj

    type FieldConverter<'T> =
        {
            TableType    : Type
            IsMonotonic  : bool
            DefaultValue : 'T
            FromField    : 'T -> TableValue
            ToField      : TableValue -> 'T
        }
    with
        interface IFieldConverter with
            member __.RecordType = typeof<'T>
            member __.TableType = __.TableType
            member __.IsMonotonic = __.IsMonotonic
            member __.DefaultValue = box __.DefaultValue
            member __.FromField o = __.FromField(o :?> 'T)
            member __.ToField o = __.ToField o :> obj

    let inline mkConverter isMonotonic defaultValue (fromField : 'T -> TableValue) (toField : TableValue -> 'T) =
        {
            TableType    = typeof<'T>
            DefaultValue = defaultArg defaultValue Unchecked.defaultof<'T>
            IsMonotonic  = isMonotonic
            FromField    = fromField
            ToField      = toField
        }
    
    let rec extractConverter<'T> () = extractConverterUntyped typeof<'T> :?> FieldConverter<'T>
    and extractConverterUntyped (fieldType : Type) : IFieldConverter =
        let ic (tv:TableValue) : 'T = 
            let msg = sprintf "cannot convert %A to type %O." tv.Value typeof<'T>
            raise <| new InvalidCastException(msg)

        match getShape fieldType with
        | :? ShapeBool -> mkConverter true None Bool (function Bool b -> b | v -> Convert.ToBoolean v.Value) :> _
        | :? ShapeByte -> mkConverter true None (int >> Int32) (fun v -> Convert.ToByte v.Value) :> _
        | :? ShapeSByte -> mkConverter true None (int >> Int32) (fun v -> Convert.ToSByte v.Value) :> _
        | :? ShapeInt16 -> mkConverter true None (int >> Int32) (fun v -> Convert.ToInt16 v.Value) :> _
        | :? ShapeInt32 -> mkConverter true None Int32 (function Int32 i -> i | v -> Convert.ToInt32 v.Value) :> _
        | :? ShapeInt64 -> mkConverter true None Int64 (function Int64 i -> i | v -> Convert.ToInt64 v.Value) :> _
        | :? ShapeUInt16 -> mkConverter true None (int >> Int32) (fun v -> Convert.ToUInt16 v.Value) :> _
        | :? ShapeUInt32 -> mkConverter true None (int64 >> Int64) (fun v -> Convert.ToUInt32 v.Value) :> _
        | :? ShapeUInt64 -> raise <| new ArgumentException("Azure table storage does not support uint64 fields.")
        | :? ShapeDouble -> mkConverter true None Double (function Double d -> d | v -> Convert.ToDouble v.Value) :> _
        | :? ShapeChar -> mkConverter true None (fun c -> String(string c)) (fun v -> Convert.ToChar v.Value) :> _
        | :? ShapeString -> mkConverter true None String (function String s -> s | v -> Convert.ToString v.Value) :> _
        | :? ShapeGuid -> mkConverter true (Some Guid.Empty) Guid (function Guid g -> g | String s -> Guid.Parse s | v -> ic v) :> _
        | :? ShapeByteArray -> mkConverter true (Some [||]) Bytes (function Bytes bs -> bs | v -> ic v) :> _
        | :? ShapeTimeSpan -> mkConverter true (Some TimeSpan.Zero) (fun t -> Int64 t.Ticks) (function Int64 i -> TimeSpan.FromTicks i | v -> ic v) :> _
        | :? ShapeDateTime -> mkConverter true (Some DateTime.MinValue) (fun d -> DateTimeOffset(new DateTimeOffset(d))) (function DateTimeOffset d -> d.LocalDateTime | v -> ic v) :> _
        | :? ShapeDateTimeOffset -> mkConverter true (Some DateTimeOffset.MinValue) DateTimeOffset (function DateTimeOffset d -> d | v -> ic v) :> _
        | ShapeEnum s ->
            s.Accept {
                new IEnumVisitor<IFieldConverter> with
                    member __.VisitEnum<'E, 'U when 'E : enum<'U>> () =
                        mkConverter false None
                            (fun (e : 'E) -> String (e.ToString())) 
                            (function String s -> Enum.Parse(typeof<'E>, s) :?> 'E 
                                           | v -> Enum.Parse(typeof<'E>, Convert.ToString v.Value) :?> 'E)
                        :> _
            }

        | ShapeNullable s ->
            s.Accept {
                new INullableVisitor<IFieldConverter> with
                    member __.VisitNullable<'T when 'T : (new : unit -> 'T) and 'T :> ValueType and 'T : struct> () =
                        let tconv = extractConverter<'T> ()
                        mkConverter false None
                            (fun (tn : Nullable<'T>) -> if tn.HasValue then tconv.FromField tn.Value else Blank)
                            (fun e -> new Nullable<'T>(tconv.ToField e))
                        :> _
            }

        | ShapeFSharpOption s ->
            s.Accept {
                new IFSharpOptionVisitor<IFieldConverter> with
                    member __.VisitFSharpOption<'T> () =
                        let tconv = extractConverter<'T> ()
                        mkConverter false None
                            (function None -> Blank | Some t -> tconv.FromField t)
                            (fun e -> Some(tconv.ToField e))
                        :> _
            }

        | _ -> raise <| new ArgumentException(sprintf "unsupported record field type '%O'." fieldType)