namespace FSharp.Azure.Storage.Table

open System
open TypeShape
open Microsoft.WindowsAzure.Storage.Table

module internal Converters =

    [<NoEquality; NoComparison>]
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
        abstract EdmType      : EdmType
        abstract IsMonotonic  : bool
        abstract DefaultValue : obj
        abstract FromField    : obj -> TableValue
        abstract ToField      : TableValue -> obj

    type FieldConverter<'T> =
        {
            EdmType      : EdmType
            IsMonotonic  : bool
            DefaultValue : 'T
            FromField    : 'T -> TableValue
            ToField      : TableValue -> 'T
        }
    with
        interface IFieldConverter with
            member __.RecordType = typeof<'T>
            member __.EdmType  = __.EdmType
            member __.IsMonotonic = __.IsMonotonic
            member __.DefaultValue = box __.DefaultValue
            member __.FromField o = __.FromField(o :?> 'T)
            member __.ToField o = __.ToField o :> obj

    let inline mkConverter isMonotonic edmTy defaultValue (fromField : 'T -> TableValue) (toField : TableValue -> 'T) =
        {
            EdmType      = edmTy
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

        match TypeShape.Resolve fieldType with
        | Shape.Bool -> mkConverter true EdmType.Boolean None Bool (function Bool b -> b | v -> Convert.ToBoolean v.Value) :> _
        | Shape.Byte -> mkConverter true EdmType.Int32 None (int >> Int32) (fun v -> Convert.ToByte v.Value) :> _
        | Shape.SByte -> mkConverter true EdmType.Int32 None (int >> Int32) (fun v -> Convert.ToSByte v.Value) :> _
        | Shape.Int16 -> mkConverter true EdmType.Int32 None (int >> Int32) (fun v -> Convert.ToInt16 v.Value) :> _
        | Shape.Int32 -> mkConverter true EdmType.Int32 None Int32 (function Int32 i -> i | v -> Convert.ToInt32 v.Value) :> _
        | Shape.Int64 -> mkConverter true EdmType.Int64 None Int64 (function Int64 i -> i | v -> Convert.ToInt64 v.Value) :> _
        | Shape.UInt16 -> mkConverter true EdmType.Int32 None (int >> Int32) (fun v -> Convert.ToUInt16 v.Value) :> _
        | Shape.UInt32 -> mkConverter true EdmType.Int64 None (int64 >> Int64) (fun v -> Convert.ToUInt32 v.Value) :> _
        | Shape.UInt64 -> raise <| new ArgumentException("Azure table storage does not support uint64 fields.")
        | Shape.Single -> mkConverter true EdmType.Double None (double >> Double) (function Double d -> single d | v -> Convert.ToSingle v.Value) :> _
        | Shape.Double -> mkConverter true EdmType.Double None Double (function Double d -> d | v -> Convert.ToDouble v.Value) :> _
        | Shape.Char -> mkConverter true EdmType.String None (fun c -> String(string c)) (fun v -> Convert.ToChar v.Value) :> _
        | Shape.String -> mkConverter true EdmType.String None String (function String s -> s | v -> Convert.ToString v.Value) :> _
        | Shape.Guid -> mkConverter true EdmType.Guid (Some Guid.Empty) Guid (function Guid g -> g | String s -> Guid.Parse s | v -> ic v) :> _
        | Shape.ByteArray -> mkConverter true EdmType.Binary None Bytes (function Bytes bs -> bs | v -> ic v) :> _
        | Shape.TimeSpan -> mkConverter true EdmType.Int64 (Some TimeSpan.Zero) (fun (t:TimeSpan) -> Int64 t.Ticks) (function Int64 i -> TimeSpan.FromTicks i | v -> ic v) :> _
        | Shape.DateTime -> mkConverter true EdmType.DateTime (Some DateTime.MinValue) (fun d -> DateTimeOffset(new DateTimeOffset(d))) (function DateTimeOffset d -> d.LocalDateTime | v -> ic v) :> _
        | Shape.DateTimeOffset -> mkConverter true EdmType.DateTime (Some DateTimeOffset.MinValue) DateTimeOffset (function DateTimeOffset d -> d | v -> ic v) :> _
        | Shape.Enum s ->
            s.Accept {
                new IEnumVisitor<IFieldConverter> with
                    member __.Visit<'E, 'U when 'E : enum<'U>> () =
                        mkConverter false EdmType.String None
                            (fun (e : 'E) -> String (e.ToString())) 
                            (function String s -> Enum.Parse(typeof<'E>, s) :?> 'E 
                                           | v -> Enum.Parse(typeof<'E>, Convert.ToString v.Value) :?> 'E)
                        :> _
            }

        | Shape.Nullable s ->
            s.Accept {
                new INullableVisitor<IFieldConverter> with
                    member __.Visit<'T when 'T : (new : unit -> 'T) and 'T :> ValueType and 'T : struct> () =
                        let tconv = extractConverter<'T> ()
                        mkConverter false tconv.EdmType (Some (Nullable<_>()))
                            (fun (tn : Nullable<'T>) -> if tn.HasValue then tconv.FromField tn.Value else Blank)
                            (fun e -> new Nullable<'T>(tconv.ToField e))
                        :> _
            }

        | Shape.FSharpOption s ->
            s.Accept {
                new IFSharpOptionVisitor<IFieldConverter> with
                    member __.Visit<'T> () =
                        let tconv = extractConverter<'T> ()
                        mkConverter false tconv.EdmType (Some None)
                            (function None -> Blank | Some t -> tconv.FromField t)
                            (fun e -> Some(tconv.ToField e))
                        :> _
            }

        | _ -> raise <| new ArgumentException(sprintf "unsupported record field type '%O'." fieldType)


    let mkSerializerConverter<'T> (serializer : IPropertySerializer) =
        let pickleConverter = extractConverterUntyped serializer.PickleType
        mkConverter false pickleConverter.EdmType None
            (fun (t : 'T) -> serializer.Serialize<'T>(t) |> pickleConverter.FromField)
            (fun tv -> let p = pickleConverter.FromField tv in serializer.Deserialize<'T>(p))


    let mkSerializerConverterUntyped (serializer : IPropertySerializer) (t : Type) =
        TypeShape.Resolve(t).Accept {
            new ITypeShapeVisitor<IFieldConverter> with
                member __.Visit<'T> () = mkSerializerConverter<'T> serializer :> _
        }