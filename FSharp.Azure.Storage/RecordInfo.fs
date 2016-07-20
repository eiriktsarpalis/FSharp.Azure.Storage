namespace FSharp.Azure.Storage.Table

open System
open System.Reflection
open FSharp.Reflection

open FSharp.Azure.Storage.Utilities
open FSharp.Azure.Storage.Table.Converters

module internal RecordInfo =

    [<Flags>]
    type PropertyFlags =
        | None         = 0
        | PartitionKey = 1
        | RowKey       = 2
        | Ignored      = 4
        | ETag         = 8
        | Timestamp    = 16
        | Serialized   = 32
    
    type RecordPropertyInfo =
        {
            Flags : PropertyFlags
            PropertyInfo : PropertyInfo
            Converter : IFieldConverter
        }

    type RecordInfo =
        {
            Type : Type
            Properties : RecordPropertyInfo []
        }

    let extractRecordInfo (recordType : Type) =
        if not <| FSharpType.IsRecord(recordType, allTypes) then
            invalidArg (string recordType) "not a valid F# record type"

        let properties = FSharpType.GetRecordFields(recordType, allTypes)

        let extractPropertyInfo (p : PropertyInfo) =
            