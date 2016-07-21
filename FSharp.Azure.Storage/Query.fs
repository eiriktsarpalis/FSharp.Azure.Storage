namespace FSharp.Azure.Storage.Table

open System
open Microsoft.WindowsAzure.Storage.Table

module internal Query =

    type EntityQuery<'T> = 
        { Filter : string
          TakeCount : int option
          SelectColumns : string Set }
        static member get_Zero() : EntityQuery<'T> = 
            { Filter = "" 
                TakeCount = None
                SelectColumns = EntityTypeCache<'T>.PropertyNames.Value }
        static member (+) (left : EntityQuery<'T>, right : EntityQuery<'T>) =
            let filter =
                match left.Filter, right.Filter with
                | "", "" -> ""
                | l, "" -> l
                | "", r -> r
                | l, r -> TableQuery.CombineFilters (l, "and", r)
            let takeCount = 
                match left.TakeCount, right.TakeCount with
                | Some l, Some r -> Some (min l r)
                | Some l, None -> Some l
                | None, Some r -> Some r
                | None, None -> None
            let selectColumns = left.SelectColumns |> Set.intersect right.SelectColumns

            { Filter = filter; TakeCount = takeCount; SelectColumns = selectColumns }

        member this.ToTableQuery() = 
            TableQuery (
                FilterString = this.Filter, 
                TakeCount = (this.TakeCount |> toNullable),
                SelectColumns = (this.SelectColumns |> Set.toArray))