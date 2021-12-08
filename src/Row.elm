module Row exposing (Row, comp, filter, fromDict, fromList, get, map, schema, select, toDict, update)

import Dict exposing (Dict)
import Field exposing (..)


type Row
    = Row (Dict String Field)


toDict : Row -> Dict String Field
toDict (Row row) =
    row


fromDict : Dict String Field -> Row
fromDict dict =
    Row dict


fromList : List ( String, Field ) -> Row
fromList list =
    fromDict (Dict.fromList list)


select : List String -> Row -> Row
select cols (Row row) =
    Row (Dict.filter (\k v -> List.member k cols) row)


filter : (Field -> Bool) -> Row -> Row
filter predicate (Row row) =
    Row (Dict.filter (\k v -> predicate v) row)


update : String -> Field -> Row -> Row
update col field (Row row) =
    Row (Dict.insert col field row)


get : String -> Row -> Field
get name (Row row) =
    Maybe.withDefault (UndefinedField ("Field " ++ name ++ " not in dataframe.")) (Dict.get name row)


map : String -> Calc Row -> Row -> Row
map col calc row =
    update col (calc row) row


comp : Calc Row -> Row -> Row -> Order
comp calc left right =
    Field.comp (calc left) (calc right)


schema : Row -> List ( String, Field )
schema row =
    row
        |> toDict
        |> Dict.toList
        |> List.map
            (\( name, field ) ->
                case field of
                    NumericField _ ->
                        ( name, NumericField 0 )

                    StringField _ ->
                        ( name, StringField "" )

                    BoolField _ ->
                        ( name, BoolField True )

                    NothingField ->
                        ( name, NothingField )

                    UndefinedField _ ->
                        ( name, UndefinedField "" )
            )
